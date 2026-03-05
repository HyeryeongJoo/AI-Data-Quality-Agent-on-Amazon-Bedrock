"""Validation router — invokes DQ pipeline via AgentCore or direct import."""

import asyncio
import json
import logging
import os
import sys
import time
import traceback
import uuid
from threading import Thread

import boto3
import botocore.config
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from models import RunValidationRequest

router = APIRouter()
logger = logging.getLogger(__name__)

# In-memory job store
_jobs: dict = {}

AGENT_CODE_PATH = os.environ.get("AGENT_CODE_PATH", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
# Set AGENT_RUNTIME_ARN env var to invoke via AgentCore; leave empty for direct invocation
DEFAULT_AGENT_ARN = os.environ.get("AGENT_RUNTIME_ARN", "")
S3_STAGING_BUCKET = os.environ.get("S3_STAGING_BUCKET", "")


@router.post("/run-validation")
async def run_validation(req: RunValidationRequest):
    """Start a DQ validation pipeline run (async — returns job_id immediately)."""
    job_id = uuid.uuid4().hex[:8]
    pipeline_id = f"WEB-{uuid.uuid4().hex[:8]}"
    _jobs[job_id] = {
        "status": "running",
        "start_time": time.time(),
        "pipeline_id": pipeline_id,
        "result": None,
        "error": None,
    }

    thread = Thread(
        target=_execute_pipeline,
        args=(job_id, req.s3_data_path, req.dry_run, pipeline_id),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "running"}


@router.get("/validation-status/{job_id}")
async def get_validation_status(job_id: str):
    """Poll the status of a running validation job."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    elapsed = time.time() - job["start_time"]
    return {
        "job_id": job_id,
        "status": job["status"],
        "elapsed_seconds": round(elapsed, 1),
    }


@router.get("/validation-results/{job_id}")
async def get_validation_results(job_id: str):
    """Get the results of a completed validation job."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] == "running":
        elapsed = time.time() - job["start_time"]
        return {"status": "running", "elapsed_seconds": round(elapsed, 1)}

    if job["status"] == "error":
        return {
            "status": "error",
            "error": job["error"],
            "elapsed_seconds": round(time.time() - job["start_time"], 1),
        }

    return {"status": "completed", **job["result"]}


@router.get("/validation-stream/{job_id}")
async def validation_stream(job_id: str):
    """SSE endpoint — polls S3 progress and streams stage events."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    pipeline_id = job.get("pipeline_id", "")
    s3_key = f"staging/{pipeline_id}/_progress.json"

    async def event_generator():
        s3 = boto3.client("s3", region_name=AWS_REGION)
        last_updated_at = None

        while True:
            # Check if the job has finished (completed or error)
            current_job = _jobs.get(job_id)
            if not current_job:
                break

            # Try to read progress from S3
            try:
                resp = s3.get_object(Bucket=S3_STAGING_BUCKET, Key=s3_key)
                progress = json.loads(resp["Body"].read().decode("utf-8"))
                updated_at = progress.get("updated_at")

                if updated_at and updated_at != last_updated_at:
                    last_updated_at = updated_at
                    # Send stage_update event
                    yield f"event: stage_update\ndata: {json.dumps(progress)}\n\n"
            except Exception:
                # Progress file doesn't exist yet — silently retry
                pass

            # Check terminal states after sending latest progress
            if current_job["status"] == "completed":
                yield f"event: completed\ndata: {json.dumps({'pipeline_id': pipeline_id})}\n\n"
                break
            elif current_job["status"] == "error":
                yield f"event: error\ndata: {json.dumps({'message': current_job.get('error', 'Unknown error')})}\n\n"
                break

            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _execute_pipeline(job_id: str, s3_data_path: str, dry_run: bool, pipeline_id: str) -> None:
    """Run pipeline in background thread."""
    logger.info("[job=%s] Background thread started (pipeline_id=%s)", job_id, pipeline_id)
    try:
        agent_arn = os.environ.get("AGENT_RUNTIME_ARN", DEFAULT_AGENT_ARN)
        if agent_arn:
            logger.info("[job=%s] Using AgentCore invocation", job_id)
            result = _invoke_agentcore(agent_arn, s3_data_path, dry_run, pipeline_id)
        else:
            logger.info("[job=%s] Using direct invocation", job_id)
            result = _invoke_direct(s3_data_path, dry_run)

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["result"] = result
        logger.info("[job=%s] Completed successfully", job_id)

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("[job=%s] Pipeline failed: %s\n%s", job_id, e, tb)
        # Provide user-friendly error with context
        err_type = type(e).__name__
        err_msg = str(e)
        if "ReadTimeoutError" in err_type or "timeout" in err_msg.lower():
            detail = f"AgentCore 호출 시간 초과: 파이프라인 실행 시간이 제한을 초과했습니다. ({err_msg})"
        elif "EndpointConnectionError" in err_type or "connect" in err_msg.lower():
            detail = f"AgentCore 연결 실패: 서비스에 연결할 수 없습니다. ({err_msg})"
        elif "AgentCore pipeline error" in err_msg:
            detail = err_msg
        else:
            detail = f"{err_type}: {err_msg}"
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = detail


def _invoke_agentcore(agent_arn: str, s3_data_path: str, dry_run: bool, pipeline_id: str) -> dict:
    """Invoke the DQ agent via Bedrock AgentCore Runtime API."""
    # Pipeline processes 100 records through 5 nodes with LLM calls — needs long timeout
    config = botocore.config.Config(
        read_timeout=600,
        connect_timeout=30,
    )
    client = boto3.client("bedrock-agentcore", region_name=AWS_REGION, config=config)

    payload = json.dumps({
        "trigger_type": "schedule",
        "dry_run": dry_run,
        "s3_data_path": s3_data_path,
        "pipeline_id": pipeline_id,
    }).encode("utf-8")

    logger.info("Invoking AgentCore: %s (timeout=600s)", agent_arn)

    response = client.invoke_agent_runtime(
        agentRuntimeArn=agent_arn,
        payload=payload,
        contentType="application/json",
        accept="application/json",
    )

    status_code = response.get("statusCode", 0)
    logger.info("AgentCore response status: %d", status_code)

    # Response body is a StreamingBody
    body = response.get("response", b"")
    if hasattr(body, "read"):
        logger.info("Reading streaming body...")
        body = body.read()
    if isinstance(body, bytes):
        body = body.decode("utf-8")

    logger.info("AgentCore response body length: %d", len(body))

    result = json.loads(body)

    if "error" in result and result["error"]:
        # Include stage info if available
        stage_results = result.get("stage_results", {})
        failed_stages = [k for k, v in stage_results.items() if v.get("status") == "failed"]
        failed_details = []
        for s in failed_stages:
            msg = stage_results[s].get("error_message", "")
            failed_details.append(f"{s}: {msg}" if msg else s)
        stage_info = f" (실패 단계: {'; '.join(failed_details)})" if failed_details else ""
        raise RuntimeError(f"AgentCore pipeline error{stage_info}: {result['error']}")

    logger.info("AgentCore pipeline completed: pipeline_id=%s, suspects=%d, judgments=%d",
                result.get("pipeline_id"), len(result.get("suspects", [])), len(result.get("judgments", [])))

    return result


def _invoke_direct(s3_data_path: str, dry_run: bool) -> dict:
    """Invoke the pipeline directly by importing the agent code."""
    src_path = os.path.join(AGENT_CODE_PATH, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    from ai_dq_agent.main import run_pipeline

    result = run_pipeline(
        trigger_type="schedule",
        dry_run=dry_run,
        s3_data_path=s3_data_path,
    )

    pipeline_state = result.get("pipeline_state", {})
    health = pipeline_state.get("table_health", {})
    validation_stats = result.get("validation_stats", {})
    analysis_stats = result.get("analysis_stats", {})

    # Extract dynamic rules
    all_rules = result.get("rule_mappings", [])
    dynamic_rules = [r for r in all_rules if str(r.get("rule_id", "")).startswith("AUTO-")]

    # Read suspects from S3
    suspects = []
    if result.get("suspects_s3_path"):
        try:
            from ai_dq_agent.tools import s3_read_objects
            resp = s3_read_objects(s3_path=result["suspects_s3_path"], file_format="jsonl")
            suspects = resp.get("records", [])
        except Exception:
            logger.warning("Failed to read suspects from S3")

    # Read judgments from S3
    judgments = []
    if result.get("judgments_s3_path"):
        try:
            from ai_dq_agent.tools import s3_read_objects
            resp = s3_read_objects(s3_path=result["judgments_s3_path"], file_format="jsonl")
            judgments = resp.get("records", [])
        except Exception:
            logger.warning("Failed to read judgments from S3")

    return {
        "pipeline_id": result.get("pipeline_id", "unknown"),
        "health_score": health.get("health_score"),
        "health_status": health.get("status"),
        "stage_results": result.get("stage_results", {}),
        "violation_count": health.get("violation_count", 0),
        "total_records": validation_stats.get("total_scanned", 0),
        "validation_stats": validation_stats,
        "analysis_stats": analysis_stats,
        "suspects": suspects,
        "judgments": judgments,
        "dynamic_rules": dynamic_rules,
    }
