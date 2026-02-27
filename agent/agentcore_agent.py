"""BedrockAgentCoreApp entry point for ai-dq-agent."""

import logging
import os
import sys
import traceback

# Add src/ to Python path so ai_dq_agent package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# Configure logging so all agent logs go to stdout → CloudWatch
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

from bedrock_agentcore.runtime import BedrockAgentCoreApp  # noqa: E402

app = BedrockAgentCoreApp()
logger = logging.getLogger("agentcore_agent")


@app.entrypoint
def invoke(payload, context):
    logger.info("invoke called with payload keys: %s", list(payload.keys()))

    try:
        # Lazy import: avoid loading all agents/tools/boto3 at module init time
        # (AgentCore requires init to complete within 30s)
        from ai_dq_agent.main import run_pipeline

        trigger_type = payload.get("trigger_type", "schedule")
        dry_run = payload.get("dry_run", True)
        event_records = payload.get("event_records")
        s3_data_path = payload.get("s3_data_path")
        pipeline_id = payload.get("pipeline_id")

        logger.info("Starting pipeline: trigger=%s, dry_run=%s, s3_data_path=%s, pipeline_id=%s",
                     trigger_type, dry_run, s3_data_path, pipeline_id)

        result = run_pipeline(
            trigger_type=trigger_type,
            event_records=event_records,
            dry_run=dry_run,
            s3_data_path=s3_data_path,
            pipeline_id=pipeline_id,
        )

        health = result.get("pipeline_state", {}).get("table_health", {})
        validation_stats = result.get("validation_stats", {})
        analysis_stats = result.get("analysis_stats", {})

        # Read back detailed suspects and judgments from S3
        from ai_dq_agent.tools import s3_read_objects

        # Extract dynamic rules (AUTO-prefixed) from rule_mappings
        all_rules = result.get("rule_mappings", [])
        dynamic_rules = [r for r in all_rules if str(r.get("rule_id", "")).startswith("AUTO-")]

        suspects = []
        if result.get("suspects_s3_path"):
            try:
                resp = s3_read_objects(s3_path=result["suspects_s3_path"], file_format="jsonl")
                suspects = resp.get("records", [])
            except Exception:
                logger.warning("Failed to read suspects from S3")

        judgments = []
        if result.get("judgments_s3_path"):
            try:
                resp = s3_read_objects(s3_path=result["judgments_s3_path"], file_format="jsonl")
                judgments = resp.get("records", [])
            except Exception:
                logger.warning("Failed to read judgments from S3")

        response = {
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
        logger.info("Pipeline completed: pipeline_id=%s, suspects=%d, judgments=%d",
                     response["pipeline_id"], len(suspects), len(judgments))
        return response

    except Exception as e:
        logger.error("Pipeline failed: %s\n%s", e, traceback.format_exc())
        return {"error": str(e), "traceback": traceback.format_exc()}


if __name__ == "__main__":
    app.run()
