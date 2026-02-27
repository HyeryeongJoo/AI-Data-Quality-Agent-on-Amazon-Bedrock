"""DQ report generation tool."""

import json
import logging
import time
from datetime import datetime, timezone

from strands import tool

from ai_dq_agent.tools.aws_clients import get_s3_client

logger = logging.getLogger(__name__)


@tool
def report_generate(
    pipeline_id: str,
    total_scanned: int,
    total_suspects: int,
    total_errors: int,
    high_confidence_errors: int,
    correction_proposals: int,
    error_type_distribution: dict,
    s3_bucket: str,
    s3_prefix: str,
) -> dict:
    """Generate a DQ report and upload it to S3.

    Args:
        pipeline_id: Pipeline execution ID.
        total_scanned: Total records scanned.
        total_suspects: Total suspect items found by rule-based validation.
        total_errors: Total confirmed errors from LLM analysis.
        high_confidence_errors: Errors with HIGH confidence.
        correction_proposals: Number of items proposed for correction.
        error_type_distribution: Error count by type dict.
        s3_bucket: Target S3 bucket.
        s3_prefix: Target S3 key prefix (e.g. 'reports/{pipeline_id}').

    Returns:
        Dict with report_s3_path, summary, and status.
    """
    start = time.monotonic()
    logger.info("[report_generate] started: pipeline=%s", pipeline_id)

    filtering_ratio = total_suspects / total_scanned if total_scanned > 0 else 0.0
    created_at = datetime.now(timezone.utc).isoformat()

    report = {
        "pipeline_id": pipeline_id,
        "created_at": created_at,
        "summary": {
            "total_scanned": total_scanned,
            "total_suspects": total_suspects,
            "total_errors": total_errors,
            "high_confidence_errors": high_confidence_errors,
            "correction_proposals": correction_proposals,
            "error_type_distribution": error_type_distribution,
            "filtering_ratio": round(filtering_ratio, 6),
        },
    }

    s3 = get_s3_client()
    report_key = f"{s3_prefix}/dq_report.json"
    report_body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")

    try:
        s3.put_object(Bucket=s3_bucket, Key=report_key, Body=report_body)
        report_s3_path = f"s3://{s3_bucket}/{report_key}"

        duration = time.monotonic() - start
        logger.info("[report_generate] completed: %s in %.2fs", report_s3_path, duration)

        return {
            "status": "success",
            "report_s3_path": report_s3_path,
            "summary": report["summary"],
            "created_at": created_at,
        }
    except Exception as e:
        duration = time.monotonic() - start
        logger.error("[report_generate] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}
