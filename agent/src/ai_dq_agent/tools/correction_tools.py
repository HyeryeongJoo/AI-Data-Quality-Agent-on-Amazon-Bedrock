"""Correction audit trail tools."""

import json
import logging
import time
from uuid import uuid4

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def snapshot_save(
    pipeline_run_id: str,
    records: list[dict],
) -> dict:
    """Save a pre-correction snapshot to DynamoDB for audit trail.

    Args:
        pipeline_run_id: Pipeline execution ID.
        records: Original records to snapshot before correction.

    Returns:
        Dict with snapshot_id, record_count, and created_at.
    """
    start = time.monotonic()
    logger.info("[snapshot_save] started: pipeline=%s, records=%d", pipeline_run_id, len(records))

    settings = get_settings()
    client = get_dynamodb_client()
    snapshot_id = uuid4().hex[:12]
    created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        client.put_item(
            TableName=settings.dynamodb_correction_table,
            Item={
                "pipeline_id": {"S": pipeline_run_id},
                "sort_key": {"S": f"SNAPSHOT#{snapshot_id}"},
                "data": {"S": json.dumps(records, ensure_ascii=False)},
                "record_count": {"N": str(len(records))},
                "created_at": {"S": created_at},
            },
        )

        duration = time.monotonic() - start
        logger.info("[snapshot_save] completed: id=%s in %.2fs", snapshot_id, duration)

        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "record_count": len(records),
            "created_at": created_at,
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[snapshot_save] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}


@tool
def correction_log_write(
    pipeline_run_id: str,
    corrections: list[dict],
    approved_by: str,
    approved_at: str,
    snapshot_id: str,
) -> dict:
    """Write correction records to DynamoDB for audit trail.

    Args:
        pipeline_run_id: Pipeline execution ID.
        corrections: List of correction dicts (record_id, column, original, corrected, etc.).
        approved_by: Approver identifier (Slack user_id).
        approved_at: Approval timestamp (ISO 8601).
        snapshot_id: Reference to the pre-correction snapshot.

    Returns:
        Dict with log_id, written_count, and status.
    """
    start = time.monotonic()
    logger.info("[correction_log_write] started: pipeline=%s, corrections=%d", pipeline_run_id, len(corrections))

    settings = get_settings()
    client = get_dynamodb_client()
    log_id = uuid4().hex[:12]
    written = 0

    for correction in corrections:
        correction_id = uuid4().hex[:8]
        try:
            client.put_item(
                TableName=settings.dynamodb_correction_table,
                Item={
                    "pipeline_id": {"S": pipeline_run_id},
                    "sort_key": {"S": f"CORRECTION#{correction_id}"},
                    "data": {"S": json.dumps(correction, ensure_ascii=False)},
                    "approved_by": {"S": approved_by},
                    "approved_at": {"S": approved_at},
                    "snapshot_id": {"S": snapshot_id},
                    "log_id": {"S": log_id},
                    "created_at": {"S": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                },
            )
            written += 1
        except ClientError as e:
            logger.error("[correction_log_write] put_item failed: %s", e)

    duration = time.monotonic() - start
    logger.info("[correction_log_write] completed: %d written in %.2fs", written, duration)

    return {"status": "success", "log_id": log_id, "written_count": written}


@tool
def feedback_log_write(
    pipeline_run_id: str,
    feedbacks: list[dict],
) -> dict:
    """Write reviewer feedback records to DynamoDB.

    Args:
        pipeline_run_id: Pipeline execution ID.
        feedbacks: List of feedback dicts with record_id, reviewer_action,
            rejection_reason, etc.

    Returns:
        Dict with written_count and status.
    """
    start = time.monotonic()
    logger.info("[feedback_log_write] started: pipeline=%s, feedbacks=%d", pipeline_run_id, len(feedbacks))

    settings = get_settings()
    client = get_dynamodb_client()
    written = 0

    for fb in feedbacks:
        record_id = fb.get("record_id", uuid4().hex[:8])
        try:
            client.put_item(
                TableName=settings.dynamodb_correction_table,
                Item={
                    "pipeline_id": {"S": pipeline_run_id},
                    "sort_key": {"S": f"FEEDBACK#{record_id}"},
                    "data": {"S": json.dumps(fb, ensure_ascii=False)},
                    "created_at": {"S": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                },
            )
            written += 1
        except ClientError as e:
            logger.error("[feedback_log_write] put_item failed: %s", e)

    duration = time.monotonic() - start
    logger.info("[feedback_log_write] completed: %d written in %.2fs", written, duration)

    return {"status": "success", "written_count": written}
