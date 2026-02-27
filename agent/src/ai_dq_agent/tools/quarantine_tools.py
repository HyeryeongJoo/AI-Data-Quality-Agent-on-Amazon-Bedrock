"""Quarantine tools — isolate bad data records."""

import json
import logging
import time
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def quarantine_write(
    records: list[dict],
    reason: str,
    pipeline_id: str = "",
) -> dict:
    """Write bad data records to a quarantine DynamoDB table.

    Records are isolated from the main dataset and tagged with the reason
    for quarantine and the pipeline that identified them.

    Args:
        records: List of record dicts to quarantine.
        reason: Description of why these records are quarantined.
        pipeline_id: Pipeline execution ID that triggered quarantine.

    Returns:
        Dict with quarantined_count, failure_count, and status.
    """
    start = time.monotonic()
    logger.info("[quarantine_write] started: %d records, reason=%s", len(records), reason)

    settings = get_settings()
    client = get_dynamodb_client()
    quarantined = 0
    failures = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for record in records:
        record_id = str(record.get("record_id", record.get("id", "")))
        try:
            client.put_item(
                TableName=settings.dynamodb_quarantine_table,
                Item={
                    "record_id": {"S": record_id},
                    "quarantined_at": {"S": now_iso},
                    "pipeline_id": {"S": pipeline_id},
                    "reason": {"S": reason},
                    "original_data": {"S": json.dumps(record, ensure_ascii=False)},
                    "status": {"S": "quarantined"},
                },
            )
            quarantined += 1
        except ClientError as e:
            logger.error("[quarantine_write] failed for %s: %s", record_id, e)
            failures += 1

    # Flag original records in source table
    flagged = 0
    for record in records:
        record_id = str(record.get("record_id", record.get("id", "")))
        try:
            client.update_item(
                TableName=settings.dynamodb_table_name,
                Key={"record_id": {"S": record_id}},
                UpdateExpression="SET #q = :val, #qat = :ts, #qr = :reason",
                ExpressionAttributeNames={
                    "#q": "_quarantined",
                    "#qat": "_quarantined_at",
                    "#qr": "_quarantine_reason",
                },
                ExpressionAttributeValues={
                    ":val": {"BOOL": True},
                    ":ts": {"S": now_iso},
                    ":reason": {"S": reason},
                },
            )
            flagged += 1
        except ClientError as e:
            logger.warning("[quarantine_write] flag failed for %s: %s", record_id, e)

    duration = time.monotonic() - start
    logger.info(
        "[quarantine_write] completed: %d quarantined, %d flagged, %d failures in %.2fs",
        quarantined, flagged, failures, duration,
    )

    return {
        "status": "success",
        "quarantined_count": quarantined,
        "flagged_count": flagged,
        "failure_count": failures,
    }
