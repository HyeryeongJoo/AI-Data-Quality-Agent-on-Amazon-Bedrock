"""Pipeline execution state management tools."""

import json
import logging
import time

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def execution_state_read(
    state_key: str,
    pipeline_run_id: str | None = None,
) -> dict:
    """Read pipeline execution state from DynamoDB.

    Args:
        state_key: State key (e.g. 'pipeline_status', 'last_checkpoint').
        pipeline_run_id: Specific pipeline run ID, or None for 'LATEST'.

    Returns:
        Dict with value, updated_at, and metadata.
    """
    start = time.monotonic()
    sk = pipeline_run_id or "LATEST"
    logger.info("[execution_state_read] started: key=%s, sk=%s", state_key, sk)

    settings = get_settings()
    client = get_dynamodb_client()

    try:
        response = client.get_item(
            TableName=settings.dynamodb_state_table,
            Key={
                "state_key": {"S": state_key},
                "sort_key": {"S": sk},
            },
        )

        item = response.get("Item")
        if not item:
            return {"status": "not_found", "state_key": state_key, "sort_key": sk}

        value_str = item.get("value", {}).get("S", "{}")
        updated_at = item.get("updated_at", {}).get("S", "")
        metadata_str = item.get("metadata", {}).get("S", "{}")

        duration = time.monotonic() - start
        logger.info("[execution_state_read] completed in %.2fs", duration)

        return {
            "status": "success",
            "state_key": state_key,
            "sort_key": sk,
            "value": json.loads(value_str),
            "updated_at": updated_at,
            "metadata": json.loads(metadata_str),
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[execution_state_read] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}


@tool
def execution_state_write(
    state_key: str,
    value: dict,
    pipeline_run_id: str,
    metadata: dict | None = None,
) -> dict:
    """Write pipeline execution state to DynamoDB.

    Writes to both the specific pipeline_run_id and the 'LATEST' record.

    Args:
        state_key: State key (e.g. 'pipeline_status', 'last_checkpoint').
        value: State value as dict.
        pipeline_run_id: Pipeline run identifier.
        metadata: Optional metadata dict.

    Returns:
        Dict with success status and updated_at.
    """
    start = time.monotonic()
    logger.info("[execution_state_write] started: key=%s, pipeline=%s", state_key, pipeline_run_id)

    settings = get_settings()
    client = get_dynamodb_client()
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    item = {
        "state_key": {"S": state_key},
        "sort_key": {"S": pipeline_run_id},
        "value": {"S": json.dumps(value, ensure_ascii=False)},
        "updated_at": {"S": updated_at},
    }
    if metadata:
        item["metadata"] = {"S": json.dumps(metadata, ensure_ascii=False)}

    try:
        # Write specific pipeline record
        client.put_item(TableName=settings.dynamodb_state_table, Item=item)

        # Also update LATEST record (BR-T11-3)
        latest_item = dict(item)
        latest_item["sort_key"] = {"S": "LATEST"}
        client.put_item(TableName=settings.dynamodb_state_table, Item=latest_item)

        duration = time.monotonic() - start
        logger.info("[execution_state_write] completed in %.2fs", duration)

        return {"status": "success", "updated_at": updated_at}
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[execution_state_write] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}
