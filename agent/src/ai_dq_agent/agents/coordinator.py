"""Coordinator Agent — pipeline orchestration and incremental data identification."""

import logging

import boto3

from ai_dq_agent.agents._node_utils import node_wrapper
from ai_dq_agent.models.pipeline import generate_pipeline_id
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    dynamodb_export_to_s3,
    dynamodb_scan_with_rate_limit,
    execution_state_read,
    execution_state_write,
    s3_write_objects,
    slack_send_message,
)

logger = logging.getLogger(__name__)


def _deserialize_dynamodb_item(item: dict) -> dict:
    """Convert DynamoDB JSON format ({"S": "val"}) to plain dict."""
    result = {}
    for key, typed_val in item.items():
        if "S" in typed_val:
            result[key] = typed_val["S"]
        elif "N" in typed_val:
            val = typed_val["N"]
            result[key] = float(val) if "." in val else int(val)
        elif "BOOL" in typed_val:
            result[key] = typed_val["BOOL"]
        elif "NULL" in typed_val:
            result[key] = None
        elif "L" in typed_val:
            result[key] = [_deserialize_dynamodb_item({"_": v}).get("_") for v in typed_val["L"]]
        elif "M" in typed_val:
            result[key] = _deserialize_dynamodb_item(typed_val["M"])
        else:
            result[key] = str(typed_val)
    return result


def _convert_export_to_jsonl(
    s3_bucket: str,
    export_prefix: str,
    output_s3_path: str,
) -> int:
    """Convert DynamoDB Export files (DYNAMODB_JSON) to a single data.jsonl.

    DynamoDB Export writes multiple .json.gz files under the export prefix.
    This reads them all, deserializes from DynamoDB JSON, and writes one JSONL file.

    Returns:
        Number of records written.
    """
    import gzip
    import io
    import json

    s3 = boto3.client("s3")

    # List all exported data files
    paginator = s3.get_paginator("list_objects_v2")
    records = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=export_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue
            resp = s3.get_object(Bucket=s3_bucket, Key=key)
            body = resp["Body"].read()
            text = gzip.decompress(body).decode("utf-8")
            for line in text.strip().split("\n"):
                if not line.strip():
                    continue
                raw = json.loads(line)
                item = raw.get("Item", raw)
                records.append(_deserialize_dynamodb_item(item))

    if not records:
        logger.warning("Export conversion found 0 records under %s", export_prefix)
        return 0

    # Write as JSONL
    from ai_dq_agent.tools import s3_write_objects as _s3_write
    _s3_write(s3_path=output_s3_path, data=records, file_format="jsonl")
    logger.info("Converted %d export records to %s", len(records), output_s3_path)
    return len(records)


TOOLS = [
    dynamodb_export_to_s3,
    dynamodb_scan_with_rate_limit,
    s3_write_objects,
    execution_state_read,
    execution_state_write,
    slack_send_message,
]


@node_wrapper("coordinator")
def invoke_coordinator(state: dict) -> dict:
    """Extract incremental data and set up the pipeline context."""
    result = {**state}
    settings = get_settings()

    trigger_type = state.get("trigger_type", "schedule")
    pipeline_id = state.get("pipeline_id") or generate_pipeline_id(trigger_type)
    result["pipeline_id"] = pipeline_id

    # Read last checkpoint
    checkpoint_resp = execution_state_read(
        state_key="last_checkpoint",
        pipeline_run_id=None,
    )
    last_checkpoint = None
    if checkpoint_resp.get("status") == "success":
        last_checkpoint = checkpoint_resp.get("value", {}).get("timestamp")

    # Record pipeline start
    execution_state_write(
        state_key="pipeline_status",
        value={"status": "running", "trigger_type": trigger_type},
        pipeline_run_id=pipeline_id,
    )

    # Extract data
    s3_prefix = f"staging/{pipeline_id}"
    s3_staging_prefix = f"s3://{settings.s3_staging_bucket}/{s3_prefix}/"

    # --- Data source selection ---
    s3_data_path = state.get("s3_data_path")

    if s3_data_path:
        # Direct S3 path provided — skip DDB entirely
        from ai_dq_agent.tools import s3_read_objects
        read_resp = s3_read_objects(s3_path=s3_data_path, file_format="jsonl")
        records = read_resp.get("records", [])
        total_records = len(records)
        if total_records > 0:
            s3_write_objects(
                s3_path=f"{s3_staging_prefix}data.jsonl",
                data=records,
                file_format="jsonl",
            )
        logger.info("[%s] Using S3 data source: %s (%d records)", pipeline_id, s3_data_path, total_records)

    elif trigger_type == "event" and state.get("event_records"):
        # Event-driven: use provided records directly
        records = state["event_records"]
        s3_write_objects(
            s3_path=f"{s3_staging_prefix}data.jsonl",
            data=records,
            file_format="jsonl",
        )
        total_records = len(records)

    else:
        # Batch: try export, fallback to scan
        table_arn = f"arn:aws:dynamodb:{settings.aws_region}:{boto3.client('sts').get_caller_identity()['Account']}:table/{settings.dynamodb_table_name}"
        export_resp = dynamodb_export_to_s3(
            table_arn=table_arn,
            s3_bucket=settings.s3_staging_bucket,
            s3_prefix=s3_prefix,
        )

        if export_resp.get("status") == "completed":
            # Convert DynamoDB Export format (AWSDynamoDB/.../data/*.json.gz) to data.jsonl
            total_records = _convert_export_to_jsonl(
                s3_bucket=settings.s3_staging_bucket,
                export_prefix=s3_prefix,
                output_s3_path=f"{s3_staging_prefix}data.jsonl",
            )
        else:
            logger.warning(
                "[%s] Export failed, falling back to scan: %s",
                pipeline_id,
                export_resp.get("error", "unknown"),
            )
            scan_resp = dynamodb_scan_with_rate_limit(
                table_name=settings.dynamodb_table_name,
                max_rcu_per_second=100,
            )
            records = scan_resp.get("records", [])
            total_records = len(records)

            if total_records > 0:
                s3_write_objects(
                    s3_path=f"{s3_staging_prefix}data.jsonl",
                    data=records,
                    file_format="jsonl",
                )

    result["s3_staging_prefix"] = s3_staging_prefix
    result["total_records"] = total_records
    result["checkpoint_timestamp"] = last_checkpoint

    # Early exit on 0 records
    if total_records == 0:
        slack_send_message(
            channel=settings.slack_channel_id,
            message=f"[DQ Agent] Pipeline {pipeline_id}: 검증 대상 데이터가 없습니다.",
        )
        execution_state_write(
            state_key="pipeline_status",
            value={"status": "completed", "reason": "no_data"},
            pipeline_run_id=pipeline_id,
        )
        result["_early_exit"] = True
        result["_records_processed"] = 0
        return result

    result["_records_processed"] = total_records
    return result
