"""DynamoDB data extraction and write tools."""

import logging
import time

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client
from ai_dq_agent.tools.utils import (
    DYNAMODB_RETRYABLE,
    RateLimiter,
    is_retryable_client_error,
    retry_with_backoff,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# @tool: dynamodb_export_to_s3
# ---------------------------------------------------------------------------

@tool
def dynamodb_export_to_s3(
    table_arn: str,
    s3_bucket: str,
    s3_prefix: str,
    export_type: str = "FULL_EXPORT",
    from_timestamp: float | None = None,
    to_timestamp: float | None = None,
) -> dict:
    """Export a DynamoDB table to S3 using point-in-time recovery.

    Args:
        table_arn: ARN of the DynamoDB table to export.
        s3_bucket: Target S3 bucket name.
        s3_prefix: Target S3 key prefix.
        export_type: FULL_EXPORT or INCREMENTAL_EXPORT.
        from_timestamp: Start time for incremental export (epoch seconds).
        to_timestamp: End time for incremental export (epoch seconds).

    Returns:
        Dict with export_arn, s3_path, record_count, and status.
    """
    start = time.monotonic()
    logger.info("[dynamodb_export_to_s3] started: table=%s, type=%s", table_arn, export_type)

    client = get_dynamodb_client()
    try:
        params = {
            "TableArn": table_arn,
            "S3Bucket": s3_bucket,
            "S3Prefix": s3_prefix,
            "ExportFormat": "DYNAMODB_JSON",
        }
        if export_type == "INCREMENTAL_EXPORT" and from_timestamp and to_timestamp:
            params["ExportType"] = "INCREMENTAL_EXPORT"
            params["IncrementalExportSpecification"] = {
                "ExportFromTime": from_timestamp,
                "ExportToTime": to_timestamp,
            }

        response = client.export_table_to_point_in_time(**params)
        export_arn = response["ExportDescription"]["ExportArn"]

        # Poll until complete
        status = "IN_PROGRESS"
        while status == "IN_PROGRESS":
            time.sleep(2)
            desc = client.describe_export(ExportArn=export_arn)
            status = desc["ExportDescription"]["ExportStatus"]

        if status != "COMPLETED":
            duration = time.monotonic() - start
            logger.error("[dynamodb_export_to_s3] failed: status=%s in %.2fs", status, duration)
            return {"status": "failed", "error": f"Export ended with status: {status}"}

        record_count = desc["ExportDescription"].get("ItemCount", 0)
        s3_path = f"s3://{s3_bucket}/{s3_prefix}"

        duration = time.monotonic() - start
        logger.info("[dynamodb_export_to_s3] completed: %d records in %.2fs", record_count, duration)

        return {
            "status": "completed",
            "export_arn": export_arn,
            "s3_path": s3_path,
            "record_count": record_count,
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[dynamodb_export_to_s3] failed: %s in %.2fs", e, duration)
        return {"status": "failed", "error": str(e)}


# ---------------------------------------------------------------------------
# @tool: dynamodb_stream_read
# ---------------------------------------------------------------------------

@tool
def dynamodb_stream_read(
    table_name: str,
    shard_iterator_type: str = "LATEST",
    last_sequence_number: str | None = None,
    max_records: int = 1000,
) -> dict:
    """Read records from a DynamoDB Stream.

    Args:
        table_name: Name of the DynamoDB table with streams enabled.
        shard_iterator_type: LATEST, TRIM_HORIZON, or AFTER_SEQUENCE_NUMBER.
        last_sequence_number: Sequence number to start after (for AFTER_SEQUENCE_NUMBER).
        max_records: Maximum number of records to return.

    Returns:
        Dict with records list, next_sequence_number, and record_count.
    """
    start = time.monotonic()
    logger.info("[dynamodb_stream_read] started: table=%s, type=%s", table_name, shard_iterator_type)

    client = get_dynamodb_client()
    try:
        # Describe table to get stream ARN
        table_desc = client.describe_table(TableName=table_name)
        stream_arn = table_desc["Table"].get("LatestStreamArn")
        if not stream_arn:
            return {"status": "error", "error": "Stream not enabled on table", "records": [], "record_count": 0}

        streams_client = get_dynamodb_client()  # DynamoDB Streams uses same client in moto
        import boto3
        from botocore.config import Config
        settings = get_settings()
        streams_client = boto3.client(
            "dynamodbstreams",
            region_name=settings.aws_region,
            config=Config(retries={"mode": "standard", "max_attempts": 3}),
        )

        # Get shards
        stream_desc = streams_client.describe_stream(StreamArn=stream_arn)
        shards = stream_desc["StreamDescription"]["Shards"]
        if not shards:
            return {"status": "success", "records": [], "record_count": 0, "next_sequence_number": None}

        # Use the last shard
        shard_id = shards[-1]["ShardId"]

        iterator_params = {"StreamArn": stream_arn, "ShardId": shard_id, "ShardIteratorType": shard_iterator_type}
        if shard_iterator_type == "AFTER_SEQUENCE_NUMBER" and last_sequence_number:
            iterator_params["SequenceNumber"] = last_sequence_number

        shard_iter_resp = streams_client.get_shard_iterator(**iterator_params)
        shard_iterator = shard_iter_resp["ShardIterator"]

        # Read records
        all_records = []
        while shard_iterator and len(all_records) < max_records:
            resp = streams_client.get_records(ShardIterator=shard_iterator, Limit=min(max_records - len(all_records), 1000))
            for r in resp.get("Records", []):
                if r["eventName"] in ("INSERT", "MODIFY"):
                    all_records.append(r.get("dynamodb", {}).get("NewImage", {}))
            shard_iterator = resp.get("NextShardIterator")
            if not resp.get("Records"):
                break

        next_seq = None
        if resp.get("Records"):
            next_seq = resp["Records"][-1].get("dynamodb", {}).get("SequenceNumber")

        duration = time.monotonic() - start
        logger.info("[dynamodb_stream_read] completed: %d records in %.2fs", len(all_records), duration)

        return {
            "status": "success",
            "records": all_records,
            "record_count": len(all_records),
            "next_sequence_number": next_seq,
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[dynamodb_stream_read] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e), "records": [], "record_count": 0}


# ---------------------------------------------------------------------------
# @tool: dynamodb_scan_with_rate_limit
# ---------------------------------------------------------------------------

@tool
def dynamodb_scan_with_rate_limit(
    table_name: str,
    filter_expression: str | None = None,
    expression_attribute_values: dict | None = None,
    expression_attribute_names: dict | None = None,
    max_rcu_per_second: float = 100.0,
    page_size: int = 1000,
    max_pages: int | None = None,
) -> dict:
    """Scan a DynamoDB table with rate limiting to avoid throttling.

    Args:
        table_name: Name of the DynamoDB table to scan.
        filter_expression: Optional FilterExpression string.
        expression_attribute_values: Optional ExpressionAttributeValues dict.
        expression_attribute_names: Optional ExpressionAttributeNames dict.
        max_rcu_per_second: Maximum RCU consumption per second.
        page_size: Number of items per page (Limit).
        max_pages: Maximum number of pages to scan (None for all).

    Returns:
        Dict with records, scanned_count, and status.
    """
    start = time.monotonic()
    logger.info("[dynamodb_scan_with_rate_limit] started: table=%s, page_size=%d", table_name, page_size)

    client = get_dynamodb_client()
    rate_limiter = RateLimiter(max_rcu_per_second / page_size) if max_rcu_per_second > 0 else None

    all_records = []
    pages_scanned = 0
    last_key = None

    try:
        while True:
            if rate_limiter:
                rate_limiter.wait()

            params = {
                "TableName": table_name,
                "Limit": page_size,
                "ReturnConsumedCapacity": "TOTAL",
            }
            if filter_expression:
                params["FilterExpression"] = filter_expression
            if expression_attribute_values:
                params["ExpressionAttributeValues"] = expression_attribute_values
            if expression_attribute_names:
                params["ExpressionAttributeNames"] = expression_attribute_names
            if last_key:
                params["ExclusiveStartKey"] = last_key

            def _do_scan():
                return client.scan(**params)

            response = retry_with_backoff(
                _do_scan,
                max_retries=3,
                retryable_exceptions=(ClientError,),
            )

            items = response.get("Items", [])
            all_records.extend(items)
            pages_scanned += 1

            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            if max_pages and pages_scanned >= max_pages:
                break

        duration = time.monotonic() - start
        logger.info(
            "[dynamodb_scan_with_rate_limit] completed: %d records, %d pages in %.2fs",
            len(all_records), pages_scanned, duration,
        )

        return {
            "status": "success",
            "records": all_records,
            "scanned_count": len(all_records),
            "pages_scanned": pages_scanned,
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[dynamodb_scan_with_rate_limit] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e), "records": all_records, "scanned_count": len(all_records)}


# ---------------------------------------------------------------------------
# @tool: dynamodb_batch_write
# ---------------------------------------------------------------------------

@tool
def dynamodb_batch_write(
    table_name: str,
    items: list[dict],
    batch_size: int = 25,
    max_retries: int = 3,
) -> dict:
    """Write items to DynamoDB in batches of 25.

    Args:
        table_name: Target DynamoDB table name.
        items: List of item dicts to write (PutRequest format values).
        batch_size: Batch size (max 25 per DynamoDB limit).
        max_retries: Maximum retry count for unprocessed items.

    Returns:
        Dict with success_count, failure_count, failures, and status.
    """
    start = time.monotonic()
    logger.info("[dynamodb_batch_write] started: table=%s, items=%d", table_name, len(items))

    client = get_dynamodb_client()
    batch_size = min(batch_size, 25)  # DynamoDB limit
    success_count = 0
    failure_count = 0
    failures = []

    for i in range(0, len(items), batch_size):
        chunk = items[i : i + batch_size]
        request_items = {table_name: [{"PutRequest": {"Item": item}} for item in chunk]}

        retries = 0
        while request_items:
            try:
                response = client.batch_write_item(RequestItems=request_items)
                unprocessed = response.get("UnprocessedItems", {})
                processed = len(chunk) - len(unprocessed.get(table_name, []))
                success_count += processed

                if not unprocessed or not unprocessed.get(table_name):
                    break

                retries += 1
                if retries > max_retries:
                    failed_items = unprocessed.get(table_name, [])
                    failure_count += len(failed_items)
                    failures.extend([{"item": fi, "error": "max retries exceeded"} for fi in failed_items])
                    break

                request_items = unprocessed
                delay = min(1.0 * (2**retries), 30.0)
                time.sleep(delay)

            except ClientError as e:
                if is_retryable_client_error(e, DYNAMODB_RETRYABLE) and retries < max_retries:
                    retries += 1
                    delay = min(1.0 * (2**retries), 30.0)
                    time.sleep(delay)
                else:
                    failure_count += len(chunk)
                    failures.extend([{"error": str(e)}])
                    break

    duration = time.monotonic() - start
    status = "completed" if failure_count == 0 else ("partial_failure" if success_count > 0 else "all_failed")
    logger.info(
        "[dynamodb_batch_write] %s: %d success, %d failures in %.2fs",
        status, success_count, failure_count, duration,
    )

    return {
        "status": status,
        "success_count": success_count,
        "failure_count": failure_count,
        "failures": failures,
    }
