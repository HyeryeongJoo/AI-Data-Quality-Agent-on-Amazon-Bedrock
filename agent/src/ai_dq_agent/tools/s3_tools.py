"""S3 data read/write tools."""

import json
import logging
import time

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.tools.aws_clients import get_s3_client
from ai_dq_agent.tools.utils import parse_s3_path, retry_with_backoff

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# @tool: s3_read_objects
# ---------------------------------------------------------------------------

@tool
def s3_read_objects(
    s3_path: str,
    file_format: str = "json",
    chunk_size: int = 10000,
    chunk_index: int = 0,
    columns: list[str] | None = None,
    sample_mode: bool = False,
    sample_size: int = 100,
) -> dict:
    """Read data from an S3 object with chunking support.

    Args:
        s3_path: Full S3 path (s3://bucket/key).
        file_format: Data format - 'json' or 'jsonl'.
        chunk_size: Number of records per chunk.
        chunk_index: Which chunk to read (0-based).
        columns: Optional list of columns to include (projection).
        sample_mode: If True, read only sample_size records.
        sample_size: Number of records to read in sample mode.

    Returns:
        Dict with records, total_count, chunk_count, current_chunk.
    """
    start = time.monotonic()
    logger.info("[s3_read_objects] started: path=%s, format=%s, chunk=%d", s3_path, file_format, chunk_index)

    try:
        bucket, key = parse_s3_path(s3_path)
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    s3 = get_s3_client()

    try:
        def _get_object():
            return s3.get_object(Bucket=bucket, Key=key)

        response = retry_with_backoff(
            _get_object,
            max_retries=3,
            retryable_exceptions=(ClientError,),
        )
        body = response["Body"].read().decode("utf-8")

        if file_format == "jsonl":
            lines = [line for line in body.strip().split("\n") if line.strip()]
            all_records = [json.loads(line) for line in lines]
        else:
            all_records = json.loads(body)
            if isinstance(all_records, dict):
                all_records = [all_records]

        total_count = len(all_records)

        if sample_mode:
            records = all_records[:sample_size]
        else:
            start_idx = chunk_index * chunk_size
            records = all_records[start_idx : start_idx + chunk_size]

        # Column projection
        if columns:
            records = [{k: r.get(k) for k in columns} for r in records]

        chunk_count = (total_count + chunk_size - 1) // chunk_size if chunk_size > 0 else 1

        duration = time.monotonic() - start
        logger.info(
            "[s3_read_objects] completed: %d records (chunk %d/%d) in %.2fs",
            len(records), chunk_index, chunk_count, duration,
        )

        return {
            "status": "success",
            "records": records,
            "total_count": total_count,
            "chunk_count": chunk_count,
            "current_chunk": chunk_index,
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[s3_read_objects] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}


# ---------------------------------------------------------------------------
# @tool: s3_write_objects
# ---------------------------------------------------------------------------

@tool
def s3_write_objects(
    s3_path: str,
    data: list[dict] | dict,
    file_format: str = "json",
    append: bool = False,
) -> dict:
    """Write data to an S3 object.

    Args:
        s3_path: Full S3 path (s3://bucket/key).
        data: Data to write (list of dicts for jsonl, or dict/list for json).
        file_format: Data format - 'json' or 'jsonl'.
        append: If True, append to existing file (jsonl only).

    Returns:
        Dict with s3_path, record_count, file_size_bytes, and status.
    """
    start = time.monotonic()
    logger.info("[s3_write_objects] started: path=%s, format=%s, append=%s", s3_path, file_format, append)

    try:
        bucket, key = parse_s3_path(s3_path)
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    s3 = get_s3_client()

    try:
        if append and file_format == "jsonl":
            # Read existing content first
            try:
                existing = s3.get_object(Bucket=bucket, Key=key)
                existing_body = existing["Body"].read().decode("utf-8")
            except ClientError:
                existing_body = ""

            items = data if isinstance(data, list) else [data]
            new_lines = "\n".join(json.dumps(item, ensure_ascii=False) for item in items)
            body = f"{existing_body}\n{new_lines}" if existing_body.strip() else new_lines
            record_count = len(existing_body.strip().split("\n")) + len(items) if existing_body.strip() else len(items)
        elif file_format == "jsonl":
            items = data if isinstance(data, list) else [data]
            body = "\n".join(json.dumps(item, ensure_ascii=False) for item in items)
            record_count = len(items)
        else:
            body = json.dumps(data, ensure_ascii=False, indent=2)
            record_count = len(data) if isinstance(data, list) else 1

        body_bytes = body.encode("utf-8")

        def _put_object():
            s3.put_object(Bucket=bucket, Key=key, Body=body_bytes)

        retry_with_backoff(
            _put_object,
            max_retries=3,
            retryable_exceptions=(ClientError,),
        )

        duration = time.monotonic() - start
        logger.info(
            "[s3_write_objects] completed: %d records, %d bytes in %.2fs",
            record_count, len(body_bytes), duration,
        )

        return {
            "status": "success",
            "s3_path": s3_path,
            "record_count": record_count,
            "file_size_bytes": len(body_bytes),
        }
    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[s3_write_objects] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}
