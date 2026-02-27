"""Data profiling tools — compute column statistics and manage profile history."""

import json
import logging
import math
import time
from collections import Counter
from datetime import datetime, timezone

from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def profile_compute(
    records: list[dict],
    columns: list[str] | None = None,
    top_n: int = 10,
) -> dict:
    """Compute statistical profiles for each column in the dataset.

    For each column, calculates: null_rate, unique_count, min, max, mean,
    stddev, and top-N value distribution.

    Args:
        records: List of record dicts to profile.
        columns: Optional list of columns to profile. If None, profiles all columns.
        top_n: Number of top frequent values to include.

    Returns:
        Dict with column_profiles list, total_records, and timestamp.
    """
    start = time.monotonic()
    logger.info("[profile_compute] started: records=%d, columns=%s", len(records), columns)

    if not records:
        return {"status": "success", "column_profiles": [], "total_records": 0, "timestamp": _now_iso()}

    all_columns = columns or sorted({k for rec in records for k in rec.keys()})
    total = len(records)
    profiles = []

    for col in all_columns:
        values = [rec.get(col) for rec in records]
        non_null = [v for v in values if v is not None]
        null_count = total - len(non_null)
        null_rate = null_count / total if total > 0 else 0.0

        unique_values = set()
        for v in non_null:
            try:
                unique_values.add(v)
            except TypeError:
                unique_values.add(str(v))
        unique_count = len(unique_values)
        uniqueness_rate = unique_count / len(non_null) if non_null else 0.0

        # Numeric stats
        numeric_vals = []
        for v in non_null:
            try:
                numeric_vals.append(float(v))
            except (ValueError, TypeError):
                pass

        min_val = min(numeric_vals) if numeric_vals else None
        max_val = max(numeric_vals) if numeric_vals else None
        mean_val = sum(numeric_vals) / len(numeric_vals) if numeric_vals else None
        stddev_val = None
        if len(numeric_vals) >= 2 and mean_val is not None:
            variance = sum((x - mean_val) ** 2 for x in numeric_vals) / len(numeric_vals)
            stddev_val = math.sqrt(variance)

        # If not numeric, use string min/max
        if min_val is None and non_null:
            try:
                str_vals = [str(v) for v in non_null]
                min_val = min(str_vals)
                max_val = max(str_vals)
            except TypeError:
                pass

        # Top values
        counter = Counter()
        for v in non_null:
            try:
                counter[v] += 1
            except TypeError:
                counter[str(v)] += 1
        top_values = [{"value": str(val), "count": cnt} for val, cnt in counter.most_common(top_n)]

        profiles.append({
            "column_name": col,
            "timestamp": _now_iso(),
            "total_count": total,
            "null_count": null_count,
            "null_rate": round(null_rate, 6),
            "unique_count": unique_count,
            "uniqueness_rate": round(uniqueness_rate, 6),
            "min_value": min_val,
            "max_value": max_val,
            "mean_value": round(mean_val, 4) if mean_val is not None else None,
            "stddev_value": round(stddev_val, 4) if stddev_val is not None else None,
            "top_values": top_values,
        })

    duration = time.monotonic() - start
    logger.info("[profile_compute] completed: %d columns profiled in %.2fs", len(profiles), duration)

    return {
        "status": "success",
        "column_profiles": profiles,
        "total_records": total,
        "timestamp": _now_iso(),
    }


@tool
def profile_history_read(
    table_name: str,
    column_name: str,
    lookback_days: int = 30,
) -> dict:
    """Read historical profile data for a specific column from DynamoDB.

    Args:
        table_name: Source table name.
        column_name: Column name to look up history for.
        lookback_days: Number of days to look back.

    Returns:
        Dict with history list (sorted by timestamp), count, and status.
    """
    start = time.monotonic()
    pk = f"{table_name}#{column_name}"
    logger.info("[profile_history_read] started: pk=%s, lookback=%d days", pk, lookback_days)

    settings = get_settings()
    client = get_dynamodb_client()

    cutoff = datetime.now(timezone.utc).timestamp() - (lookback_days * 86400)
    cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()

    try:
        response = client.query(
            TableName=settings.dynamodb_profile_table,
            KeyConditionExpression="pk = :pk AND sk >= :cutoff",
            ExpressionAttributeValues={
                ":pk": {"S": pk},
                ":cutoff": {"S": cutoff_iso},
            },
            ScanIndexForward=True,
        )

        history = []
        for item in response.get("Items", []):
            data_str = item.get("data", {}).get("S", "{}")
            history.append(json.loads(data_str))

        duration = time.monotonic() - start
        logger.info("[profile_history_read] completed: %d entries in %.2fs", len(history), duration)

        return {
            "status": "success",
            "history": history,
            "count": len(history),
            "partition_key": pk,
        }
    except Exception as e:
        duration = time.monotonic() - start
        logger.error("[profile_history_read] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e), "history": [], "count": 0}


@tool
def profile_history_write(
    table_name: str,
    profiles: list[dict],
) -> dict:
    """Write current column profiles to DynamoDB for historical tracking.

    Args:
        table_name: Source table name.
        profiles: List of column profile dicts (from profile_compute).

    Returns:
        Dict with written_count and status.
    """
    start = time.monotonic()
    logger.info("[profile_history_write] started: table=%s, profiles=%d", table_name, len(profiles))

    settings = get_settings()
    client = get_dynamodb_client()
    written = 0
    timestamp = _now_iso()

    for profile in profiles:
        col_name = profile.get("column_name", "")
        pk = f"{table_name}#{col_name}"

        try:
            client.put_item(
                TableName=settings.dynamodb_profile_table,
                Item={
                    "pk": {"S": pk},
                    "sk": {"S": timestamp},
                    "data": {"S": json.dumps(profile, ensure_ascii=False)},
                    "table_name": {"S": table_name},
                    "column_name": {"S": col_name},
                    "created_at": {"S": timestamp},
                },
            )
            written += 1
        except Exception as e:
            logger.error("[profile_history_write] failed for %s: %s", pk, e)

    duration = time.monotonic() - start
    logger.info("[profile_history_write] completed: %d/%d written in %.2fs", written, len(profiles), duration)

    return {"status": "success", "written_count": written}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
