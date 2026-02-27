"""Root cause tracing tools — trace violations back to upstream sources."""

import json
import logging
import time

from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def root_cause_trace(
    violation: dict,
    lineage_info: dict,
    profile_history: dict | None = None,
) -> dict:
    """Trace the root cause of a violation through upstream lineage.

    Logic:
    1. Identify the violated column(s).
    2. Look up column lineage to find source table/column.
    3. Check if the upstream column's profile has recently changed.
    4. If profile change detected, flag upstream as root cause.

    Args:
        violation: Violation dict with rule_id, target_columns, error_type.
        lineage_info: Lineage data from lineage_read.
        profile_history: Optional profile history for upstream tables.
            If None, attempts to read from DynamoDB.

    Returns:
        Dict with root_cause_table, root_cause_column, confidence, description.
    """
    start = time.monotonic()
    rule_id = violation.get("rule_id", "")
    target_cols = violation.get("target_columns", [])
    logger.info("[root_cause_trace] started: rule=%s, columns=%s", rule_id, target_cols)

    column_lineage = lineage_info.get("column_lineage", {})
    upstream_tables = lineage_info.get("upstream_tables", [])

    # Step 1: Find upstream source for violated columns
    traced_sources = []
    for col in target_cols:
        source = column_lineage.get(col)
        if source:
            # Expected format: "upstream_table.column_name"
            parts = source.split(".", 1)
            if len(parts) == 2:
                traced_sources.append({
                    "violation_column": col,
                    "source_table": parts[0],
                    "source_column": parts[1],
                })

    if not traced_sources:
        # No column lineage found — check if any upstream tables exist
        if upstream_tables:
            duration = time.monotonic() - start
            logger.info("[root_cause_trace] no column lineage, %d upstream tables in %.2fs", len(upstream_tables), duration)
            return {
                "status": "partial",
                "root_cause_table": upstream_tables[0] if upstream_tables else "",
                "root_cause_column": "",
                "confidence": "LOW",
                "description": f"No column lineage available. Upstream tables: {upstream_tables}",
                "profile_change_detected": False,
            }
        else:
            duration = time.monotonic() - start
            logger.info("[root_cause_trace] no upstream lineage in %.2fs", duration)
            return {
                "status": "no_lineage",
                "root_cause_table": "",
                "root_cause_column": "",
                "confidence": "LOW",
                "description": "No upstream lineage information available.",
                "profile_change_detected": False,
            }

    # Step 2: Check profile history for upstream changes
    settings = get_settings()
    client = get_dynamodb_client()
    best_cause = None
    best_score = 0.0

    for source in traced_sources:
        src_table = source["source_table"]
        src_col = source["source_column"]
        pk = f"{src_table}#{src_col}"

        # Read recent profile history (last 2 entries to compare)
        try:
            if profile_history and pk in profile_history:
                history = profile_history[pk]
            else:
                resp = client.query(
                    TableName=settings.dynamodb_profile_table,
                    KeyConditionExpression="pk = :pk",
                    ExpressionAttributeValues={":pk": {"S": pk}},
                    ScanIndexForward=False,
                    Limit=2,
                )
                history = [json.loads(item.get("data", {}).get("S", "{}")) for item in resp.get("Items", [])]

            profile_changed = False
            change_desc = ""
            if len(history) >= 2:
                recent = history[0]
                previous = history[1]
                # Compare null_rate, unique_count changes
                null_rate_change = abs(recent.get("null_rate", 0) - previous.get("null_rate", 0))
                unique_change = abs(recent.get("unique_count", 0) - previous.get("unique_count", 0))
                prev_unique = previous.get("unique_count", 1) or 1

                if null_rate_change > 0.05:
                    profile_changed = True
                    change_desc = f"null_rate changed by {null_rate_change:.2%}"
                elif unique_change / prev_unique > 0.2:
                    profile_changed = True
                    change_desc = f"unique_count changed by {unique_change / prev_unique:.0%}"

            confidence = "HIGH" if profile_changed else "MEDIUM"
            score = 2.0 if profile_changed else 1.0

            if score > best_score:
                best_score = score
                best_cause = {
                    "root_cause_table": src_table,
                    "root_cause_column": src_col,
                    "confidence": confidence,
                    "description": (
                        f"Upstream {src_table}.{src_col} profile change detected: {change_desc}"
                        if profile_changed
                        else f"Upstream {src_table}.{src_col} is source for violated column {source['violation_column']}"
                    ),
                    "profile_change_detected": profile_changed,
                }

        except Exception as e:
            logger.warning("[root_cause_trace] profile check failed for %s: %s", pk, e)
            if best_cause is None:
                best_cause = {
                    "root_cause_table": src_table,
                    "root_cause_column": src_col,
                    "confidence": "LOW",
                    "description": f"Upstream source identified but profile check failed: {e}",
                    "profile_change_detected": False,
                }

    duration = time.monotonic() - start
    if best_cause:
        logger.info("[root_cause_trace] completed: %s.%s (%s) in %.2fs",
                     best_cause["root_cause_table"], best_cause["root_cause_column"],
                     best_cause["confidence"], duration)
        return {"status": "success", **best_cause}

    return {
        "status": "no_cause_found",
        "root_cause_table": "",
        "root_cause_column": "",
        "confidence": "LOW",
        "description": "Could not determine root cause.",
        "profile_change_detected": False,
    }
