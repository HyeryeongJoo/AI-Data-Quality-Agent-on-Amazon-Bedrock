"""Rule-based validation tools (pure Python, no AWS dependency)."""

import logging
import re
import time
from datetime import datetime
from functools import lru_cache

from strands import tool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Compiled regex cache (DP-04)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=128)
def _compile_pattern(pattern: str) -> re.Pattern:
    return re.compile(pattern)


# ---------------------------------------------------------------------------
# @tool: regex_validate
# ---------------------------------------------------------------------------

@tool
def regex_validate(
    records: list[dict],
    column_name: str,
    pattern: str,
    primary_key: list[str],
    nullable: bool = True,
) -> dict:
    """Validate column values against a regex pattern using fullmatch.

    Args:
        records: List of record dicts to validate.
        column_name: The column to check.
        pattern: Regex pattern string (fullmatch).
        primary_key: List of primary key column names for record identification.
        nullable: If True, skip None values; if False, None is a violation.

    Returns:
        Dict with violations list, violation_count, and total_checked.
    """
    start = time.monotonic()
    logger.info("[regex_validate] started: column=%s, pattern=%s, records=%d", column_name, pattern, len(records))

    try:
        compiled = _compile_pattern(pattern)
    except re.error as e:
        logger.error("[regex_validate] invalid pattern %s: %s", pattern, e)
        return {"status": "error", "error": "InvalidPattern", "message": str(e)}

    violations = []
    checked = 0

    for record in records:
        value = record.get(column_name)
        if value is None:
            if not nullable:
                pk_vals = {k: record.get(k) for k in primary_key}
                violations.append({
                    "record_id": str(pk_vals),
                    "column": column_name,
                    "actual_value": "null",
                    "expected_condition": f"pattern:{pattern}",
                })
            continue

        checked += 1
        if not compiled.fullmatch(str(value)):
            pk_vals = {k: record.get(k) for k in primary_key}
            violations.append({
                "record_id": str(pk_vals),
                "column": column_name,
                "actual_value": str(value),
                "expected_condition": f"pattern:{pattern}",
            })

    duration = time.monotonic() - start
    logger.info("[regex_validate] completed: %d violations / %d checked in %.2fs", len(violations), checked, duration)

    return {
        "status": "success",
        "violations": violations,
        "violation_count": len(violations),
        "total_checked": checked,
    }


# ---------------------------------------------------------------------------
# @tool: range_check
# ---------------------------------------------------------------------------

@tool
def range_check(
    records: list[dict],
    column_name: str,
    primary_key: list[str],
    allowed_values: list | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
    nullable: bool = True,
) -> dict:
    """Validate column values against allowed values or numeric range.

    Args:
        records: List of record dicts to validate.
        column_name: The column to check.
        primary_key: List of primary key column names for record identification.
        allowed_values: If provided, value must be in this list.
        min_value: If provided, numeric value must be >= min_value.
        max_value: If provided, numeric value must be <= max_value.
        nullable: If True, skip None values; if False, None is a violation.

    Returns:
        Dict with violations list, violation_count, and total_checked.
    """
    start = time.monotonic()
    logger.info("[range_check] started: column=%s, records=%d", column_name, len(records))

    allowed_set = set(allowed_values) if allowed_values else None
    violations = []
    checked = 0

    for record in records:
        value = record.get(column_name)
        if value is None:
            if not nullable:
                pk_vals = {k: record.get(k) for k in primary_key}
                violations.append({
                    "record_id": str(pk_vals),
                    "column": column_name,
                    "actual_value": "null",
                    "expected_condition": _range_condition(allowed_values, min_value, max_value),
                })
            continue

        checked += 1
        violated = False

        if allowed_set is not None:
            check_val = str(value) if isinstance(value, (dict, list)) else value
            if check_val not in allowed_set:
                violated = True

        if not violated and (min_value is not None or max_value is not None):
            try:
                numeric = float(value)
                if min_value is not None and numeric < min_value:
                    violated = True
                if max_value is not None and numeric > max_value:
                    violated = True
            except (ValueError, TypeError):
                violated = True

        if violated:
            pk_vals = {k: record.get(k) for k in primary_key}
            violations.append({
                "record_id": str(pk_vals),
                "column": column_name,
                "actual_value": str(value),
                "expected_condition": _range_condition(allowed_values, min_value, max_value),
            })

    duration = time.monotonic() - start
    logger.info("[range_check] completed: %d violations / %d checked in %.2fs", len(violations), checked, duration)

    return {
        "status": "success",
        "violations": violations,
        "violation_count": len(violations),
        "total_checked": checked,
    }


def _range_condition(allowed_values, min_value, max_value) -> str:
    parts = []
    if allowed_values is not None:
        parts.append(f"allowed:{allowed_values}")
    if min_value is not None:
        parts.append(f"min:{min_value}")
    if max_value is not None:
        parts.append(f"max:{max_value}")
    return ", ".join(parts) if parts else "no_constraint"


# ---------------------------------------------------------------------------
# @tool: timestamp_compare
# ---------------------------------------------------------------------------

@tool
def timestamp_compare(
    records: list[dict],
    earlier_column: str,
    later_column: str,
    primary_key: list[str],
    time_formats: list[str] | None = None,
) -> dict:
    """Validate temporal ordering between two timestamp columns.

    Checks that earlier_column < later_column for every record.

    Args:
        records: List of record dicts to validate.
        earlier_column: Column that should contain the earlier timestamp.
        later_column: Column that should contain the later timestamp.
        primary_key: List of primary key column names for record identification.
        time_formats: List of datetime format strings to try for parsing.

    Returns:
        Dict with violations, violation_count, total_checked, and parse_failures.
    """
    start = time.monotonic()
    logger.info(
        "[timestamp_compare] started: %s < %s, records=%d",
        earlier_column, later_column, len(records),
    )

    if time_formats is None:
        time_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M%S", "%H:%M:%S"]

    violations = []
    parse_failures = []
    checked = 0

    for record in records:
        earlier_str = record.get(earlier_column)
        later_str = record.get(later_column)

        if earlier_str is None or later_str is None:
            continue

        pk_vals = {k: record.get(k) for k in primary_key}
        record_id = str(pk_vals)

        earlier_dt = _try_parse(str(earlier_str), time_formats)
        if earlier_dt is None:
            parse_failures.append({
                "record_id": record_id,
                "column": earlier_column,
                "value": str(earlier_str),
                "error": "Failed to parse timestamp",
            })
            continue

        later_dt = _try_parse(str(later_str), time_formats)
        if later_dt is None:
            parse_failures.append({
                "record_id": record_id,
                "column": later_column,
                "value": str(later_str),
                "error": "Failed to parse timestamp",
            })
            continue

        checked += 1
        if earlier_dt >= later_dt:
            violations.append({
                "record_id": record_id,
                "column": f"{earlier_column},{later_column}",
                "actual_value": f"{earlier_str} >= {later_str}",
                "expected_condition": f"{earlier_column} < {later_column}",
                "earlier_column": earlier_column,
                "earlier_value": str(earlier_str),
                "later_column": later_column,
                "later_value": str(later_str),
            })

    duration = time.monotonic() - start
    logger.info(
        "[timestamp_compare] completed: %d violations / %d checked, %d parse failures in %.2fs",
        len(violations), checked, len(parse_failures), duration,
    )

    return {
        "status": "success",
        "violations": violations,
        "violation_count": len(violations),
        "total_checked": checked,
        "parse_failures": parse_failures,
        "parse_failure_count": len(parse_failures),
    }


def _try_parse(value: str, formats: list[str]) -> datetime | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None
