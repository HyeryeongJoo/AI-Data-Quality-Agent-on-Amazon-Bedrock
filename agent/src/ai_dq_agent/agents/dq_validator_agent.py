"""Rule Validator — deterministic rule-based validation with LLM-assisted rule discovery.

Pipeline flow:
1. Schema inference + rule registry loading (absorbed from schema_analyzer)
2. LLM Round 1: discover cross-column profile targets from schema
3. Full-data profiling based on LLM suggestions
4. LLM Round 2: generate dynamic rules from profile statistics
5. Deterministic full-scan validation (existing + dynamic rules)
"""

import hashlib
import json
import logging
import re
import time

from botocore.exceptions import ClientError

from ai_dq_agent.agents._node_utils import node_wrapper
from ai_dq_agent.rules.registry import load_default, load_from_s3
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    pipeline_state_write,
    range_check,
    regex_validate,
    rule_generate,
    s3_read_objects,
    s3_write_objects,
    timestamp_compare,
)
from ai_dq_agent.tools.aws_clients import get_bedrock_client, get_s3_client

logger = logging.getLogger(__name__)

# --- Column type hints for schema inference (from schema_analyzer) ----------

_DATETIME_HINTS = {"time", "timestamp", "date", "at", "created", "updated"}

_DYNAMIC_RULES_CACHE_TTL = 3600  # 1 hour


def _schema_fingerprint(schema_info: dict) -> str:
    """Compute a stable hash from column names + types for cache keying."""
    cols = sorted(
        (c["name"], c["data_type"]) for c in schema_info.get("columns", [])
    )
    raw = json.dumps(cols, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _read_dynamic_rules_cache(fingerprint: str) -> list[dict] | None:
    """Read cached dynamic rules from S3. Returns None on miss."""
    settings = get_settings()
    s3 = get_s3_client()
    key = f"rules/dynamic_rules_cache/{fingerprint}.json"
    try:
        resp = s3.get_object(Bucket=settings.s3_rules_bucket, Key=key)
        body = json.loads(resp["Body"].read().decode("utf-8"))
        cached_at = body.get("cached_at", 0)
        if time.time() - cached_at > _DYNAMIC_RULES_CACHE_TTL:
            logger.info("[rule_validator] Dynamic rules cache expired (fingerprint=%s)", fingerprint)
            return None
        rules = body.get("rules", [])
        logger.info("[rule_validator] Dynamic rules cache HIT: %d rules (fingerprint=%s)", len(rules), fingerprint)
        return rules
    except ClientError:
        return None
    except Exception as e:
        logger.warning("[rule_validator] Cache read error: %s", e)
        return None


def _write_dynamic_rules_cache(fingerprint: str, rules: list[dict]) -> None:
    """Write dynamic rules to S3 cache."""
    settings = get_settings()
    s3 = get_s3_client()
    key = f"rules/dynamic_rules_cache/{fingerprint}.json"
    body = json.dumps({"cached_at": time.time(), "fingerprint": fingerprint, "rules": rules}, ensure_ascii=False)
    try:
        s3.put_object(Bucket=settings.s3_rules_bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("[rule_validator] Dynamic rules cached: %d rules (fingerprint=%s)", len(rules), fingerprint)
    except Exception as e:
        logger.warning("[rule_validator] Cache write error: %s", e)


DISCOVER_PROFILE_SYSTEM_PROMPT = (
    "당신은 택배 데이터 품질 전문가입니다.\n"
    "주어진 스키마 정보(컬럼명, 타입, 샘플 값)를 보고, "
    "비즈니스적으로 검증해야 할 크로스컬럼 조건들을 제안하세요.\n\n"
    "각 조건은 다음 JSON 형식으로 반환하세요:\n"
    "[\n"
    '  {"condition": "column_a == column_b", "description": "설명", '
    '"columns": ["column_a", "column_b"]},\n'
    '  {"condition": "column_x == 0 AND column_y == \'DELIVERED\'", '
    '"description": "설명", "columns": ["column_x", "column_y"]}\n'
    "]\n\n"
    "반드시 JSON array만 반환하세요. "
    "기존 규칙에서 이미 다루는 조건은 제외하세요."
)


# ---------------------------------------------------------------------------
# Schema inference helpers (absorbed from dq_schema_agent.py)
# ---------------------------------------------------------------------------


def _infer_data_type(column_name: str, sample_values: list) -> str:
    """Infer column data type from name hints and sample values."""
    lower_name = column_name.lower()
    for hint in _DATETIME_HINTS:
        if hint in lower_name:
            return "datetime"

    non_null = [v for v in sample_values if v is not None]
    if not non_null:
        return "string"

    first = non_null[0]
    if isinstance(first, bool):
        return "boolean"
    if isinstance(first, int):
        return "integer"
    if isinstance(first, float):
        return "float"
    return "string"


def _build_schema_info(records: list[dict], rules, settings) -> dict:
    """Build schema information from sample records and rule definitions."""
    columns_info = []
    if records:
        all_columns = set()
        for rec in records:
            all_columns.update(rec.keys())

        for col in sorted(all_columns):
            sample_values = [rec.get(col) for rec in records]
            non_null = [v for v in sample_values if v is not None]
            nullable = len(non_null) < len(sample_values)
            data_type = _infer_data_type(col, sample_values)
            columns_info.append({
                "name": col,
                "data_type": data_type,
                "nullable": nullable,
                "description": "",
            })

    schema_info: dict = {
        "table_name": settings.dynamodb_table_name,
        "columns": columns_info,
        "primary_key": [],
        "temporal_relations": [],
        "cross_column_relations": [],
        "sample_count": len(records),
    }

    for rule in rules:
        params = rule.params or {} if hasattr(rule, "params") else rule.get("params", {})
        target_cols = rule.target_columns if hasattr(rule, "target_columns") else rule.get("target_columns", [])
        validation_tool = rule.validation_tool if hasattr(rule, "validation_tool") else rule.get("validation_tool", "")
        error_type = rule.error_type if hasattr(rule, "error_type") else rule.get("error_type", "")

        if validation_tool == "timestamp_compare" and len(target_cols) >= 2:
            desc = rule.description if hasattr(rule, "description") else rule.get("description", "")
            schema_info["temporal_relations"].append({
                "earlier_column": target_cols[0],
                "later_column": target_cols[1],
                "description": desc,
            })
        if error_type == "cross_column_inconsistency" and len(target_cols) >= 2:
            desc = rule.description if hasattr(rule, "description") else rule.get("description", "")
            schema_info["cross_column_relations"].append({
                "source_column": target_cols[0],
                "target_column": target_cols[1],
                "relation_type": params.get("relation_type", "semantic"),
                "description": desc,
            })

    return schema_info


# ---------------------------------------------------------------------------
# LLM Round 1: Discover cross-column profile targets
# ---------------------------------------------------------------------------


def _discover_profile_targets(schema_info: dict, sample_records: list[dict], existing_rules: list[dict]) -> tuple[list[dict], dict]:
    """Ask LLM which cross-column conditions to profile on all data.

    Returns a tuple of (targets list, token_usage dict).
    """
    settings = get_settings()
    bedrock = get_bedrock_client()

    # Build compact schema summary for LLM
    col_summary = []
    for col in schema_info.get("columns", []):
        col_summary.append(f"- {col['name']} ({col['data_type']}, nullable={col['nullable']})")

    sample_preview = json.dumps(sample_records[:5], ensure_ascii=False, indent=2)
    rules_preview = json.dumps(existing_rules[:10], ensure_ascii=False, indent=2)

    user_message = (
        f"## 스키마 컬럼\n" + "\n".join(col_summary) + "\n\n"
        f"## 샘플 레코드 (5건)\n{sample_preview}\n\n"
        f"## 기존 검증 규칙 (일부)\n{rules_preview}\n\n"
        f"위 스키마를 보고, 기존 규칙에 없지만 비즈니스적으로 검증해야 할 "
        f"크로스컬럼 조건을 필요한 만큼 자유롭게 제안하세요. 데이터 특성에 따라 적절한 수를 제안하되, 중복은 제외하세요."
    )

    try:
        response = bedrock.converse(
            modelId=settings.agent_model_id,
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=[{"text": DISCOVER_PROFILE_SYSTEM_PROMPT}],
            inferenceConfig={"temperature": 0.2, "maxTokens": 2048},
        )

        usage = response.get("usage", {})
        token_usage = {
            "input_tokens": usage.get("inputTokens", 0),
            "output_tokens": usage.get("outputTokens", 0),
        }

        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        text = "\n".join(block.get("text", "") for block in content_blocks if "text" in block)

        start_idx = text.find("[")
        end_idx = text.rfind("]")
        if start_idx != -1 and end_idx != -1:
            targets = json.loads(text[start_idx:end_idx + 1])
        else:
            targets = json.loads(text)

        if not isinstance(targets, list):
            targets = [targets]

        logger.info("[rule_validator] LLM discovered %d profile targets", len(targets))
        return targets, token_usage

    except Exception as e:
        logger.warning("[rule_validator] Profile target discovery failed: %s", e)
        return [], {"input_tokens": 0, "output_tokens": 0}


# ---------------------------------------------------------------------------
# Full-data profiling
# ---------------------------------------------------------------------------


def _run_full_profiling(
    s3_staging_prefix: str,
    profile_targets: list[dict],
    chunk_size: int,
) -> dict:
    """Profile all data: per-column stats + cross-column condition counts.

    Returns a dict with 'column_stats' and 'cross_column_stats'.
    """
    from collections import Counter

    column_counters: dict[str, Counter] = {}
    column_null_counts: dict[str, int] = {}
    cross_column_counts: dict[str, int] = {}
    total_records = 0
    offset = 0

    # Initialize cross-column counters
    for target in profile_targets:
        key = target.get("condition", "")
        if key:
            cross_column_counts[key] = 0

    while True:
        read_resp = s3_read_objects(
            s3_path=f"{s3_staging_prefix}data.jsonl",
            file_format="jsonl",
            chunk_size=chunk_size,
            chunk_index=offset // chunk_size if chunk_size > 0 else 0,
        )
        records = read_resp.get("records", [])
        if not records:
            break

        total_records += len(records)

        for rec in records:
            # Per-column stats
            for col, val in rec.items():
                if col not in column_counters:
                    column_counters[col] = Counter()
                    column_null_counts[col] = 0
                if val is None:
                    column_null_counts[col] += 1
                else:
                    column_counters[col][val] += 1

            # Cross-column condition evaluation
            for target in profile_targets:
                condition = target.get("condition", "")
                columns = target.get("columns", [])
                if not condition or not columns:
                    continue
                try:
                    if _evaluate_condition(rec, condition, columns):
                        cross_column_counts[condition] = cross_column_counts.get(condition, 0) + 1
                except Exception:
                    pass

        offset += len(records)
        if len(records) < chunk_size:
            break

    # Build column stats summary
    column_stats = {}
    for col, counter in column_counters.items():
        total_non_null = sum(counter.values())
        total_col = total_non_null + column_null_counts.get(col, 0)
        null_rate = column_null_counts.get(col, 0) / total_col if total_col > 0 else 0
        column_stats[col] = {
            "total_count": total_col,
            "null_count": column_null_counts.get(col, 0),
            "null_rate": round(null_rate, 6),
            "unique_count": len(counter),
            "top_values": [{"value": str(v), "count": c} for v, c in counter.most_common(5)],
        }

    logger.info(
        "[rule_validator] Profiling complete: %d records, %d columns, %d cross-column conditions",
        total_records, len(column_stats), len(cross_column_counts),
    )

    return {
        "total_records": total_records,
        "column_stats": column_stats,
        "cross_column_stats": {
            k: {"condition": k, "match_count": v, "description": next(
                (t.get("description", "") for t in profile_targets if t.get("condition") == k), ""
            )}
            for k, v in cross_column_counts.items()
        },
    }


def _evaluate_condition(record: dict, condition: str, columns: list[str]) -> bool:
    """Evaluate a simple cross-column condition against a record.

    Supports patterns like:
    - "col_a == col_b"
    - "col_a == 0 AND col_b == 'VALUE'"
    """
    # Build a safe evaluation context with only the relevant column values
    ctx = {}
    for col in columns:
        val = record.get(col)
        ctx[col] = val

    # Replace AND/OR with Python operators for simple evaluation
    expr = condition.replace(" AND ", " and ").replace(" OR ", " or ")

    try:
        return bool(eval(expr, {"__builtins__": {}}, ctx))  # noqa: S307
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Deterministic validation checks (unchanged)
# ---------------------------------------------------------------------------


def _run_range_checks(records: list, rules: list) -> list[dict]:
    """Run out_of_range validation for applicable rules."""
    suspects = []
    range_rules = [r for r in rules if r.get("error_type") == "out_of_range"]
    for rule in range_rules:
        for col in rule.get("target_columns", []):
            params = rule.get("params", {})
            resp = range_check(
                records=records,
                column_name=col,
                primary_key=["record_id"],
                allowed_values=params.get("allowed_values"),
                min_value=params.get("min_value"),
                max_value=params.get("max_value"),
            )
            for v in resp.get("violations", []):
                suspects.append({
                    "record_id": v.get("record_id", ""),
                    "rule_id": rule["rule_id"],
                    "error_type": "out_of_range",
                    "target_columns": [col],
                    "current_values": {col: v.get("actual_value")},
                    "reason": v.get("expected_condition", "Out of allowed range"),
                    "severity": rule.get("severity", "warning"),
                })
    return suspects


def _run_format_checks(records: list, rules: list) -> list[dict]:
    """Run format_inconsistency validation for applicable rules."""
    suspects = []
    format_rules = [r for r in rules if r.get("error_type") == "format_inconsistency"]
    for rule in format_rules:
        for col in rule.get("target_columns", []):
            pattern = rule.get("params", {}).get("pattern", "")
            if not pattern:
                continue
            resp = regex_validate(
                records=records,
                column_name=col,
                pattern=pattern,
                primary_key=["record_id"],
            )
            total_checked = resp.get("total_checked", 1) or 1
            violation_rate = resp.get("violation_count", 0) / total_checked
            if violation_rate > 0.9:
                logger.warning(
                    "[rule_validator] High violation rate %.0f%% for rule %s on %s — pattern may need adjustment",
                    violation_rate * 100, rule["rule_id"], col,
                )

            for v in resp.get("violations", []):
                suspects.append({
                    "record_id": v.get("record_id", ""),
                    "rule_id": rule["rule_id"],
                    "error_type": "format_inconsistency",
                    "target_columns": [col],
                    "current_values": {col: v.get("actual_value")},
                    "reason": v.get("expected_condition", f"Does not match pattern {pattern}"),
                    "severity": rule.get("severity", "warning"),
                })
    return suspects


def _run_temporal_checks(records: list, rules: list) -> list[dict]:
    """Run temporal_violation validation for applicable rules."""
    suspects = []
    temporal_rules = [r for r in rules if r.get("error_type") == "temporal_violation"]
    for rule in temporal_rules:
        cols = rule.get("target_columns", [])
        if len(cols) < 2:
            continue
        params = rule.get("params", {})
        time_formats = params.get("time_formats")
        resp = timestamp_compare(
            records=records,
            earlier_column=params.get("earlier_column", cols[0]),
            later_column=params.get("later_column", cols[1]),
            primary_key=["record_id"],
            time_formats=time_formats,
        )
        for v in resp.get("violations", []):
            suspects.append({
                "record_id": v.get("record_id", ""),
                "rule_id": rule["rule_id"],
                "error_type": "temporal_violation",
                "target_columns": cols[:2],
                "current_values": {cols[0]: v.get("earlier_value"), cols[1]: v.get("later_value")},
                "reason": v.get("expected_condition", "Temporal order violation"),
                "severity": rule.get("severity", "warning"),
            })
    return suspects


# Regex patterns for Korean address type detection (replaces external API call)
_ROAD_ADDR_RE = re.compile(r"(로|길)\s*\d+")
_JIBUN_ADDR_RE = re.compile(r"(동|리|가)\s*\d+|\d+-\d+")


def _classify_address(text: str) -> str:
    """Classify a Korean address as 'road', 'jibun', or 'ambiguous' using regex."""
    if not text or not isinstance(text, str):
        return "ambiguous"
    has_road = bool(_ROAD_ADDR_RE.search(text))
    has_jibun = bool(_JIBUN_ADDR_RE.search(text))
    if has_road and not has_jibun:
        return "road"
    if has_jibun and not has_road:
        return "jibun"
    return "ambiguous"


def _run_cross_column_checks(records: list, rules: list) -> list[dict]:
    """Run cross_column_inconsistency validation.

    Supports two validation_tool types:
    - address_normalize / address_classify: Korean address type vs flag check
    - value_condition: generic conditional cross-column validation
    """
    suspects = []
    cross_rules = [r for r in rules if r.get("error_type") == "cross_column_inconsistency"]

    for rule in cross_rules:
        cols = rule.get("target_columns", [])
        if len(cols) < 2:
            continue

        validation_tool = rule.get("validation_tool", "")

        # --- Address classification rules ---
        if validation_tool in ("address_normalize", "address_classify"):
            address_col = cols[0]
            flag_col = cols[1]

            for rec in records:
                record_id = str(rec.get("record_id", rec.get("id", "")))
                addr_text = str(rec.get(address_col, ""))
                if not addr_text:
                    continue

                addr_type = _classify_address(addr_text)
                flag_value = rec.get(flag_col)

                if addr_type == "road" and str(flag_value) != "1":
                    suspects.append({
                        "record_id": record_id,
                        "rule_id": rule["rule_id"],
                        "error_type": "cross_column_inconsistency",
                        "target_columns": cols[:2],
                        "current_values": {address_col: addr_text, flag_col: flag_value},
                        "reason": f"Address is road type but {flag_col}={flag_value}",
                        "severity": rule.get("severity", "warning"),
                    })
                elif addr_type == "jibun" and str(flag_value) != "0":
                    suspects.append({
                        "record_id": record_id,
                        "rule_id": rule["rule_id"],
                        "error_type": "cross_column_inconsistency",
                        "target_columns": cols[:2],
                        "current_values": {address_col: addr_text, flag_col: flag_value},
                        "reason": f"Address is jibun type but {flag_col}={flag_value}",
                        "severity": rule.get("severity", "warning"),
                    })
                elif addr_type == "ambiguous":
                    suspects.append({
                        "record_id": record_id,
                        "rule_id": rule["rule_id"],
                        "error_type": "cross_column_inconsistency",
                        "target_columns": cols[:2],
                        "current_values": {address_col: addr_text, flag_col: flag_value},
                        "reason": "Address type ambiguous — deferred to LLM Analyzer",
                        "severity": "info",
                    })

        # --- Generic value-condition rules ---
        elif validation_tool == "value_condition":
            conditions = rule.get("params", {}).get("conditions", [])
            for rec in records:
                record_id = str(rec.get("record_id", rec.get("id", "")))
                for cond in conditions:
                    when_col = cond.get("when_column", "")
                    when_vals = cond.get("when_values", [])
                    check_col = cond.get("check_column", "")
                    check_op = cond.get("check_op", "")
                    check_val = cond.get("check_value")
                    reason = cond.get("reason", "Cross-column condition violation")

                    rec_when_val = rec.get(when_col)
                    if rec_when_val not in when_vals:
                        continue

                    rec_check_val = rec.get(check_col)
                    violated = False
                    if check_op == "eq" and rec_check_val != check_val:
                        violated = True
                    elif check_op == "neq" and rec_check_val == check_val:
                        violated = True
                    elif check_op == "gt" and not (rec_check_val is not None and rec_check_val > check_val):
                        violated = True
                    elif check_op == "gte" and not (rec_check_val is not None and rec_check_val >= check_val):
                        violated = True
                    elif check_op == "lt" and not (rec_check_val is not None and rec_check_val < check_val):
                        violated = True
                    elif check_op == "lte" and not (rec_check_val is not None and rec_check_val <= check_val):
                        violated = True

                    if violated:
                        suspects.append({
                            "record_id": record_id,
                            "rule_id": rule["rule_id"],
                            "error_type": "cross_column_inconsistency",
                            "target_columns": cols[:2],
                            "current_values": {when_col: rec_when_val, check_col: rec_check_val},
                            "reason": reason,
                            "severity": rule.get("severity", "warning"),
                        })

    return suspects


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


@node_wrapper("rule_validator")
def invoke_rule_validator(state: dict) -> dict:
    """Run rule-based validation: schema inference, LLM rule discovery, full-scan validation."""
    result = {**state}
    settings = get_settings()
    pipeline_id = state.get("pipeline_id", "unknown")
    chunk_size = settings.chunk_size

    # --- Phase 1: Schema inference + Rule loading ---
    sample_resp = s3_read_objects(
        s3_path=f"{state['s3_staging_prefix']}data.jsonl",
        file_format="jsonl",
        sample_mode=True,
        sample_size=1000,
    )
    sample_records = sample_resp.get("records", [])

    # Load rule registry
    try:
        registry = load_from_s3(
            bucket=settings.s3_rules_bucket,
            key=settings.s3_rules_key,
            region=settings.aws_region,
        )
    except Exception:
        logger.warning("[%s] Rule registry load failed, using defaults", pipeline_id)
        registry = load_default()

    rules = registry.get_all_enabled()
    rule_mappings = [r.model_dump() for r in rules]

    # Build schema info
    schema_info = _build_schema_info(sample_records, rules, settings)
    result["schema_info"] = schema_info

    # --- Phases 2-4: Dynamic rule discovery (with S3 caching) ---
    fingerprint = _schema_fingerprint(schema_info)
    cached_rules = _read_dynamic_rules_cache(fingerprint)

    # Track LLM token usage across rule_validator
    rv_input_tokens = 0
    rv_output_tokens = 0

    if cached_rules is not None:
        dynamic_rules = cached_rules
        logger.info("[%s] Using %d cached dynamic rules (skipped LLM)", pipeline_id, len(dynamic_rules))
    else:
        # Phase 2: LLM Round 1 — Discover profile targets
        profile_targets, discover_tokens = _discover_profile_targets(schema_info, sample_records, rule_mappings)
        rv_input_tokens += discover_tokens.get("input_tokens", 0)
        rv_output_tokens += discover_tokens.get("output_tokens", 0)

        # Phase 3: Full-data profiling
        profile_stats = _run_full_profiling(state["s3_staging_prefix"], profile_targets, chunk_size)

        # Phase 4: LLM Round 2 — Generate dynamic rules
        dynamic_rules = []
        try:
            gen_resp = rule_generate(
                schema_info={**schema_info, "profile_stats": profile_stats},
                sample_data=sample_records[:20],
                existing_rules=rule_mappings,
            )
            dynamic_rules = gen_resp.get("generated_rules", [])
            rv_input_tokens += gen_resp.get("input_tokens", 0)
            rv_output_tokens += gen_resp.get("output_tokens", 0)
            logger.info("[%s] Generated %d dynamic rules", pipeline_id, len(dynamic_rules))
        except Exception as e:
            logger.warning("[%s] Dynamic rule generation failed: %s", pipeline_id, e)

        # Cache for future runs
        if dynamic_rules:
            _write_dynamic_rules_cache(fingerprint, dynamic_rules)

    logger.info("[%s] rule_validator LLM tokens: input=%d, output=%d", pipeline_id, rv_input_tokens, rv_output_tokens)

    # Merge all rules
    all_rules = rule_mappings + dynamic_rules
    result["rule_mappings"] = all_rules
    result["dynamic_rule_count"] = len(dynamic_rules)

    # --- Phase 5: Deterministic full-scan validation ---
    all_suspects: list[dict] = []
    total_scanned = 0
    chunk_count = 0
    offset = 0

    while True:
        read_resp = s3_read_objects(
            s3_path=f"{state['s3_staging_prefix']}data.jsonl",
            file_format="jsonl",
            chunk_size=chunk_size,
            chunk_index=offset // chunk_size if chunk_size > 0 else 0,
        )
        records = read_resp.get("records", [])
        if not records:
            break

        chunk_count += 1
        total_scanned += len(records)

        all_suspects.extend(_run_range_checks(records, all_rules))
        all_suspects.extend(_run_format_checks(records, all_rules))
        all_suspects.extend(_run_temporal_checks(records, all_rules))
        all_suspects.extend(_run_cross_column_checks(records, all_rules))

        offset += len(records)
        if len(records) < chunk_size:
            break

    # Deduplicate by (record_id, rule_id)
    seen: set[tuple[str, str]] = set()
    unique_suspects: list[dict] = []
    for s in all_suspects:
        key = (s["record_id"], s["rule_id"])
        if key not in seen:
            seen.add(key)
            unique_suspects.append(s)

    suspect_count = len(unique_suspects)

    # Write suspects to S3
    suspects_s3_path = f"{state['s3_staging_prefix']}suspects.jsonl"
    if suspect_count > 0:
        s3_write_objects(
            s3_path=suspects_s3_path,
            data=unique_suspects,
            file_format="jsonl",
        )

    # Compute stats
    stats_by_error_type: dict[str, int] = {}
    stats_by_severity: dict[str, int] = {}
    for s in unique_suspects:
        et = s.get("error_type", "unknown")
        sev = s.get("severity", "unknown")
        stats_by_error_type[et] = stats_by_error_type.get(et, 0) + 1
        stats_by_severity[sev] = stats_by_severity.get(sev, 0) + 1

    result["suspects_s3_path"] = suspects_s3_path
    result["suspect_count"] = suspect_count
    result["validation_stats"] = {
        "pipeline_id": pipeline_id,
        "total_scanned": total_scanned,
        "suspect_count": suspect_count,
        "suspects_s3_path": suspects_s3_path,
        "stats_by_error_type": stats_by_error_type,
        "stats_by_severity": stats_by_severity,
        "chunk_count": chunk_count,
        "dynamic_rule_count": len(dynamic_rules),
    }
    result["_records_processed"] = total_scanned

    # Store rule_validator LLM token usage for pipeline-wide aggregation
    result["rv_input_tokens"] = rv_input_tokens
    result["rv_output_tokens"] = rv_output_tokens

    # Store in pipeline state for graph routing
    pipeline_state_write(key="suspect_count", value=suspect_count)
    pipeline_state_write(key="total_scanned", value=total_scanned)
    pipeline_state_write(key="suspects_s3_path", value=suspects_s3_path)

    logger.info(
        "[%s] rule_validator: scanned=%d, suspects=%d (static_rules=%d, dynamic_rules=%d)",
        pipeline_id, total_scanned, suspect_count, len(rule_mappings), len(dynamic_rules),
    )

    return result
