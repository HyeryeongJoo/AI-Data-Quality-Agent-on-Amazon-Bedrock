"""Dynamic rule generation tools — LLM-based rule discovery and registry update."""

import json
import logging
import time
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_bedrock_client, get_s3_client

logger = logging.getLogger(__name__)

RULE_GENERATE_SYSTEM_PROMPT = (
    "당신은 데이터 품질 규칙 생성 전문가입니다.\n"
    "주어진 스키마 정보와 샘플 데이터, 기존 규칙 목록을 분석하여 "
    "기존 규칙으로 커버되지 않는 새로운 검증 규칙을 제안합니다.\n\n"
    "각 규칙은 다음 JSON 형식으로 반환하세요:\n"
    "[\n"
    "  {\n"
    '    "rule_id": "AUTO-NNN",\n'
    '    "error_type": "out_of_range|format_inconsistency|temporal_violation|cross_column_inconsistency",\n'
    '    "description": "규칙 설명 (한국어)",\n'
    '    "target_columns": ["col1", "col2"],\n'
    '    "validation_tool": "range_check|regex_validate|timestamp_compare|address_classify",\n'
    '    "params": { ... },\n'
    '    "severity": "critical|warning|info",\n'
    '    "enabled": true\n'
    "  }\n"
    "]\n\n"
    "반드시 JSON array만 반환하세요. 기존 규칙과 중복되지 않도록 주의하세요."
)


@tool
def rule_generate(
    schema_info: dict,
    sample_data: list[dict],
    existing_rules: list[dict],
) -> dict:
    """Generate new validation rules by analyzing data patterns not covered by existing rules.

    The LLM examines schema, sample data, and existing rules to identify
    uncovered data quality patterns and propose new rules.

    Args:
        schema_info: Schema information dict (columns, types, relations).
        sample_data: Sample records for pattern analysis.
        existing_rules: Current list of rule dicts.

    Returns:
        Dict with generated_rules list and status.
    """
    start = time.monotonic()
    logger.info(
        "[rule_generate] started: %d columns, %d samples, %d existing rules",
        len(schema_info.get("columns", [])),
        len(sample_data),
        len(existing_rules),
    )

    settings = get_settings()
    bedrock = get_bedrock_client()

    # Build user message
    user_message = (
        f"## 스키마 정보\n{json.dumps(schema_info, ensure_ascii=False, indent=2)}\n\n"
        f"## 샘플 데이터 (처음 {min(len(sample_data), 20)}건)\n"
        f"{json.dumps(sample_data[:20], ensure_ascii=False, indent=2)}\n\n"
        f"## 기존 규칙 ({len(existing_rules)}개)\n"
        f"{json.dumps(existing_rules, ensure_ascii=False, indent=2)}\n\n"
        "위 정보를 분석하여 기존 규칙으로 커버되지 않는 새 규칙을 필요한 만큼 자유롭게 제안하세요. "
        "데이터 특성에 따라 적절한 수의 규칙을 생성하되, 중복이나 불필요한 규칙은 제외하세요."
    )

    try:
        response = bedrock.converse(
            modelId=settings.agent_model_id,
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            system=[{"text": RULE_GENERATE_SYSTEM_PROMPT}],
            inferenceConfig={"temperature": 0.2, "maxTokens": 8192},
        )

        # Extract token usage
        usage = response.get("usage", {})
        input_tokens = usage.get("inputTokens", 0)
        output_tokens = usage.get("outputTokens", 0)

        # Extract text
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        text = "\n".join(block.get("text", "") for block in content_blocks if "text" in block)

        # Parse JSON array
        start_idx = text.find("[")
        end_idx = text.rfind("]")
        if start_idx != -1 and end_idx != -1:
            json_str = text[start_idx:end_idx + 1]
            generated_rules = json.loads(json_str)
        else:
            generated_rules = json.loads(text)

        if not isinstance(generated_rules, list):
            generated_rules = [generated_rules]

        # Validate and add AUTO prefix
        valid_rules = []
        for i, rule in enumerate(generated_rules):
            if not rule.get("rule_id", "").startswith("AUTO-"):
                rule["rule_id"] = f"AUTO-{i + 1:03d}"
            rule["enabled"] = True
            rule.setdefault("severity", "warning")
            valid_rules.append(rule)

        duration = time.monotonic() - start
        logger.info("[rule_generate] completed: %d rules generated in %.2fs", len(valid_rules), duration)

        return {
            "status": "success",
            "generated_rules": valid_rules,
            "count": len(valid_rules),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }

    except Exception as e:
        duration = time.monotonic() - start
        logger.error("[rule_generate] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e), "generated_rules": [], "count": 0, "input_tokens": 0, "output_tokens": 0}


@tool
def rule_registry_update(
    new_rules: list[dict],
    registry_s3_path: str | None = None,
) -> dict:
    """Add dynamically generated rules to the runtime registry stored in S3.

    Reads existing rules from S3, merges new rules (avoiding duplicates by rule_id),
    and writes the updated registry back.

    Args:
        new_rules: List of new rule dicts to add.
        registry_s3_path: Optional S3 path to the rules file.
            Defaults to s3://{rules_bucket}/rules/dynamic_rules.yaml.

    Returns:
        Dict with total_rules count, added_count, and status.
    """
    start = time.monotonic()
    logger.info("[rule_registry_update] started: %d new rules", len(new_rules))

    settings = get_settings()
    s3 = get_s3_client()

    bucket = settings.s3_rules_bucket
    key = "rules/dynamic_rules.json"
    if registry_s3_path:
        from ai_dq_agent.tools.utils import parse_s3_path
        bucket, key = parse_s3_path(registry_s3_path)

    # Read existing dynamic rules
    existing_rules = []
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
        existing_rules = json.loads(body)
        if not isinstance(existing_rules, list):
            existing_rules = []
    except ClientError:
        logger.info("[rule_registry_update] no existing dynamic rules file, creating new")

    # Merge — avoid duplicates by rule_id
    existing_ids = {r.get("rule_id") for r in existing_rules}
    added = []
    for rule in new_rules:
        if rule.get("rule_id") not in existing_ids:
            rule["_generated_at"] = datetime.now(timezone.utc).isoformat()
            existing_rules.append(rule)
            added.append(rule["rule_id"])

    # Write back
    try:
        body = json.dumps(existing_rules, ensure_ascii=False, indent=2).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body)

        duration = time.monotonic() - start
        logger.info(
            "[rule_registry_update] completed: %d total, %d added in %.2fs",
            len(existing_rules), len(added), duration,
        )

        return {
            "status": "success",
            "total_rules": len(existing_rules),
            "added_count": len(added),
            "added_rule_ids": added,
        }

    except ClientError as e:
        duration = time.monotonic() - start
        logger.error("[rule_registry_update] write failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}
