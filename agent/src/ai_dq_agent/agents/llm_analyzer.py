"""LLM Analyzer — PRIMARY semantic analysis.

Simplified from dq_semantic_agent.py:
- PRIMARY analysis with explicit confidence criteria (REFLECTION removed for speed)
- Removed: DEEP_ANALYSIS, impact scoring, lineage, root cause tracing
- Removed: Strands Agent (no ReAct loop — deterministic orchestration)
- Removed: Judgment cache (no DynamoDB caching)
"""

import logging

from ai_dq_agent.agents._node_utils import node_wrapper, validate_state_keys
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    llm_batch_analyze,
    pipeline_state_write,
    s3_read_objects,
    s3_write_objects,
)

logger = logging.getLogger(__name__)

PRIMARY_SYSTEM_PROMPT = (
    "당신은 택배 데이터 품질 검증 전문가입니다. "
    "Rule-based 검증에서 의심 항목으로 분류된 레코드를 분석하여 실제 오류 여부를 판정합니다.\n\n"
    "각 항목에 대해 다음 필드를 포함하는 JSON array를 반환하세요:\n"
    "- record_id: 레코드 식별자\n"
    "- is_error: 실제 오류인지 여부 (true/false)\n"
    "- error_type: 오류 유형\n"
    "- confidence: 판정 신뢰도 (HIGH/MEDIUM/LOW)\n"
    "- evidence: 판정 근거\n"
    "- suggested_correction: 보정 추천값 (dict 또는 null)\n"
    "  is_error가 true이고 올바른 값을 추론할 수 있는 경우, "
    '{"컬럼명": 보정값} 형태로 제안하세요.\n'
    '  예시: {"phone": "010-1234-5678"}, {"weight_kg": 2.5}, {"zipcode": "06134"}\n'
    "  보정값을 확신할 수 없으면 null로 반환하세요.\n\n"
    "confidence 판정 기준:\n"
    "- HIGH: 데이터만으로 오류 여부를 확실히 판단할 수 있는 경우. "
    "형식 오류(우편번호 자릿수, 전화번호 패턴), 명백한 범위 초과(음수 중량, 미래 날짜), "
    "논리적 모순(배송완료 시간 < 접수 시간)이 해당합니다.\n"
    "- MEDIUM: 오류 가능성이 높지만 비즈니스 예외가 존재할 수 있는 경우. "
    "범위 경계값(최소/최대에 근접), 비표준이지만 유효할 수 있는 형식, "
    "도메인 지식이 필요한 크로스컬럼 불일치가 해당합니다.\n"
    "- LOW: 오류인지 확신할 수 없는 경우. "
    "통계적으로 드문 값이지만 정상 범위일 수 있는 경우, "
    "비즈니스 컨텍스트에 따라 정상/오류가 달라지는 경우가 해당합니다.\n\n"
    "반드시 JSON array만 반환하세요."
)



@node_wrapper("llm_analyzer")
def invoke_llm_analyzer(state: dict) -> dict:
    """Run PRIMARY LLM analysis.

    Flow:
    1. Load suspects from S3
    2. PRIMARY LLM analysis on all suspects
    3. Merge results, save to S3
    """
    validate_state_keys(state, ["suspects_s3_path", "suspect_count"])
    result = {**state}
    settings = get_settings()
    pipeline_id = state.get("pipeline_id", "unknown")

    # Load suspects
    read_resp = s3_read_objects(
        s3_path=state["suspects_s3_path"],
        file_format="jsonl",
    )
    suspects = read_resp.get("records", [])

    # --- PRIMARY LLM analysis ---
    all_judgments = []
    primary_failures = []
    total_input_tokens = 0
    total_output_tokens = 0
    if suspects:
        primary_resp = llm_batch_analyze(
            items=suspects,
            analysis_type="PRIMARY",
            system_prompt=PRIMARY_SYSTEM_PROMPT,
            batch_size=settings.llm_batch_size,
        )
        all_judgments = primary_resp.get("results", [])
        primary_failures = primary_resp.get("failures", [])
        total_input_tokens += primary_resp.get("input_tokens", 0)
        total_output_tokens += primary_resp.get("output_tokens", 0)

    # Compute stats — deduplicate by record_id for unique record counts
    # When one record has multiple judgments (multiple rule violations), count the record once
    seen_ids: dict[str, dict] = {}
    for j in all_judgments:
        rid = str(j.get("record_id", ""))
        if rid not in seen_ids:
            seen_ids[rid] = j

    unique_judgments = list(seen_ids.values())
    error_count = sum(1 for j in unique_judgments if j.get("is_error"))
    high_count = sum(1 for j in unique_judgments if j.get("confidence") == "HIGH")
    medium_count = sum(1 for j in unique_judgments if j.get("confidence") == "MEDIUM")
    low_count = sum(1 for j in unique_judgments if j.get("confidence") == "LOW")

    # Error-only counts by confidence (for health score calculation)
    high_error_count = sum(1 for j in unique_judgments if j.get("is_error") and j.get("confidence") == "HIGH")
    medium_error_count = sum(1 for j in unique_judgments if j.get("is_error") and j.get("confidence") == "MEDIUM")
    low_error_count = sum(1 for j in unique_judgments if j.get("is_error") and j.get("confidence") == "LOW")

    # Write judgments to S3
    judgments_s3_path = f"{state['s3_staging_prefix']}judgments.jsonl"
    if all_judgments:
        s3_write_objects(
            s3_path=judgments_s3_path,
            data=all_judgments,
            file_format="jsonl",
        )

    # Collect unique failure reasons
    all_failure_reasons = list({
        f.get("error", "unknown") for f in primary_failures if f.get("error")
    })

    result["judgments_s3_path"] = judgments_s3_path
    result["analysis_stats"] = {
        "pipeline_id": pipeline_id,
        "judgments_s3_path": judgments_s3_path,
        "total_analyzed": len(unique_judgments),
        "error_count": error_count,
        "high_confidence_count": high_count,
        "medium_confidence_count": medium_count,
        "low_confidence_count": low_count,
        "high_error_count": high_error_count,
        "medium_error_count": medium_error_count,
        "low_error_count": low_error_count,
        "suspect_input_count": len(suspects),
        "primary_failed_count": len(primary_failures),
        "failure_reasons": all_failure_reasons,
        "input_tokens": total_input_tokens + state.get("rv_input_tokens", 0),
        "output_tokens": total_output_tokens + state.get("rv_output_tokens", 0),
    }
    result["_records_processed"] = len(all_judgments)

    # Store analysis stats in pipeline state
    pipeline_state_write(key="analysis_stats", value=result["analysis_stats"])

    return result
