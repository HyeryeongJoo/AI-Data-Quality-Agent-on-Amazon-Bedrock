"""LLM Analyzer — PRIMARY + REFLECTION semantic analysis with judgment cache.

Simplified from dq_semantic_agent.py:
- Keeps PRIMARY analysis + REFLECTION self-verification (per spec)
- Keeps judgment cache for consistency and cost savings
- Removed: DEEP_ANALYSIS, impact scoring, lineage, root cause tracing
- Removed: Strands Agent (no ReAct loop — deterministic orchestration)
"""

import logging

from ai_dq_agent.agents._node_utils import node_wrapper, validate_state_keys
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    judgment_cache_read,
    judgment_cache_write,
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
    "반드시 JSON array만 반환하세요."
)

REFLECTION_SYSTEM_PROMPT = (
    "당신은 데이터 품질 판정 검토자입니다. "
    "이전 1차 판정 결과를 재검토하여 동의 여부를 판단합니다.\n\n"
    "각 항목에 대해 다음 필드를 포함하는 JSON array를 반환하세요:\n"
    "- record_id: 레코드 식별자\n"
    "- is_error: 재검토 후 오류 여부 (true/false)\n"
    "- confidence: 재검토 신뢰도 (HIGH/MEDIUM/LOW)\n"
    "- evidence: 재검토 근거\n\n"
    "반드시 JSON array만 반환하세요."
)


def _build_cache_key(suspect: dict) -> str:
    """Build a cache lookup key from a suspect item."""
    return f"{suspect.get('error_type', '')}:{suspect.get('rule_id', '')}:{','.join(suspect.get('target_columns', []))}"


@node_wrapper("llm_analyzer")
def invoke_llm_analyzer(state: dict) -> dict:
    """Run PRIMARY + REFLECTION LLM analysis with judgment cache.

    Flow:
    1. Load suspects from S3
    2. Cache lookup — skip already-judged patterns
    3. PRIMARY LLM analysis on uncached suspects
    4. REFLECTION self-verification
    5. Merge results, write cache, save to S3
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

    # Apply LLM max items cap
    if len(suspects) > settings.llm_max_items:
        logger.warning(
            "[%s] Suspect count %d exceeds llm_max_items %d",
            pipeline_id, len(suspects), settings.llm_max_items,
        )
        suspects = suspects[:settings.llm_max_items]

    # --- Cache lookup ---
    cache_keys = [_build_cache_key(s) for s in suspects]
    cache_resp = judgment_cache_read(pattern_keys=cache_keys)
    cached_results = {
        hit["pattern_key"]: hit["judgment"]
        for hit in cache_resp.get("hits", [])
    }

    cached_judgments = []
    uncached_suspects = []
    for i, suspect in enumerate(suspects):
        key = cache_keys[i]
        if key in cached_results and cached_results[key] is not None:
            cached_judgments.append(cached_results[key])
        else:
            uncached_suspects.append(suspect)

    cache_hit_count = len(cached_judgments)
    logger.info("[%s] Cache: %d hits, %d misses", pipeline_id, cache_hit_count, len(uncached_suspects))

    # --- PRIMARY LLM analysis ---
    primary_judgments = []
    primary_failures = []
    total_input_tokens = 0
    total_output_tokens = 0
    if uncached_suspects:
        primary_resp = llm_batch_analyze(
            items=uncached_suspects,
            analysis_type="PRIMARY",
            system_prompt=PRIMARY_SYSTEM_PROMPT,
            batch_size=settings.llm_batch_size,
        )
        primary_judgments = primary_resp.get("results", [])
        primary_failures = primary_resp.get("failures", [])
        total_input_tokens += primary_resp.get("input_tokens", 0)
        total_output_tokens += primary_resp.get("output_tokens", 0)

    # --- REFLECTION self-verification ---
    reflection_mismatch_count = 0
    reflection_failures = []
    if uncached_suspects:
        reflection_resp = llm_batch_analyze(
            items=uncached_suspects,
            analysis_type="REFLECTION",
            system_prompt=REFLECTION_SYSTEM_PROMPT,
            batch_size=settings.llm_batch_size,
        )
        reflection_judgments = reflection_resp.get("results", [])
        reflection_failures = reflection_resp.get("failures", [])
        total_input_tokens += reflection_resp.get("input_tokens", 0)
        total_output_tokens += reflection_resp.get("output_tokens", 0)
        reflection_map = {j.get("record_id"): j for j in reflection_judgments}

        for j in primary_judgments:
            rid = j.get("record_id")
            ref = reflection_map.get(rid)
            if ref and ref.get("is_error") != j.get("is_error"):
                # Mismatch: downgrade confidence to LOW, use reflection result
                j["confidence"] = "LOW"
                j["reflection_match"] = False
                j["reflection_note"] = "Primary/Reflection mismatch"
                reflection_mismatch_count += 1
            else:
                j["reflection_match"] = True
                j["reflection_note"] = ""

    # --- Cache write (HIGH confidence only) ---
    cache_entries = []
    for j in primary_judgments:
        if j.get("confidence") == "HIGH":
            suspect_match = next(
                (s for s in uncached_suspects if str(s.get("record_id")) == str(j.get("record_id"))),
                None,
            )
            if suspect_match:
                cache_entries.append({
                    "pattern_key": _build_cache_key(suspect_match),
                    "judgment": j,
                    "confidence": "HIGH",
                })
    if cache_entries:
        judgment_cache_write(entries=cache_entries)

    # Combine cached + new judgments
    all_judgments = cached_judgments + primary_judgments

    # Compute stats
    error_count = sum(1 for j in all_judgments if j.get("is_error"))
    high_count = sum(1 for j in all_judgments if j.get("confidence") == "HIGH")
    medium_count = sum(1 for j in all_judgments if j.get("confidence") == "MEDIUM")
    low_count = sum(1 for j in all_judgments if j.get("confidence") == "LOW")

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
        f.get("error", "unknown") for f in primary_failures + reflection_failures if f.get("error")
    })

    result["judgments_s3_path"] = judgments_s3_path
    result["analysis_stats"] = {
        "pipeline_id": pipeline_id,
        "judgments_s3_path": judgments_s3_path,
        "total_analyzed": len(all_judgments),
        "error_count": error_count,
        "high_confidence_count": high_count,
        "medium_confidence_count": medium_count,
        "low_confidence_count": low_count,
        "reflection_mismatch_count": reflection_mismatch_count,
        "cache_hit_count": cache_hit_count,
        "suspect_input_count": len(suspects),
        "primary_failed_count": len(primary_failures),
        "reflection_failed_count": len(reflection_failures),
        "failure_reasons": all_failure_reasons,
        "input_tokens": total_input_tokens + state.get("rv_input_tokens", 0),
        "output_tokens": total_output_tokens + state.get("rv_output_tokens", 0),
    }
    result["_records_processed"] = len(all_judgments)

    # Store analysis stats in pipeline state
    pipeline_state_write(key="analysis_stats", value=result["analysis_stats"])

    return result
