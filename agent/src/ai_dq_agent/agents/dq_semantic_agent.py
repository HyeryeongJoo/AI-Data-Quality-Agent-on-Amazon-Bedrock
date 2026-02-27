"""Semantic Analyzer Agent — autonomous LLM analysis with iterative reasoning and impact scoring.

Agentic capabilities:
- Autonomy B: ReAct loop — LLM autonomously decides tool usage order.
- Autonomy C: Iterative reasoning — PRIMARY → REFLECTION → DEEP_ANALYSIS for low-confidence items.
- Autonomy D: Strategy switching — adjusts batch_size/temperature on throttling.
- Impact scoring: Prioritizes analysis by downstream impact.
- Root cause tracing: Traces violations back to upstream sources.
"""

import logging

from ai_dq_agent.agents._node_utils import node_wrapper, validate_state_keys
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    impact_score_compute,
    judgment_cache_read,
    judgment_cache_write,
    lineage_read,
    llm_batch_analyze,
    pipeline_state_read,
    pipeline_state_write,
    root_cause_trace,
    s3_read_objects,
    s3_write_objects,
)

logger = logging.getLogger(__name__)

SEMANTIC_TOOLS = [
    llm_batch_analyze,
    judgment_cache_read,
    judgment_cache_write,
    lineage_read,
    impact_score_compute,
    root_cause_trace,
    s3_read_objects,
    s3_write_objects,
    pipeline_state_read,
    pipeline_state_write,
]

SEMANTIC_SYSTEM_PROMPT = (
    "당신은 택배 데이터 품질 시맨틱 분석 전문가입니다.\n"
    "영향도 기반으로 분석 우선순위를 결정하고, 불확실한 건은 반복 재분석합니다.\n\n"
    "사용 가능한 도구:\n"
    "- llm_batch_analyze: LLM 배치 분석 (PRIMARY/REFLECTION/DEEP_ANALYSIS)\n"
    "- judgment_cache_read/write: 판정 캐시 읽기/쓰기\n"
    "- lineage_read: 테이블 리니지 읽기\n"
    "- impact_score_compute: 영향도 점수 계산\n"
    "- root_cause_trace: 근본 원인 추적\n"
    "- s3_read_objects/write_objects: 데이터 읽기/쓰기\n"
    "- pipeline_state_read/write: 파이프라인 상태\n\n"
    "분석 절차:\n"
    "1. lineage_read로 리니지 정보를 읽습니다.\n"
    "2. impact_score_compute로 영향도 점수를 계산합니다.\n"
    "3. 영향도 높은 순서대로 분석합니다.\n"
    "4. PRIMARY 분석 → REFLECTION 자기검증 → 불일치 건 DEEP_ANALYSIS\n"
    "5. HIGH confidence 오류에 대해 root_cause_trace를 실행합니다.\n"
    "6. 결과를 S3에 저장하고 pipeline_state에 기록합니다.\n\n"
    "전략 전환 (Autonomy D):\n"
    "- Bedrock throttling → batch_size를 20으로 축소\n"
    "- 결과 품질 낮음 → temperature를 0.05로 낮추고 system_prompt 강화\n\n"
    "반복 추론 (Autonomy C):\n"
    "- PRIMARY 분석 후 REFLECTION으로 자기 검증\n"
    "- 불일치 건(confidence=LOW)은 추가 컨텍스트와 함께 DEEP_ANALYSIS\n"
    "- DEEP_ANALYSIS 후에도 LOW인 건은 최종 결과로 확정"
)

PRIMARY_SYSTEM_PROMPT = (
    "당신은 택배 데이터 품질 검증 전문가입니다. "
    "Rule-based 검증에서 의심 항목으로 분류된 레코드를 분석하여 실제 오류 여부를 판정합니다.\n\n"
    "각 항목에 대해 다음 필드를 포함하는 JSON array를 반환하세요:\n"
    "- record_id: 레코드 식별자\n"
    "- is_error: 실제 오류인지 여부 (true/false)\n"
    "- error_type: 오류 유형\n"
    "- confidence: 판정 신뢰도 (HIGH/MEDIUM/LOW)\n"
    "- evidence: 판정 근거\n"
    "- suggested_correction: 수정 제안 (dict 또는 null)\n\n"
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

DEEP_ANALYSIS_SYSTEM_PROMPT = (
    "당신은 데이터 품질 심층 분석 전문가입니다.\n"
    "이전 1차 분석과 Reflection에서 불일치가 발생한 항목을 추가 컨텍스트와 함께 심층 분석합니다.\n"
    "추가 컨텍스트를 활용하여 더 정확한 판정을 내려주세요.\n\n"
    "각 항목에 대해 다음 필드를 포함하는 JSON array를 반환하세요:\n"
    "- record_id, is_error, confidence (HIGH/MEDIUM/LOW), evidence\n\n"
    "반드시 JSON array만 반환하세요."
)


def _build_cache_key(suspect: dict) -> str:
    """Build a cache lookup key from a suspect item."""
    return f"{suspect.get('error_type', '')}:{suspect.get('rule_id', '')}:{','.join(suspect.get('target_columns', []))}"


def create_semantic_agent():
    """Create a Semantic Analyzer Agent with iterative reasoning capabilities."""
    try:
        from strands import Agent
        from strands.models.bedrock import BedrockModel

        from ai_dq_agent.tools.aws_clients import get_bedrock_boto_config

        settings = get_settings()
        model = BedrockModel(
            model_id=settings.agent_model_id,
            region_name=settings.aws_region,
            boto_client_config=get_bedrock_boto_config(),
        )

        return Agent(
            model=model,
            tools=SEMANTIC_TOOLS,
            system_prompt=SEMANTIC_SYSTEM_PROMPT,
            max_handler_turns=15,
        )
    except ImportError:
        logger.warning("Strands Agent not available, using deterministic semantic analyzer")
        return None


@node_wrapper("semantic_analyzer")
def invoke_semantic_analyzer(state: dict) -> dict:
    """Run LLM semantic analysis with iterative reasoning, impact scoring, and root cause tracing."""
    validate_state_keys(state, ["suspects_s3_path", "suspect_count"])
    result = {**state}
    settings = get_settings()
    pipeline_id = state.get("pipeline_id", "unknown")
    table_name = settings.dynamodb_table_name

    # Load suspects
    read_resp = s3_read_objects(
        s3_path=state["suspects_s3_path"],
        file_format="jsonl",
    )
    suspects = read_resp.get("records", [])

    # Include delegated suspects from rule_validator
    from ai_dq_agent.tools.pipeline_state_tools import get_pipeline_state
    ps = get_pipeline_state()
    delegated = ps.get("delegated_suspects", [])
    if delegated:
        suspects.extend(delegated)
        logger.info("[%s] Added %d delegated suspects from rule_validator", pipeline_id, len(delegated))

    # Apply LLM max items cap
    if len(suspects) > settings.llm_max_items:
        logger.warning("[%s] Suspect count %d exceeds llm_max_items %d", pipeline_id, len(suspects), settings.llm_max_items)
        suspects = suspects[:settings.llm_max_items]

    # --- Impact scoring ---
    lineage_info = lineage_read(table_name=table_name)
    if lineage_info.get("status") == "success":
        impact_resp = impact_score_compute(violations=suspects, lineage_info=lineage_info)
        suspects = impact_resp.get("scored_violations", suspects)
        logger.info("[%s] Suspects sorted by impact score (max=%.2f)", pipeline_id,
                     impact_resp.get("max_impact_score", 0))

    # --- Cache lookup ---
    cache_keys = [_build_cache_key(s) for s in suspects]
    cache_resp = judgment_cache_read(pattern_keys=cache_keys)
    cached_results = {hit["pattern_key"]: hit["judgment"] for hit in cache_resp.get("hits", [])}

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
    if uncached_suspects:
        primary_resp = llm_batch_analyze(
            items=uncached_suspects,
            analysis_type="PRIMARY",
            system_prompt=PRIMARY_SYSTEM_PROMPT,
            batch_size=settings.llm_batch_size,
        )
        primary_judgments = primary_resp.get("results", [])

    # --- REFLECTION self-verification (Autonomy C) ---
    reflection_mismatch_count = 0
    mismatched_items = []
    if uncached_suspects:
        reflection_resp = llm_batch_analyze(
            items=uncached_suspects,
            analysis_type="REFLECTION",
            system_prompt=REFLECTION_SYSTEM_PROMPT,
            batch_size=settings.llm_batch_size,
        )
        reflection_judgments = reflection_resp.get("results", [])
        reflection_map = {j.get("record_id"): j for j in reflection_judgments}

        for j in primary_judgments:
            rid = j.get("record_id")
            ref = reflection_map.get(rid)
            if ref and ref.get("is_error") != j.get("is_error"):
                j["confidence"] = "LOW"
                j["reflection_match"] = False
                j["reflection_note"] = "Primary/Reflection mismatch"
                reflection_mismatch_count += 1
                mismatched_items.append(j)
            else:
                j["reflection_match"] = True
                j["reflection_note"] = ""

    # --- DEEP_ANALYSIS for mismatched items (Autonomy C — iterative reasoning) ---
    deep_analysis_count = 0
    if mismatched_items:
        logger.info("[%s] Running DEEP_ANALYSIS on %d mismatched items", pipeline_id, len(mismatched_items))
        deep_resp = llm_batch_analyze(
            items=mismatched_items,
            analysis_type="DEEP_ANALYSIS",
            system_prompt=DEEP_ANALYSIS_SYSTEM_PROMPT,
            batch_size=min(settings.llm_batch_size, 20),
            temperature=0.05,
        )
        deep_results = deep_resp.get("results", [])
        deep_map = {d.get("record_id"): d for d in deep_results}
        deep_analysis_count = len(deep_results)

        # Merge deep analysis results back into primary judgments
        for j in primary_judgments:
            rid = j.get("record_id")
            if rid in deep_map:
                deep = deep_map[rid]
                j["confidence"] = deep.get("confidence", j["confidence"])
                j["is_error"] = deep.get("is_error", j["is_error"])
                j["reflection_note"] = f"DEEP_ANALYSIS: {deep.get('reasoning', deep.get('evidence', ''))}"

    # --- Root cause tracing for HIGH confidence errors ---
    for j in primary_judgments:
        if j.get("is_error") and j.get("confidence") == "HIGH":
            violation = {
                "rule_id": j.get("rule_id", ""),
                "target_columns": [j.get("correction_column", "")] if j.get("correction_column") else [],
                "error_type": j.get("error_type", ""),
            }
            rc_resp = root_cause_trace(violation=violation, lineage_info=lineage_info)
            if rc_resp.get("status") in ("success", "partial"):
                j["root_cause"] = rc_resp.get("description", "")
                j["root_cause_table"] = rc_resp.get("root_cause_table", "")
                j["root_cause_column"] = rc_resp.get("root_cause_column", "")

    # --- Carry over impact_score ---
    suspect_map = {str(s.get("record_id")): s for s in suspects}
    for j in primary_judgments:
        rid = str(j.get("record_id"))
        if rid in suspect_map:
            j["impact_score"] = suspect_map[rid].get("impact_score", 0.0)

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

    # Sort by impact_score descending
    all_judgments.sort(key=lambda x: x.get("impact_score", 0), reverse=True)

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
        "deep_analysis_count": deep_analysis_count,
        "cache_hit_count": cache_hit_count,
        "delegated_suspect_count": len(delegated),
    }
    result["_records_processed"] = len(all_judgments)
    return result
