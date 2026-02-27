"""LLM batch analysis tool using Amazon Bedrock Converse API."""

import json
import logging
import time

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_bedrock_client
from ai_dq_agent.tools.utils import retry_with_backoff

logger = logging.getLogger(__name__)


@tool
def llm_batch_analyze(
    items: list[dict],
    analysis_type: str,
    system_prompt: str,
    batch_size: int = 50,
    temperature: float = 0.1,
    max_tokens: int = 16384,
    max_retries: int = 2,
) -> dict:
    """Analyze suspect items using LLM via Bedrock Converse API.

    Args:
        items: List of dicts with record_id, columns, suspect_reason, schema_context.
        analysis_type: 'PRIMARY' for initial judgment or 'REFLECTION' for self-review.
        system_prompt: System prompt for the LLM.
        batch_size: Number of items per LLM call.
        temperature: LLM temperature (lower = more deterministic).
        max_tokens: Maximum response tokens.
        max_retries: Retry count for transient API errors.

    Returns:
        Dict with results, processed_count, failed_count, and failures.
    """
    start = time.monotonic()
    settings = get_settings()
    logger.info(
        "[llm_batch_analyze] started: %d items, type=%s, batch_size=%d",
        len(items), analysis_type, batch_size,
    )

    bedrock = get_bedrock_client()
    all_results = []
    failures = []
    total_input_tokens = 0
    total_output_tokens = 0

    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]

        user_message = _build_user_message(batch, analysis_type)

        try:
            def _call_bedrock():
                return bedrock.converse(
                    modelId=settings.bedrock_model_id,
                    messages=[{"role": "user", "content": [{"text": user_message}]}],
                    system=[{"text": system_prompt}],
                    inferenceConfig={"temperature": temperature, "maxTokens": max_tokens},
                )

            response = retry_with_backoff(
                _call_bedrock,
                max_retries=max_retries,
                retryable_exceptions=(ClientError,),
            )

            # Extract token usage from Bedrock response
            usage = response.get("usage", {})
            total_input_tokens += usage.get("inputTokens", 0)
            total_output_tokens += usage.get("outputTokens", 0)

            response_text = _extract_response_text(response)
            parsed = _parse_llm_response(response_text, batch)
            all_results.extend(parsed["results"])
            failures.extend(parsed["failures"])

        except Exception as e:
            logger.error("[llm_batch_analyze] batch %d failed: %s", i // batch_size, e)
            for item in batch:
                failures.append({
                    "record_id": item.get("record_id", ""),
                    "error": str(e),
                })

    duration = time.monotonic() - start
    logger.info(
        "[llm_batch_analyze] completed: %d results, %d failures in %.2fs (tokens: in=%d, out=%d)",
        len(all_results), len(failures), duration, total_input_tokens, total_output_tokens,
    )

    return {
        "status": "success",
        "results": all_results,
        "processed_count": len(all_results),
        "failed_count": len(failures),
        "failures": failures,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
    }


def _build_user_message(batch: list[dict], analysis_type: str) -> str:
    """Build the user message with batch items."""
    items_json = json.dumps(batch, ensure_ascii=False, indent=2)

    if analysis_type == "REFLECTION":
        return (
            f"다음은 이전 1차 판정 결과입니다. 각 판정을 재검토하고 "
            f"동의 여부와 근거를 JSON array로 반환하세요.\n\n{items_json}"
        )

    return (
        f"다음 의심 항목들을 분석하여 각 항목에 대해 오류 여부를 판정하세요.\n"
        f"각 항목에 대해 is_error, error_type, confidence(HIGH/MEDIUM/LOW), "
        f"evidence(근거), suggested_correction을 JSON array로 반환하세요.\n\n"
        f"suggested_correction 작성 지침:\n"
        f'- is_error가 true이고 올바른 값을 추론할 수 있으면 {{"컬럼명": 보정값}} 형태로 제안하세요.\n'
        f"- current_values를 참고하여 어떤 컬럼이 잘못되었는지 파악하세요.\n"
        f"- 보정값을 확신할 수 없으면 null로 반환하세요.\n\n"
        f"{items_json}"
    )


def _extract_response_text(response: dict) -> str:
    """Extract text content from Bedrock Converse response."""
    output = response.get("output", {})
    message = output.get("message", {})
    content_blocks = message.get("content", [])
    texts = [block.get("text", "") for block in content_blocks if "text" in block]
    return "\n".join(texts)


def _parse_llm_response(text: str, batch: list[dict]) -> dict:
    """Parse LLM JSON response into structured results."""
    results = []
    failures = []

    # Try to extract JSON array from response
    try:
        # Find JSON array in response text
        start_idx = text.find("[")
        end_idx = text.rfind("]")
        if start_idx != -1 and end_idx != -1:
            json_str = text[start_idx : end_idx + 1]
            parsed = json.loads(json_str)
        else:
            parsed = json.loads(text)

        if not isinstance(parsed, list):
            parsed = [parsed]

        for item in parsed:
            # Validate required fields
            if "evidence" not in item and "reasoning" not in item:
                failures.append({"record_id": item.get("record_id", ""), "error": "Missing evidence field"})
                continue

            results.append({
                "record_id": item.get("record_id", ""),
                "is_error": item.get("is_error", False),
                "error_type": item.get("error_type", ""),
                "confidence": item.get("confidence", "LOW"),
                "reasoning": item.get("evidence", item.get("reasoning", "")),
                "suggested_correction": item.get("suggested_correction"),
            })

    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("[llm_batch_analyze] JSON parse failed: %s", e)
        for item in batch:
            failures.append({"record_id": item.get("record_id", ""), "error": f"JSON parse error: {e}"})

    return {"results": results, "failures": failures}
