"""LLM judgment cache tools using DynamoDB TTL."""

import json
import logging
import time

from botocore.exceptions import ClientError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def judgment_cache_read(
    pattern_keys: list[str],
) -> dict:
    """Read cached LLM judgments from DynamoDB cache table.

    Args:
        pattern_keys: List of cache keys to look up.
            Format: '{error_type}:{column}:{normalized_pattern}'.

    Returns:
        Dict with hits (list), misses (list), and hit_rate.
    """
    start = time.monotonic()
    logger.info("[judgment_cache_read] started: %d keys", len(pattern_keys))

    settings = get_settings()
    client = get_dynamodb_client()
    now = int(time.time())

    hits = []
    misses = []

    # BatchGetItem supports up to 100 keys per call
    for i in range(0, len(pattern_keys), 100):
        batch_keys = pattern_keys[i : i + 100]
        request_keys = [{"pattern_key": {"S": pk}} for pk in batch_keys]

        try:
            response = client.batch_get_item(
                RequestItems={
                    settings.dynamodb_cache_table: {"Keys": request_keys}
                }
            )
            found_items = response.get("Responses", {}).get(settings.dynamodb_cache_table, [])

            found_keys = set()
            for item in found_items:
                pk = item["pattern_key"]["S"]
                ttl_val = int(item.get("ttl", {}).get("N", "0"))

                if ttl_val > 0 and ttl_val < now:
                    # Expired
                    misses.append(pk)
                else:
                    judgment_str = item.get("judgment", {}).get("S", "{}")
                    hits.append({
                        "pattern_key": pk,
                        "judgment": json.loads(judgment_str),
                        "cached_at": item.get("cached_at", {}).get("S", ""),
                    })
                found_keys.add(pk)

            # Keys not found in response
            for pk in batch_keys:
                if pk not in found_keys:
                    misses.append(pk)

        except ClientError as e:
            logger.error("[judgment_cache_read] batch_get_item failed: %s", e)
            misses.extend(batch_keys)

    total = len(pattern_keys)
    hit_rate = len(hits) / total if total > 0 else 0.0

    duration = time.monotonic() - start
    logger.info(
        "[judgment_cache_read] completed: %d hits, %d misses (%.1f%%) in %.2fs",
        len(hits), len(misses), hit_rate * 100, duration,
    )

    return {
        "status": "success",
        "hits": hits,
        "misses": misses,
        "hit_count": len(hits),
        "miss_count": len(misses),
        "hit_rate": hit_rate,
    }


@tool
def judgment_cache_write(
    entries: list[dict],
    ttl_days: int = 30,
) -> dict:
    """Write LLM judgments to DynamoDB cache table with TTL.

    Only HIGH confidence judgments should be cached.

    Args:
        entries: List of dicts with pattern_key, judgment (dict), and confidence.
        ttl_days: Time-to-live in days (default 30).

    Returns:
        Dict with written_count and status.
    """
    start = time.monotonic()
    logger.info("[judgment_cache_write] started: %d entries", len(entries))

    settings = get_settings()
    client = get_dynamodb_client()
    now = int(time.time())
    ttl_seconds = ttl_days * 86400
    written = 0

    for entry in entries:
        # Only cache HIGH confidence (BR-T08-2)
        if entry.get("confidence", "").upper() != "HIGH":
            continue

        pattern_key = entry.get("pattern_key", "")
        judgment = entry.get("judgment", {})
        cached_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        try:
            client.put_item(
                TableName=settings.dynamodb_cache_table,
                Item={
                    "pattern_key": {"S": pattern_key},
                    "judgment": {"S": json.dumps(judgment, ensure_ascii=False)},
                    "confidence": {"S": "HIGH"},
                    "cached_at": {"S": cached_at},
                    "ttl": {"N": str(now + ttl_seconds)},
                },
            )
            written += 1
        except ClientError as e:
            logger.error("[judgment_cache_write] put_item failed for %s: %s", pattern_key, e)

    duration = time.monotonic() - start
    logger.info("[judgment_cache_write] completed: %d written in %.2fs", written, duration)

    return {"status": "success", "written_count": written}
