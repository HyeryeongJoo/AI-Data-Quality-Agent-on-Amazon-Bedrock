"""Address normalization tool using 행정안전부 도로명주소 API."""

import logging
import time

from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.http_client import get_http_client

logger = logging.getLogger(__name__)


@tool
def address_normalize(
    addresses: list[dict],
    batch_size: int = 100,
    timeout_seconds: float = 30.0,
) -> dict:
    """Normalize addresses by querying the Korean address API.

    Each address dict must have 'record_id' and 'address_text' keys.

    Args:
        addresses: List of dicts with record_id and address_text.
        batch_size: Number of addresses per batch.
        timeout_seconds: Per-request timeout in seconds.

    Returns:
        Dict with results, success_count, failure_count, and failures.
    """
    start = time.monotonic()
    logger.info("[address_normalize] started: %d addresses, batch_size=%d", len(addresses), batch_size)

    settings = get_settings()
    results = []
    failures = []

    for i in range(0, len(addresses), batch_size):
        batch = addresses[i : i + batch_size]

        for addr in batch:
            record_id = addr.get("record_id", "")
            address_text = addr.get("address_text", "")

            try:
                client = get_http_client()
                resp = client.get(
                    settings.address_api_url,
                    params={
                        "confmKey": settings.address_api_key,
                        "keyword": address_text,
                        "resultType": "json",
                        "countPerPage": "5",
                        "currentPage": "1",
                    },
                    timeout=timeout_seconds,
                )
                resp.raise_for_status()
                data = resp.json()

                juso_list = data.get("results", {}).get("juso", [])

                if not juso_list:
                    results.append({
                        "record_id": record_id,
                        "original_address": address_text,
                        "normalized_address": address_text,
                        "address_type": "ambiguous",
                        "confidence": 0.0,
                    })
                elif len(juso_list) == 1:
                    juso = juso_list[0]
                    road_addr = juso.get("roadAddr", "")
                    jibun_addr = juso.get("jibunAddr", "")
                    addr_type = "road" if road_addr else ("jibun" if jibun_addr else "ambiguous")
                    results.append({
                        "record_id": record_id,
                        "original_address": address_text,
                        "normalized_address": road_addr or jibun_addr or address_text,
                        "address_type": addr_type,
                        "confidence": 0.9,
                    })
                else:
                    # Multiple results → ambiguous but use first match
                    juso = juso_list[0]
                    road_addr = juso.get("roadAddr", "")
                    results.append({
                        "record_id": record_id,
                        "original_address": address_text,
                        "normalized_address": road_addr or address_text,
                        "address_type": "ambiguous",
                        "confidence": 0.5,
                    })

            except Exception as e:
                logger.warning("[address_normalize] API call failed for %s: %s", record_id, e)
                failures.append({
                    "record_id": record_id,
                    "address_text": address_text,
                    "error": str(e),
                })

    # Fallback: if all failed, mark everything as ambiguous (DP-02)
    if len(failures) == len(addresses) and len(addresses) > 0:
        logger.warning("[address_normalize] API total failure — applying fallback (all ambiguous)")
        results = [
            {
                "record_id": a.get("record_id", ""),
                "original_address": a.get("address_text", ""),
                "normalized_address": a.get("address_text", ""),
                "address_type": "ambiguous",
                "confidence": 0.0,
            }
            for a in addresses
        ]
        failures = []

    duration = time.monotonic() - start
    logger.info(
        "[address_normalize] completed: %d success, %d failures in %.2fs",
        len(results), len(failures), duration,
    )

    return {
        "status": "success",
        "results": results,
        "success_count": len(results),
        "failure_count": len(failures),
        "failures": failures,
    }
