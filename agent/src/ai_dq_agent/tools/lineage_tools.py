"""Lineage and impact score tools — downstream impact analysis."""

import json
import logging
import time

from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.aws_clients import get_dynamodb_client

logger = logging.getLogger(__name__)


@tool
def lineage_read(
    table_name: str,
) -> dict:
    """Read table lineage information from DynamoDB.

    Returns upstream/downstream tables, query volume, and certification status.

    Args:
        table_name: Name of the table to look up lineage for.

    Returns:
        Dict with lineage data including upstream_tables, downstream_tables,
        query_volume_7d, certification_status, and column_lineage.
    """
    start = time.monotonic()
    logger.info("[lineage_read] started: table=%s", table_name)

    settings = get_settings()
    client = get_dynamodb_client()

    try:
        response = client.get_item(
            TableName=settings.dynamodb_lineage_table,
            Key={"table_name": {"S": table_name}},
        )

        item = response.get("Item")
        if not item:
            logger.info("[lineage_read] no lineage found for %s", table_name)
            return {
                "status": "not_found",
                "table_name": table_name,
                "upstream_tables": [],
                "downstream_tables": [],
                "query_volume_7d": 0,
                "certification_status": "uncertified",
                "column_lineage": {},
            }

        data_str = item.get("data", {}).get("S", "{}")
        lineage_data = json.loads(data_str)

        duration = time.monotonic() - start
        logger.info("[lineage_read] completed in %.2fs", duration)

        return {
            "status": "success",
            "table_name": table_name,
            "upstream_tables": lineage_data.get("upstream_tables", []),
            "downstream_tables": lineage_data.get("downstream_tables", []),
            "query_volume_7d": lineage_data.get("query_volume_7d", 0),
            "certification_status": lineage_data.get("certification_status", "uncertified"),
            "column_lineage": lineage_data.get("column_lineage", {}),
            "last_updated": lineage_data.get("last_updated", ""),
        }
    except Exception as e:
        duration = time.monotonic() - start
        logger.error("[lineage_read] failed: %s in %.2fs", e, duration)
        return {"status": "error", "error": str(e)}


@tool
def impact_score_compute(
    violations: list[dict],
    lineage_info: dict,
) -> dict:
    """Compute impact scores for violations based on downstream lineage.

    impact_score = severity_weight x downstream_table_count x query_volume_weight

    Args:
        violations: List of violation/suspect dicts with rule_id, severity, etc.
        lineage_info: Lineage data from lineage_read (downstream_tables, query_volume_7d).

    Returns:
        Dict with scored_violations list (sorted by impact_score desc) and statistics.
    """
    start = time.monotonic()
    logger.info("[impact_score_compute] started: %d violations", len(violations))

    downstream_count = len(lineage_info.get("downstream_tables", []))
    query_volume = lineage_info.get("query_volume_7d", 0)

    # Normalize query volume weight (0.1 ~ 1.0)
    # Use log scale to avoid extreme values; minimum weight is 0.1
    if query_volume > 0:
        import math
        query_volume_weight = min(1.0, max(0.1, math.log10(query_volume + 1) / 5.0))
    else:
        query_volume_weight = 0.1

    severity_weights = {"critical": 10.0, "warning": 5.0, "info": 1.0}

    scored = []
    for v in violations:
        severity = v.get("severity", "warning")
        sev_weight = severity_weights.get(severity, 1.0)
        score = sev_weight * max(downstream_count, 1) * query_volume_weight

        scored.append({
            **v,
            "impact_score": round(score, 2),
            "severity_weight": sev_weight,
            "downstream_table_count": downstream_count,
            "query_volume_weight": round(query_volume_weight, 4),
        })

    # Sort by impact_score descending
    scored.sort(key=lambda x: x["impact_score"], reverse=True)

    duration = time.monotonic() - start
    logger.info("[impact_score_compute] completed: %d scored in %.2fs", len(scored), duration)

    return {
        "status": "success",
        "scored_violations": scored,
        "total_count": len(scored),
        "max_impact_score": scored[0]["impact_score"] if scored else 0.0,
        "avg_impact_score": round(sum(s["impact_score"] for s in scored) / len(scored), 2) if scored else 0.0,
    }
