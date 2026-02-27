"""Report & Notify Agent — report generation, health score, and Slack notifications."""

import logging
from datetime import datetime, timezone

from ai_dq_agent.agents._node_utils import node_wrapper, validate_state_keys
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    pipeline_state_read,
    pipeline_state_write,
    report_generate,
    s3_read_objects,
    slack_send_interactive_message,
    slack_send_message,
)

logger = logging.getLogger(__name__)

TOOLS = [
    report_generate,
    slack_send_message,
    slack_send_interactive_message,
    pipeline_state_read,
    pipeline_state_write,
]


def _compute_health_score(
    total_scanned: int,
    total_errors: int,
    high_confidence: int,
    stats_by_severity: dict,
) -> dict:
    """Compute table health score and status.

    Rate-based formula — proportional to total scanned records:
      weighted_errors = HIGH * 1.0 + (MEDIUM + LOW) * 0.5
      error_rate = weighted_errors / total_scanned
      health_score = 1.0 - error_rate   (clamped to [0.0, 1.0])

    Thresholds: >= 0.8 healthy, >= 0.5 warning, < 0.5 critical.
    """
    critical_count = stats_by_severity.get("critical", 0)   # HIGH confidence
    warning_count = stats_by_severity.get("warning", 0)      # MEDIUM + LOW

    if total_scanned > 0:
        weighted_errors = critical_count * 1.0 + warning_count * 0.5
        error_rate = weighted_errors / total_scanned
        health_score = max(0.0, min(1.0, 1.0 - error_rate))
    else:
        health_score = 1.0

    if health_score >= 0.8:
        status = "healthy"
    elif health_score >= 0.5:
        status = "warning"
    else:
        status = "critical"

    return {
        "table_name": "",  # set by caller
        "health_score": round(health_score, 4),
        "status": status,
        "violation_count": total_errors,
        "critical_violation_count": critical_count,
        "warning_violation_count": warning_count,
        "last_checked": datetime.now(timezone.utc).isoformat(),
    }


@node_wrapper("report_notify")
def invoke_report_notify(state: dict) -> dict:
    """Generate DQ report with health score and send Slack notification."""
    validate_state_keys(state, ["pipeline_id"])
    result = {**state}
    settings = get_settings()
    pipeline_id = state["pipeline_id"]

    validation_stats = state.get("validation_stats", {})
    analysis_stats = state.get("analysis_stats", {})

    # If llm_analyzer was skipped (no suspects), create analysis_stats with rule_validator tokens
    if not analysis_stats:
        analysis_stats = {
            "pipeline_id": pipeline_id,
            "total_analyzed": 0,
            "error_count": 0,
            "high_confidence_count": 0,
            "medium_confidence_count": 0,
            "low_confidence_count": 0,
            "input_tokens": state.get("rv_input_tokens", 0),
            "output_tokens": state.get("rv_output_tokens", 0),
        }
        result["analysis_stats"] = analysis_stats

    total_scanned = validation_stats.get("total_scanned", state.get("total_records", 0))
    total_suspects = validation_stats.get("suspect_count", state.get("suspect_count", 0))
    total_errors = analysis_stats.get("error_count", 0)
    high_confidence = analysis_stats.get("high_confidence_count", 0)
    error_type_dist = validation_stats.get("stats_by_error_type", {})

    # Build severity stats from confirmed errors (not raw suspects)
    # HIGH confidence confirmed errors → critical, MEDIUM/LOW → warning
    confirmed_severity = {
        "critical": analysis_stats.get("high_confidence_count", 0),
        "warning": analysis_stats.get("medium_confidence_count", 0)
                   + analysis_stats.get("low_confidence_count", 0),
    }

    # Compute health score based on confirmed errors
    health = _compute_health_score(
        total_scanned=total_scanned,
        total_errors=total_errors,
        high_confidence=high_confidence,
        stats_by_severity=confirmed_severity,
    )
    health["table_name"] = settings.dynamodb_table_name
    result["table_health"] = health

    # Store health in pipeline state
    pipeline_state_write(key="table_health", value=health)

    # Generate report
    report_resp = report_generate(
        pipeline_id=pipeline_id,
        total_scanned=total_scanned,
        total_suspects=total_suspects,
        total_errors=total_errors,
        high_confidence_errors=high_confidence,
        correction_proposals=high_confidence,
        error_type_distribution=error_type_dist,
        s3_bucket=settings.s3_staging_bucket,
        s3_prefix=f"reports/{pipeline_id}",
    )
    report_s3_path = report_resp.get("report_s3_path", "")
    result["report_s3_path"] = report_s3_path

    # Health indicator emoji for Slack
    health_indicator = {"healthy": "GREEN", "warning": "YELLOW", "critical": "RED"}.get(health["status"], "?")

    # Notification logic
    if total_errors == 0:
        slack_send_message(
            channel=settings.slack_channel_id,
            message=(
                f"[DQ Agent] Pipeline {pipeline_id} 완료\n"
                f"Health: [{health_indicator}] {health['health_score']:.0%}\n"
                f"- 전체 검증: {total_scanned:,}건\n"
                f"- 의심 항목: {total_suspects:,}건\n"
                f"- 오류 확정: 0건\n"
                f"이상 없음"
            ),
        )
        result["approval_status"] = "no_errors"
        result["approved_items"] = []
    else:
        # Load judgments sorted by impact_score
        judgments = []
        if state.get("judgments_s3_path"):
            j_resp = s3_read_objects(
                s3_path=state["judgments_s3_path"],
                file_format="jsonl",
            )
            judgments = j_resp.get("records", [])

        # Sort by confidence level (HIGH first)
        conf_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        judgments.sort(key=lambda x: conf_order.get(x.get("confidence", "LOW"), 2))

        # Filter HIGH confidence confirmed errors for approval
        high_items = [
            j for j in judgments
            if j.get("is_error") and j.get("confidence") == "HIGH"
        ]

        summary_text = (
            f"[DQ Agent] Pipeline {pipeline_id} 검증 결과\n"
            f"Health: [{health_indicator}] {health['health_score']:.0%}\n"
            f"- 전체 검증: {total_scanned:,}건\n"
            f"- 의심 항목: {total_suspects:,}건\n"
            f"- 오류 확정: {total_errors}건 (HIGH: {high_confidence}건)\n"
            f"- 리포트: {report_s3_path}\n\n"
            f"보정 승인이 필요합니다."
        )

        slack_send_interactive_message(
            channel=settings.slack_channel_id,
            pipeline_id=pipeline_id,
            report_summary=summary_text,
            report_link=report_s3_path,
            correction_count=len(high_items),
        )

        result["approval_status"] = "pending"
        result["approved_items"] = high_items
        result["_all_judgments"] = judgments

    result["_records_processed"] = total_errors
    return result
