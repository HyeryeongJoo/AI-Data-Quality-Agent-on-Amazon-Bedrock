"""Correction Agent — apply approved corrections with audit logging and quarantine."""

import logging

from ai_dq_agent.agents._node_utils import node_wrapper
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import (
    correction_log_write,
    dynamodb_batch_write,
    feedback_log_write,
    quarantine_write,
    slack_send_message,
    snapshot_save,
)

logger = logging.getLogger(__name__)

TOOLS = [
    snapshot_save,
    dynamodb_batch_write,
    correction_log_write,
    feedback_log_write,
    quarantine_write,
    slack_send_message,
]


@node_wrapper("correction")
def invoke_correction(state: dict) -> dict:
    """Apply approved corrections to source data and quarantine bad records."""
    result = {**state}
    settings = get_settings()
    pipeline_id = state.get("pipeline_id", "unknown")

    approval_status = state.get("approval_status", "")
    dry_run = state.get("dry_run", False)

    # Skip correction for non-approved statuses
    if approval_status in ("rejected", "expired", "no_errors") or dry_run:
        reason = "dry_run" if dry_run else approval_status
        logger.info("[%s] Correction skipped: %s", pipeline_id, reason)

        if approval_status == "rejected":
            all_judgments = state.get("_all_judgments", [])
            error_judgments = [j for j in all_judgments if j.get("is_error")]
            if error_judgments:
                feedback_log_write(
                    pipeline_run_id=pipeline_id,
                    feedbacks=[{
                        "record_id": str(j.get("record_id", "")),
                        "reviewer_action": "rejected",
                        "rejection_reason": state.get("rejection_reason", "Reviewer rejected"),
                    } for j in error_judgments],
                )

        slack_send_message(
            channel=settings.slack_channel_id,
            message=f"[DQ Agent] Pipeline {pipeline_id}: 보정 미수행 ({reason})",
        )
        result["_records_processed"] = 0
        return result

    # Process approved items
    approved_items = state.get("approved_items", [])
    if not approved_items:
        logger.info("[%s] No approved items to correct", pipeline_id)
        result["_records_processed"] = 0
        return result

    # --- Quarantine HIGH confidence errors ---
    high_errors = [
        item for item in approved_items
        if item.get("is_error") and item.get("confidence") == "HIGH"
    ]
    quarantined_count = 0
    if high_errors:
        q_resp = quarantine_write(
            records=high_errors,
            reason=f"Pipeline {pipeline_id}: HIGH confidence errors quarantined before correction",
            pipeline_id=pipeline_id,
        )
        quarantined_count = q_resp.get("quarantined_count", 0)
        logger.info("[%s] Quarantined %d HIGH confidence error records", pipeline_id, quarantined_count)

    # --- Apply corrections ---
    # Save snapshot first
    snapshot_resp = snapshot_save(
        pipeline_run_id=pipeline_id,
        records=approved_items,
    )
    snapshot_id = snapshot_resp.get("snapshot_id", "")

    corrections = []
    success_count = 0
    failure_count = 0
    failures = []

    for item in approved_items:
        record_id = str(item.get("record_id", ""))
        if not record_id:
            continue

        raw_correction = item.get("suggested_correction") or {}

        # Support two formats:
        # 1) Flat dict: {"column_name": corrected_value, ...}  (new LLM format)
        # 2) Legacy dict: {"column": "col_name", "value": val}
        if "column" in raw_correction and "value" in raw_correction:
            # Legacy format — single column correction
            correction_pairs = [(raw_correction["column"], raw_correction["value"])]
        elif raw_correction:
            # Flat dict — may have multiple column corrections
            correction_pairs = list(raw_correction.items())
        else:
            # Fallback to legacy flat fields
            col = item.get("correction_column", "")
            val = item.get("correction_value")
            correction_pairs = [(col, val)] if col else []

        if not correction_pairs:
            continue

        for correction_column, correction_value in correction_pairs:
            if not correction_column:
                continue
            try:
                write_resp = dynamodb_batch_write(
                    table_name=settings.dynamodb_table_name,
                    items=[{
                        "record_id": record_id,
                        correction_column: correction_value,
                    }],
                )

                if write_resp.get("status") in ("completed", "success"):
                    corrections.append({
                        "record_id": record_id,
                        "column": correction_column,
                        "original_value": item.get("current_values", {}).get(correction_column),
                        "corrected_value": correction_value,
                        "confidence": item.get("confidence", ""),
                        "impact_score": item.get("impact_score", 0.0),
                        "status": "success",
                    })
                    success_count += 1
                else:
                    failure_count += 1
                    failures.append({
                        "record_id": record_id,
                        "error": write_resp.get("error", "Write failed"),
                    })

            except Exception as e:
                logger.error("[%s] Correction failed for %s.%s: %s", pipeline_id, record_id, correction_column, e)
                failure_count += 1
                failures.append({"record_id": record_id, "error": str(e)})

    # Log corrections
    if corrections:
        correction_log_write(
            pipeline_run_id=pipeline_id,
            corrections=corrections,
            approved_by=state.get("reviewer_id", "unknown"),
            approved_at=state.get("approved_at", ""),
            snapshot_id=snapshot_id,
        )

    # Send completion notification
    msg = (
        f"[DQ Agent] Pipeline {pipeline_id} 보정 완료\n"
        f"- 시도: {len(approved_items)}건\n"
        f"- 성공: {success_count}건\n"
        f"- 실패: {failure_count}건\n"
        f"- 격리: {quarantined_count}건"
    )
    if failures:
        failure_ids = [f["record_id"] for f in failures[:5]]
        msg += f"\n- 실패 ID: {', '.join(failure_ids)}"
        if len(failures) > 5:
            msg += f" 외 {len(failures) - 5}건"

    slack_send_message(channel=settings.slack_channel_id, message=msg)

    result["_records_processed"] = success_count
    result["quarantined_count"] = quarantined_count
    return result
