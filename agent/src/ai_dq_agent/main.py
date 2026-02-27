"""AI DQ Agent Pipeline v2 — entry point with pipeline state integration."""

import argparse
import logging

from ai_dq_agent.agents._node_utils import (
    PipelineError,
    log_pipeline_complete,
    log_pipeline_start,
    log_pipeline_stats,
)
from ai_dq_agent.agents.graph import build_pipeline
from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools import execution_state_write, slack_send_message
from ai_dq_agent.tools.pipeline_state_tools import get_pipeline_state, reset_pipeline_state

logger = logging.getLogger(__name__)


def _acquire_pipeline_lock(pipeline_id: str) -> bool:
    """Attempt to acquire the distributed pipeline lock."""
    try:
        resp = execution_state_write(
            state_key="pipeline_lock",
            value={"pipeline_id": pipeline_id, "status": "running"},
            pipeline_run_id="ACTIVE",
        )
        return resp.get("status") == "success"
    except Exception:
        logger.warning("Pipeline lock acquisition failed")
        return False


def _release_pipeline_lock(pipeline_id: str, final_status: str) -> None:
    """Release the distributed pipeline lock."""
    try:
        execution_state_write(
            state_key="pipeline_lock",
            value={"pipeline_id": pipeline_id, "status": final_status},
            pipeline_run_id="ACTIVE",
        )
    except Exception:
        logger.warning("Failed to release pipeline lock for %s", pipeline_id)


def run_pipeline(
    trigger_type: str = "schedule",
    event_records: list[dict] | None = None,
    dry_run: bool = False,
    s3_data_path: str | None = None,
    pipeline_id: str | None = None,
) -> dict:
    """Run the DQ validation pipeline.

    Args:
        trigger_type: 'schedule' for batch or 'event' for stream-triggered.
        event_records: DynamoDB stream event records (event trigger only).
        dry_run: If True, generate report only without corrections.
        s3_data_path: If provided, read data from this S3 path instead of DynamoDB.
        pipeline_id: If provided, use this pipeline_id instead of generating one.

    Returns:
        Final pipeline state dict.
    """
    settings = get_settings()

    # Reset inter-agent shared state for new pipeline run
    reset_pipeline_state()

    # Build graph
    graph = build_pipeline()

    # Initial state
    initial_state: dict = {
        "trigger_type": trigger_type,
        "event_records": event_records,
        "dry_run": dry_run,
        "s3_data_path": s3_data_path,
        "stage_results": {},
        "error": None,
    }
    if pipeline_id:
        initial_state["pipeline_id"] = pipeline_id

    pipeline_id = "unknown"

    try:
        result = graph.invoke(initial_state)
        pipeline_id = result.get("pipeline_id", pipeline_id)

        log_pipeline_start(pipeline_id, trigger_type)
        log_pipeline_stats(pipeline_id, result)
        log_pipeline_complete(pipeline_id, result.get("stage_results", {}))

        # Merge pipeline state into result for visibility
        ps = get_pipeline_state()
        result["pipeline_state"] = ps

        # Log health score
        health = ps.get("table_health", {})
        if health:
            logger.info(
                "Health score: %.0f%% (%s) for %s",
                health.get("health_score", 0) * 100,
                health.get("status", "unknown"),
                health.get("table_name", "unknown"),
            )

        # Update checkpoint on success
        if result.get("stage_results", {}).get("coordinator", {}).get("status") == "completed":
            execution_state_write(
                state_key="last_checkpoint",
                value={"timestamp": result.get("checkpoint_timestamp")},
                pipeline_run_id=pipeline_id,
            )

        return result

    except PipelineError as e:
        logger.error("Pipeline error: %s", e)
        _send_failure_alert(str(e), pipeline_id, settings)
        raise

    except Exception as e:
        logger.error("Unexpected pipeline failure: %s", e)
        _send_failure_alert(str(e), pipeline_id, settings)
        raise


def resume_pipeline(state: dict, approval: dict) -> dict:
    """Resume pipeline after HITL approval.

    Args:
        state: Pipeline state from interrupted execution.
        approval: Approval decision dict with 'decision' and optionally
                  'approved_item_ids', 'reviewer_id', 'rejection_reason'.

    Returns:
        Final pipeline state dict.
    """
    state = {**state}
    state["approval_status"] = approval.get("decision", "rejected")
    state["reviewer_id"] = approval.get("reviewer_id", "unknown")
    state["approved_at"] = approval.get("approved_at", "")

    if approval.get("decision") == "approved_partial":
        approved_ids = set(approval.get("approved_item_ids", []))
        state["approved_items"] = [
            item for item in state.get("approved_items", [])
            if str(item.get("record_id", "")) in approved_ids
        ]
    elif approval.get("decision") == "rejected":
        state["approved_items"] = []
        state["rejection_reason"] = approval.get("rejection_reason", "")

    graph = build_pipeline()

    try:
        result = graph.invoke(state)
        pipeline_id = result.get("pipeline_id", "unknown")
        log_pipeline_complete(pipeline_id, result.get("stage_results", {}))
        return result
    except Exception as e:
        pipeline_id = state.get("pipeline_id", "unknown")
        logger.error("Pipeline resume failed: %s", e)
        settings = get_settings()
        _send_failure_alert(str(e), pipeline_id, settings)
        raise


def _send_failure_alert(error_msg: str, pipeline_id: str, settings=None) -> None:
    """Send Slack failure notification."""
    if settings is None:
        settings = get_settings()
    try:
        slack_send_message(
            channel=settings.slack_channel_id,
            message=f"[DQ Agent] Pipeline {pipeline_id} 실패: {error_msg}",
        )
    except Exception:
        logger.warning("Failed to send failure alert to Slack")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="AI DQ Agent Pipeline v2")
    parser.add_argument(
        "--trigger",
        choices=["schedule", "event"],
        default="schedule",
        help="Trigger type (default: schedule)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report only, skip corrections",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    result = run_pipeline(trigger_type=args.trigger, dry_run=args.dry_run)
    pipeline_id = result.get("pipeline_id", "unknown")
    stages = result.get("stage_results", {})
    status_summary = {k: v.get("status") for k, v in stages.items()}

    # Print health info
    health = result.get("pipeline_state", {}).get("table_health", {})
    health_str = ""
    if health:
        health_str = f" | Health: {health.get('health_score', 0):.0%} ({health.get('status', '?')})"

    print(f"Pipeline {pipeline_id} completed: {status_summary}{health_str}")


if __name__ == "__main__":
    main()
