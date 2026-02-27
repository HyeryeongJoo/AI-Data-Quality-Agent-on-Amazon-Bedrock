"""Shared utilities for graph node functions.

Provides:
- node_wrapper decorator (DP-A01): logging, timing, stage_results, error handling
- validate_state_keys (BR-13): required state key validation
- Pipeline logging helpers (DP-A05)
- Custom exceptions (DP-A04)
"""

import logging
import time
from datetime import datetime, timezone
from functools import wraps

from ai_dq_agent.agents._progress import write_progress

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Base exception for pipeline-level errors."""


class PipelineTimeoutError(PipelineError):
    """Raised when pipeline exceeds the configured timeout."""


def node_wrapper(stage_name: str):
    """Decorator for graph node functions.

    Automatically handles:
    - Start/completion logging with pipeline_id
    - Execution timing measurement
    - stage_results dict update in GraphState
    - Error logging and re-raise for Graph-level handling
    """

    def decorator(func):
        @wraps(func)
        def wrapper(state: dict) -> dict:
            pipeline_id = state.get("pipeline_id", "unknown")
            started_at = datetime.now(timezone.utc).isoformat()

            logger.info("[%s] %s started", pipeline_id, stage_name)
            write_progress(state, stage_name, "running")
            start = time.monotonic()

            try:
                result = func(state)
                duration = time.monotonic() - start
                completed_at = datetime.now(timezone.utc).isoformat()

                stage_results = dict(result.get("stage_results", {}))
                stage_results[stage_name] = {
                    "status": "completed",
                    "started_at": started_at,
                    "completed_at": completed_at,
                    "duration_seconds": round(duration, 2),
                    "records_processed": result.get("_records_processed", 0),
                }
                result["stage_results"] = stage_results
                result.pop("_records_processed", None)

                write_progress(result, stage_name, "completed")

                logger.info(
                    "[%s] %s completed in %.2fs",
                    pipeline_id,
                    stage_name,
                    duration,
                )
                return result

            except Exception as e:
                duration = time.monotonic() - start
                logger.error(
                    "[%s] %s failed after %.2fs: %s",
                    pipeline_id,
                    stage_name,
                    duration,
                    e,
                )
                stage_results = dict(state.get("stage_results", {}))
                stage_results[stage_name] = {
                    "status": "failed",
                    "started_at": started_at,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "duration_seconds": round(duration, 2),
                    "error_message": str(e),
                }
                state = {**state, "stage_results": stage_results}
                write_progress(state, stage_name, "failed")
                raise

        return wrapper

    return decorator


def validate_state_keys(state: dict, required_keys: list[str]) -> None:
    """Raise ValueError if any required key is missing from state."""
    missing = [k for k in required_keys if k not in state or state[k] is None]
    if missing:
        raise ValueError(f"Missing required state keys: {missing}")


def log_pipeline_start(pipeline_id: str, trigger_type: str) -> None:
    """Log pipeline execution start."""
    logger.info(
        "Pipeline started | pipeline_id=%s trigger=%s",
        pipeline_id,
        trigger_type,
    )


def log_pipeline_complete(pipeline_id: str, stage_results: dict) -> None:
    """Log pipeline completion with stage summary."""
    total_duration = sum(
        s.get("duration_seconds", 0) for s in stage_results.values()
    )
    stages_summary = {
        name: info.get("status", "unknown") for name, info in stage_results.items()
    }
    logger.info(
        "Pipeline completed | pipeline_id=%s total_duration=%.2fs stages=%s",
        pipeline_id,
        total_duration,
        stages_summary,
    )


def log_pipeline_stats(pipeline_id: str, state: dict) -> None:
    """Log pipeline processing statistics."""
    total_records = state.get("total_records", 0)
    suspect_count = state.get("suspect_count", 0)
    analysis_stats = state.get("analysis_stats", {})

    logger.info(
        "Pipeline stats | pipeline_id=%s total_records=%d suspect_count=%d "
        "suspect_ratio=%.4f error_count=%d cache_hit_count=%d",
        pipeline_id,
        total_records,
        suspect_count,
        suspect_count / max(total_records, 1),
        analysis_stats.get("error_count", 0),
        analysis_stats.get("cache_hit_count", 0),
    )
