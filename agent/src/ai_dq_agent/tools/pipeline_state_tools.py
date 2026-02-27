"""Pipeline state tools — shared state between agents in the graph."""

import json
import logging
import time

from strands import tool

logger = logging.getLogger(__name__)

# In-process shared state for inter-agent communication within a single pipeline run.
# This is a module-level singleton dict; each pipeline run clears it at start.
_pipeline_state: dict = {}


def reset_pipeline_state() -> None:
    """Reset the shared pipeline state (called at pipeline start)."""
    _pipeline_state.clear()


@tool
def pipeline_state_read(
    key: str,
) -> dict:
    """Read a value from the shared pipeline state.

    The pipeline state is an in-process key-value store shared across
    all agent nodes within a single pipeline run.

    Args:
        key: State key to read.

    Returns:
        Dict with value, found flag, and available keys.
    """
    start = time.monotonic()
    value = _pipeline_state.get(key)
    found = key in _pipeline_state

    duration = time.monotonic() - start
    logger.debug("[pipeline_state_read] key=%s found=%s in %.4fs", key, found, duration)

    return {
        "status": "success",
        "key": key,
        "value": value,
        "found": found,
        "available_keys": list(_pipeline_state.keys()),
    }


@tool
def pipeline_state_write(
    key: str,
    value: str | int | float | bool | list | dict | None,
    merge: bool = False,
) -> dict:
    """Write a value to the shared pipeline state.

    Args:
        key: State key to write.
        value: Value to store (must be JSON-serializable).
        merge: If True and existing value is a dict, merge keys instead of replacing.

    Returns:
        Dict with key, status, and current keys.
    """
    start = time.monotonic()

    if merge and isinstance(value, dict) and isinstance(_pipeline_state.get(key), dict):
        _pipeline_state[key] = {**_pipeline_state[key], **value}
    elif merge and isinstance(value, list) and isinstance(_pipeline_state.get(key), list):
        _pipeline_state[key] = _pipeline_state[key] + value
    else:
        _pipeline_state[key] = value

    duration = time.monotonic() - start
    logger.debug("[pipeline_state_write] key=%s merge=%s in %.4fs", key, merge, duration)

    return {
        "status": "success",
        "key": key,
        "available_keys": list(_pipeline_state.keys()),
    }


def get_pipeline_state() -> dict:
    """Get a snapshot of the current pipeline state (for graph routing decisions)."""
    return dict(_pipeline_state)
