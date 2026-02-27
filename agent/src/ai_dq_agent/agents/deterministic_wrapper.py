"""Deterministic agent wrapper — wraps fixed-logic node functions as AgentBase.

Provides a unified interface for deterministic nodes (coordinator, report_notify,
correction) so they can be used alongside autonomous Strands Agent nodes in the
GraphBuilder pipeline.
"""

import logging
from typing import Callable

logger = logging.getLogger(__name__)


class DeterministicAgentWrapper:
    """Wraps a deterministic node function to comply with the graph node interface.

    Deterministic nodes don't use LLM reasoning — they execute fixed logic.
    This wrapper simply delegates to the underlying invoke function.
    """

    def __init__(self, name: str, invoke_fn: Callable[[dict], dict], tools: list | None = None):
        self.name = name
        self._invoke_fn = invoke_fn
        self.tools = tools or []

    def __call__(self, state: dict) -> dict:
        """Execute the deterministic node function."""
        return self._invoke_fn(state)

    def __repr__(self) -> str:
        return f"DeterministicAgentWrapper(name={self.name!r})"
