"""Graph pipeline definition — 5-node DQ validation pipeline.

Pipeline flow::

    coordinator ─(has_data)─► rule_validator ─(has_suspects)─► llm_analyzer → report_notify → correction
                                      │                                            ▲
                                      └──────────(no_suspects)──────────────────────┘

Uses strands.multiagent.graph.GraphBuilder with FunctionNodeAgent adapters.
Falls back to _SimplePipeline when GraphBuilder is unavailable.
"""

import logging
from typing import Any

from ai_dq_agent.agents.coordinator import invoke_coordinator
from ai_dq_agent.agents.correction import invoke_correction
from ai_dq_agent.agents.dq_validator_agent import invoke_rule_validator
from ai_dq_agent.agents.llm_analyzer import invoke_llm_analyzer
from ai_dq_agent.agents.report_notify import invoke_report_notify

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FunctionNodeAgent — adapts (state → state) functions to AgentBase Protocol
# ---------------------------------------------------------------------------


class _FunctionNodeAgent:
    """Wraps a state-transforming function as a strands AgentBase-compatible executor.

    The strands GraphBuilder requires AgentBase instances (Protocol with __call__,
    invoke_async, stream_async).  This adapter wraps existing node functions that
    follow the ``(dict) -> dict`` pattern so they can be used as graph nodes
    without rewriting them as full Strands Agents.

    State is shared between nodes through the owning DQPipeline instance.
    """

    def __init__(self, func, name: str, pipeline: "DQPipeline") -> None:
        self.func = func
        self.name = name
        self.id = name
        self._pipeline = pipeline

    # -- AgentBase Protocol methods ------------------------------------------

    def __call__(self, prompt: Any = None, **kwargs: Any):
        return self._execute()

    async def invoke_async(self, prompt: Any = None, **kwargs: Any):
        return self._execute()

    async def stream_async(self, prompt: Any = None, **kwargs: Any):
        result = self._execute()
        yield {"result": result}

    # -- internals -----------------------------------------------------------

    def _execute(self):
        """Run the wrapped function with the pipeline's shared state."""
        result_state = self.func(self._pipeline._state)
        self._pipeline._state = result_state
        return self._make_result()

    @staticmethod
    def _make_result():
        from strands.agent.agent_result import AgentResult

        return AgentResult(
            stop_reason="end_turn",
            message={"role": "assistant", "content": [{"text": "done"}]},
            metrics=None,
            state=None,
        )


# ---------------------------------------------------------------------------
# DQPipeline — GraphBuilder-based 5-node pipeline
# ---------------------------------------------------------------------------


class DQPipeline:
    """5-node DQ validation pipeline built with ``strands.multiagent.graph.GraphBuilder``.

    Pipeline flow::

        coordinator ─(has_data)─► rule_validator ─(has_suspects)─► llm_analyzer
                                          │                              ↓
                                          └──(no_suspects)──► report_notify → correction
    """

    def __init__(self) -> None:
        self._state: dict = {}
        self._graph = self._build_graph()

    def invoke(self, state: dict) -> dict:
        """Run the pipeline.  Interface matches ``_SimplePipeline.invoke``."""
        self._state = dict(state)
        self._graph("Run DQ validation pipeline")
        return self._state

    # -- node factory --------------------------------------------------------

    def _node(self, func, name: str) -> _FunctionNodeAgent:
        return _FunctionNodeAgent(func, name, self)

    # -- edge conditions -----------------------------------------------------

    def _has_data(self, graph_state) -> bool:
        """Edge condition: coordinator found records to validate."""
        return (
            not self._state.get("_early_exit")
            and self._state.get("total_records", 0) > 0
        )

    def _has_suspects(self, graph_state) -> bool:
        """Edge condition: rule_validator found suspects for LLM analysis."""
        return self._state.get("suspect_count", 0) > 0

    # -- graph construction --------------------------------------------------

    def _build_graph(self):
        from strands.multiagent.graph import GraphBuilder

        from ai_dq_agent.settings import get_settings

        settings = get_settings()
        builder = GraphBuilder()

        # Register 5 nodes
        builder.add_node(self._node(invoke_coordinator, "coordinator"), "coordinator")
        builder.add_node(self._node(invoke_rule_validator, "rule_validator"), "rule_validator")
        builder.add_node(self._node(invoke_llm_analyzer, "llm_analyzer"), "llm_analyzer")
        builder.add_node(self._node(invoke_report_notify, "report_notify"), "report_notify")
        builder.add_node(self._node(invoke_correction, "correction"), "correction")

        # Entry point
        builder.set_entry_point("coordinator")

        # Conditional: coordinator → rule_validator (only if data exists)
        builder.add_edge("coordinator", "rule_validator", condition=self._has_data)

        # Conditional: rule_validator → llm_analyzer (only if suspects exist)
        builder.add_edge("rule_validator", "llm_analyzer", condition=self._has_suspects)

        # Fallback: rule_validator → report_notify (no suspects — skip LLM)
        builder.add_edge("rule_validator", "report_notify", condition=lambda s: not self._has_suspects(s))

        # Linear: llm_analyzer → report_notify → correction
        builder.add_edge("llm_analyzer", "report_notify")
        builder.add_edge("report_notify", "correction")

        # Pipeline timeout
        builder.set_execution_timeout(settings.pipeline_timeout_minutes * 60)

        return builder.build()


# ---------------------------------------------------------------------------
# Fallback — simple sequential pipeline (no GraphBuilder dependency)
# ---------------------------------------------------------------------------


def route_after_coordinator(state: dict) -> str:
    """Route after coordinator: skip pipeline if no data."""
    if state.get("_early_exit") or state.get("total_records", 0) == 0:
        return "no_data"
    return "has_data"


def route_after_rule_validator(state: dict) -> str:
    """Route after rule validation: check for suspects."""
    if state.get("suspect_count", 0) > 0:
        return "has_suspects"
    return "no_suspects"


def route_after_report(state: dict) -> str:
    """Route after report: skip correction if no errors or dry run."""
    approval_status = state.get("approval_status", "")
    if state.get("dry_run") or approval_status in ("no_errors", "rejected", "expired"):
        return "skip_correction"
    return "needs_correction"


class _SimplePipeline:
    """Fallback sequential pipeline when GraphBuilder is unavailable."""

    def invoke(self, state: dict) -> dict:
        state = invoke_coordinator(state)
        if route_after_coordinator(state) == "no_data":
            return state

        state = invoke_rule_validator(state)

        if route_after_rule_validator(state) == "has_suspects":
            state = invoke_llm_analyzer(state)

        state = invoke_report_notify(state)

        if route_after_report(state) == "needs_correction":
            state = invoke_correction(state)

        return state


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_pipeline():
    """Build the 5-node DQ validation pipeline.

    Returns ``DQPipeline`` (GraphBuilder-based) when strands.multiagent is
    available, otherwise falls back to ``_SimplePipeline``.
    """
    try:
        return DQPipeline()
    except ImportError:
        logger.warning(
            "strands.multiagent.graph not available, using simple sequential runner"
        )
        return _SimplePipeline()
