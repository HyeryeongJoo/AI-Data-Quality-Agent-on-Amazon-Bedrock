"""Agent delegation tools — dynamic inter-agent task delegation."""

import json
import logging
import time

from strands import tool

from ai_dq_agent.tools.pipeline_state_tools import _pipeline_state

logger = logging.getLogger(__name__)


@tool
def delegate_to_agent(
    agent_name: str,
    task_description: str,
    context: dict | str,
) -> dict:
    """Delegate a task to another agent in the pipeline.

    Stores the delegation request in the shared pipeline state so the
    target agent can pick it up when it runs. The graph routing logic
    reads delegation flags to decide which agent runs next.

    Supported target agents:
    - "semantic_analyzer": Delegate ambiguous rule-based items for LLM analysis.
    - "rule_validator": Delegate items needing re-validation with adjusted rules.
    - "profiler": Delegate additional profiling on specific columns.

    Args:
        agent_name: Name of the target agent to delegate to.
        task_description: Description of the delegated task.
        context: Context data for the target agent (dict or JSON string).

    Returns:
        Dict with delegation_id, status, and target agent.
    """
    start = time.monotonic()
    logger.info("[delegate_to_agent] started: target=%s, task=%s", agent_name, task_description[:100])

    if isinstance(context, str):
        try:
            context = json.loads(context)
        except json.JSONDecodeError:
            context = {"raw": context}

    delegation_id = f"deleg-{agent_name}-{int(time.time())}"

    delegation_entry = {
        "delegation_id": delegation_id,
        "target_agent": agent_name,
        "task_description": task_description,
        "context": context,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": "pending",
    }

    # Store in pipeline state under "delegations" list
    delegations = _pipeline_state.get("delegations", [])
    delegations.append(delegation_entry)
    _pipeline_state["delegations"] = delegations

    # Also set specific flags for graph routing
    if agent_name == "semantic_analyzer":
        suspects = context.get("suspects", []) if isinstance(context, dict) else []
        existing = _pipeline_state.get("delegated_suspects", [])
        _pipeline_state["delegated_suspects"] = existing + suspects

    duration = time.monotonic() - start
    logger.info("[delegate_to_agent] completed: id=%s in %.2fs", delegation_id, duration)

    return {
        "status": "success",
        "delegation_id": delegation_id,
        "target_agent": agent_name,
        "task_description": task_description,
    }
