"""Tool functions for the AI DQ Agent pipeline v2.

All 33 @tool functions are re-exported here for convenient imports.
"""

from ai_dq_agent.tools.address_tools import address_normalize
from ai_dq_agent.tools.cache_tools import judgment_cache_read, judgment_cache_write
from ai_dq_agent.tools.correction_tools import (
    correction_log_write,
    feedback_log_write,
    snapshot_save,
)
from ai_dq_agent.tools.delegation_tools import delegate_to_agent
from ai_dq_agent.tools.dynamodb_tools import (
    dynamodb_batch_write,
    dynamodb_export_to_s3,
    dynamodb_scan_with_rate_limit,
    dynamodb_stream_read,
)
from ai_dq_agent.tools.lineage_tools import impact_score_compute, lineage_read
from ai_dq_agent.tools.llm_tools import llm_batch_analyze
from ai_dq_agent.tools.pipeline_state_tools import pipeline_state_read, pipeline_state_write
from ai_dq_agent.tools.profile_tools import profile_compute, profile_history_read, profile_history_write
from ai_dq_agent.tools.quarantine_tools import quarantine_write
from ai_dq_agent.tools.report_tools import report_generate
from ai_dq_agent.tools.root_cause_tools import root_cause_trace
from ai_dq_agent.tools.rule_generate_tools import rule_generate, rule_registry_update
from ai_dq_agent.tools.s3_tools import s3_read_objects, s3_write_objects
from ai_dq_agent.tools.slack_tools import (
    slack_receive_response,
    slack_send_interactive_message,
    slack_send_message,
)
from ai_dq_agent.tools.state_tools import execution_state_read, execution_state_write
from ai_dq_agent.tools.validation_tools import range_check, regex_validate, timestamp_compare

__all__ = [
    # Validation (3)
    "regex_validate",
    "range_check",
    "timestamp_compare",
    # DynamoDB (4)
    "dynamodb_export_to_s3",
    "dynamodb_stream_read",
    "dynamodb_scan_with_rate_limit",
    "dynamodb_batch_write",
    # S3 (2)
    "s3_read_objects",
    "s3_write_objects",
    # Address (1)
    "address_normalize",
    # LLM (1)
    "llm_batch_analyze",
    # Cache (2)
    "judgment_cache_read",
    "judgment_cache_write",
    # Report (1)
    "report_generate",
    # Slack (3)
    "slack_send_message",
    "slack_send_interactive_message",
    "slack_receive_response",
    # State (2)
    "execution_state_read",
    "execution_state_write",
    # Correction (3)
    "snapshot_save",
    "correction_log_write",
    "feedback_log_write",
    # Profile (3) — NEW v2
    "profile_compute",
    "profile_history_read",
    "profile_history_write",
    # Lineage (2) — NEW v2
    "lineage_read",
    "impact_score_compute",
    # Root Cause (1) — NEW v2
    "root_cause_trace",
    # Quarantine (1) — NEW v2
    "quarantine_write",
    # Pipeline State (2) — NEW v2
    "pipeline_state_read",
    "pipeline_state_write",
    # Rule Generate (2) — NEW v2
    "rule_generate",
    "rule_registry_update",
    # Delegation (1) — NEW v2
    "delegate_to_agent",
]
