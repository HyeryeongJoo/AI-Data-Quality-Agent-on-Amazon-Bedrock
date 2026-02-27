"""Slack notification and interactive message tools."""

import logging
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from strands import tool

from ai_dq_agent.settings import get_settings
from ai_dq_agent.tools.utils import retry_with_backoff

logger = logging.getLogger(__name__)


def _get_slack_client() -> WebClient:
    """Create a Slack WebClient from settings."""
    settings = get_settings()
    return WebClient(token=settings.slack_bot_token)


# ---------------------------------------------------------------------------
# @tool: slack_send_message
# ---------------------------------------------------------------------------

@tool
def slack_send_message(
    channel: str,
    message: str,
    thread_ts: str | None = None,
) -> dict:
    """Send a text message to a Slack channel.

    Args:
        channel: Slack channel ID.
        message: Text message to send.
        thread_ts: Optional thread timestamp for threaded replies.

    Returns:
        Dict with ok, ts, channel, and error fields.
    """
    settings = get_settings()
    if not settings.slack_bot_token or not channel:
        logger.info("[slack_send_message] skipped: no token or channel configured")
        return {"status": "skipped", "ok": False, "error": "slack not configured"}

    start = time.monotonic()
    logger.info("[slack_send_message] started: channel=%s", channel)

    try:
        client = _get_slack_client()

        def _send():
            params = {"channel": channel, "text": message}
            if thread_ts:
                params["thread_ts"] = thread_ts
            return client.chat_postMessage(**params)

        response = retry_with_backoff(
            _send,
            max_retries=3,
            base_delay=5.0,
            retryable_exceptions=(SlackApiError,),
        )

        duration = time.monotonic() - start
        logger.info("[slack_send_message] completed in %.2fs", duration)

        return {
            "status": "success",
            "ok": True,
            "ts": response.get("ts", ""),
            "channel": response.get("channel", channel),
        }
    except SlackApiError as e:
        duration = time.monotonic() - start
        logger.error("[slack_send_message] failed: %s in %.2fs", e, duration)
        return {"status": "error", "ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# @tool: slack_send_interactive_message
# ---------------------------------------------------------------------------

@tool
def slack_send_interactive_message(
    channel: str,
    pipeline_id: str,
    report_summary: str,
    report_link: str,
    correction_count: int,
    thread_ts: str | None = None,
) -> dict:
    """Send an interactive Slack message with approval/review/reject buttons.

    Args:
        channel: Slack channel ID.
        pipeline_id: Pipeline execution ID for context.
        report_summary: Summary text of the DQ report.
        report_link: URL or S3 path to the full report.
        correction_count: Number of items proposed for correction.
        thread_ts: Optional thread timestamp for threading.

    Returns:
        Dict with ok, ts, message_id fields.
    """
    start = time.monotonic()
    logger.info("[slack_send_interactive_message] started: pipeline=%s", pipeline_id)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"DQ Report: {pipeline_id}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": report_summary,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Report*: `{report_link}`\n*Correction proposals*: {correction_count} items",
            },
        },
        {
            "type": "actions",
            "block_id": f"dq_approval_{pipeline_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Approve All"},
                    "style": "primary",
                    "action_id": "approved_all",
                    "value": pipeline_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Review Individually"},
                    "action_id": "approved_partial",
                    "value": pipeline_id,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Reject All"},
                    "style": "danger",
                    "action_id": "rejected",
                    "value": pipeline_id,
                },
            ],
        },
    ]

    settings = get_settings()
    if not settings.slack_bot_token or not channel:
        logger.info("[slack_send_interactive_message] skipped: no token or channel configured")
        return {"status": "skipped", "ok": False, "error": "slack not configured"}

    try:
        client = _get_slack_client()

        def _send():
            params = {
                "channel": channel,
                "text": f"DQ Report ready for review: {pipeline_id}",
                "blocks": blocks,
            }
            if thread_ts:
                params["thread_ts"] = thread_ts
            return client.chat_postMessage(**params)

        response = retry_with_backoff(
            _send,
            max_retries=3,
            base_delay=5.0,
            retryable_exceptions=(SlackApiError,),
        )

        duration = time.monotonic() - start
        logger.info("[slack_send_interactive_message] completed in %.2fs", duration)

        return {
            "status": "success",
            "ok": True,
            "ts": response.get("ts", ""),
            "message_id": f"{channel}_{response.get('ts', '')}",
        }
    except SlackApiError as e:
        duration = time.monotonic() - start
        logger.error("[slack_send_interactive_message] failed: %s in %.2fs", e, duration)
        return {"status": "error", "ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# @tool: slack_receive_response
# ---------------------------------------------------------------------------

@tool
def slack_receive_response(
    pipeline_id: str,
    approval_response: dict | None = None,
) -> dict:
    """Retrieve Slack interactive response from Graph state.

    This tool works with the Graph interrupt/resume pattern.
    The actual Slack callback is handled externally; this tool
    reads the response data passed via Graph state on resume.

    Args:
        pipeline_id: Pipeline execution ID.
        approval_response: Response data injected by Graph resume.
            Expected keys: action_value, user_id, user_name, responded_at.

    Returns:
        Dict with response details or timeout status.
    """
    logger.info("[slack_receive_response] called: pipeline=%s", pipeline_id)

    if approval_response is None:
        return {
            "status": "waiting",
            "pipeline_id": pipeline_id,
            "message": "Awaiting Slack response (Graph interrupt)",
        }

    return {
        "status": "received",
        "pipeline_id": pipeline_id,
        "action_value": approval_response.get("action_value", ""),
        "user_id": approval_response.get("user_id", ""),
        "user_name": approval_response.get("user_name", ""),
        "responded_at": approval_response.get("responded_at", ""),
        "rejection_reason": approval_response.get("rejection_reason"),
    }
