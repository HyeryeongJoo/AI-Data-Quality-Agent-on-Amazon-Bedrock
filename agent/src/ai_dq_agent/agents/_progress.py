"""S3 progress writer — records per-stage progress for SSE streaming.

Writes a _progress.json file to the S3 staging prefix so the backend
SSE endpoint can poll it and relay updates to the browser.
"""

import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)

_s3 = None


def _get_s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def write_progress(state: dict, stage_name: str, status: str) -> None:
    """Write stage progress to S3.

    Args:
        state: Graph state dict (must contain pipeline_id; may contain s3_staging_prefix).
        stage_name: Name of the current stage (e.g. "coordinator").
        status: One of "running", "completed", "failed".
    """
    try:
        pipeline_id = state.get("pipeline_id")
        if not pipeline_id:
            return

        # Resolve bucket and key
        s3_staging_prefix = state.get("s3_staging_prefix", "")
        if s3_staging_prefix.startswith("s3://"):
            # e.g. s3://bucket/staging/PIPE-xxx/
            parts = s3_staging_prefix[5:].split("/", 1)
            bucket = parts[0]
            prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
        else:
            # Fall back to settings
            from ai_dq_agent.settings import get_settings
            settings = get_settings()
            bucket = settings.s3_staging_bucket
            prefix = f"staging/{pipeline_id}"

        key = f"{prefix}/_progress.json"
        now = datetime.now(timezone.utc).isoformat()

        # Read existing progress (merge, don't overwrite)
        s3 = _get_s3_client()
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            progress = json.loads(resp["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            progress = {"pipeline_id": pipeline_id, "stages": {}}
        except Exception:
            progress = {"pipeline_id": pipeline_id, "stages": {}}

        # Update the stage entry
        stage_entry = progress.get("stages", {}).get(stage_name, {})
        stage_entry["status"] = status
        if status == "running":
            stage_entry["started_at"] = now
        elif status in ("completed", "failed"):
            stage_entry["completed_at"] = now

        progress.setdefault("stages", {})[stage_name] = stage_entry
        progress["current_stage"] = stage_name
        progress["updated_at"] = now

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(progress).encode("utf-8"),
            ContentType="application/json",
        )

    except Exception:
        # Never let progress tracking break the pipeline
        logger.debug("Failed to write progress for stage %s", stage_name, exc_info=True)
