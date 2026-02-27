"""Pipeline execution state models."""

from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class StageStatus(BaseModel):
    """Execution status of a single pipeline stage."""

    stage_name: str
    status: Literal["pending", "running", "completed", "failed", "skipped"] = "pending"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    records_processed: int = 0
    error_message: str | None = None


class PipelineState(BaseModel):
    """Tracks the overall state of a pipeline execution session."""

    pipeline_id: str
    trigger_type: Literal["schedule", "event"]
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    status: Literal["running", "completed", "failed", "interrupted"] = "running"
    current_stage: str = ""
    s3_staging_prefix: str = ""
    total_records: int = 0
    checkpoint_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: str | None = None
    stages: list[StageStatus] = Field(default_factory=list)


def generate_pipeline_id(trigger_type: str) -> str:
    """Generate a unique pipeline ID.

    Format: dq-{trigger_type}-{YYYYMMDD-HHMMSS}-{8-char-hex}
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    short_id = uuid4().hex[:8]
    return f"dq-{trigger_type}-{ts}-{short_id}"
