"""Validation result models."""

from typing import Any, Literal

from pydantic import BaseModel

from ai_dq_agent.models.schema import ErrorType


class SuspectItem(BaseModel):
    """A single suspect item identified by rule-based validation."""

    record_id: str
    rule_id: str
    error_type: ErrorType
    target_columns: list[str]
    current_values: dict[str, Any]
    reason: str
    severity: Literal["critical", "warning", "info"]


class ValidationResult(BaseModel):
    """Metadata summary of rule-based validation results."""

    pipeline_id: str
    total_scanned: int
    suspect_count: int
    suspects_s3_path: str
    stats_by_error_type: dict[str, int]
    stats_by_severity: dict[str, int]
    processing_time_seconds: float
    chunk_count: int
