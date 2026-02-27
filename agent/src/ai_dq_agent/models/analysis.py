"""LLM analysis result models."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel

from ai_dq_agent.models.schema import ErrorType


class Judgment(BaseModel):
    """Single judgment from LLM semantic analysis."""

    record_id: str
    rule_id: str
    is_error: bool
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    error_type: ErrorType
    evidence: str
    correction_value: Any | None = None
    correction_column: str | None = None
    reflection_match: bool = True
    reflection_note: str = ""
    # v2: impact and root cause fields
    impact_score: float = 0.0
    root_cause: str = ""
    root_cause_table: str = ""
    root_cause_column: str = ""


class AnalysisResult(BaseModel):
    """Metadata summary of semantic analysis results."""

    pipeline_id: str
    judgments_s3_path: str
    total_analyzed: int
    error_count: int
    high_confidence_count: int
    medium_confidence_count: int
    low_confidence_count: int
    reflection_mismatch_count: int
    cache_hit_count: int = 0
    deep_analysis_count: int = 0


class CorrectionRecord(BaseModel):
    """Record of a single data correction."""

    correction_id: str
    pipeline_id: str
    record_id: str
    column_name: str
    original_value: Any
    corrected_value: Any
    correction_type: ErrorType
    confidence: str
    approved_by: str
    approved_at: datetime
    executed_at: datetime
    status: Literal["success", "failed", "rolled_back"]
    error_message: str | None = None
