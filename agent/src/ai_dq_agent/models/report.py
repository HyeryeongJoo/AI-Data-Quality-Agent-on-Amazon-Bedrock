"""Report models."""

from datetime import datetime

from pydantic import BaseModel


class ReportSummary(BaseModel):
    """Summary statistics for a DQ report."""

    total_scanned: int
    total_suspects: int
    total_errors: int
    high_confidence_errors: int
    correction_proposals: int
    error_type_distribution: dict[str, int]
    filtering_ratio: float


class DQReport(BaseModel):
    """Final data quality report."""

    pipeline_id: str
    created_at: datetime
    report_s3_path: str
    summary: ReportSummary
