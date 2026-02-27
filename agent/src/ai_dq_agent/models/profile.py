"""Data profiling models for historical analysis."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ColumnProfile(BaseModel):
    """Statistical profile for a single column."""

    column_name: str
    timestamp: datetime
    total_count: int
    null_count: int
    null_rate: float
    unique_count: int
    uniqueness_rate: float
    min_value: Any | None = None
    max_value: Any | None = None
    mean_value: float | None = None
    stddev_value: float | None = None
    top_values: list[dict] = Field(default_factory=list)  # [{"value": "X", "count": 100}, ...]


class TableProfile(BaseModel):
    """Aggregate profile for an entire table."""

    table_name: str
    pipeline_id: str
    timestamp: datetime
    total_records: int
    column_profiles: list[ColumnProfile] = Field(default_factory=list)


class ProfileAnomaly(BaseModel):
    """An anomaly detected by comparing current vs historical profiles."""

    column_name: str
    metric: str  # e.g. "null_rate", "unique_count", "mean_value"
    current_value: float
    historical_avg: float
    deviation_pct: float
    severity: Literal["info", "warning", "critical"]
    description: str


class TableHealth(BaseModel):
    """Overall health indicator for a table/dataset."""

    table_name: str
    health_score: float = Field(ge=0.0, le=1.0)  # 0.0 (worst) ~ 1.0 (perfect)
    status: Literal["healthy", "warning", "critical"]
    freshness_ok: bool = True
    completeness_ok: bool = True
    violation_count: int = 0
    critical_violation_count: int = 0
    warning_violation_count: int = 0
    profile_anomaly_count: int = 0
    last_checked: datetime | None = None
