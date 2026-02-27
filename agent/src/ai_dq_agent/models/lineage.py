"""Table lineage models for impact analysis and root cause tracing."""

from datetime import datetime

from pydantic import BaseModel, Field


class TableLineage(BaseModel):
    """Lineage information for a table — upstream/downstream dependencies."""

    table_name: str
    upstream_tables: list[str] = Field(default_factory=list)
    downstream_tables: list[str] = Field(default_factory=list)
    query_volume_7d: int = 0
    certification_status: str = "uncertified"  # "certified" | "uncertified" | "deprecated"
    last_updated: datetime | None = None
    column_lineage: dict[str, str] = Field(default_factory=dict)  # {col: "upstream_table.col"}


class ImpactScore(BaseModel):
    """Impact score for a single violation."""

    record_id: str
    rule_id: str
    impact_score: float
    severity_weight: float
    downstream_table_count: int
    query_volume_weight: float
    description: str = ""


class RootCause(BaseModel):
    """Root cause analysis result for a violation."""

    violation_rule_id: str
    violation_column: str
    root_cause_table: str
    root_cause_column: str
    root_cause_description: str
    confidence: str  # HIGH / MEDIUM / LOW
    profile_change_detected: bool = False
    upstream_pipeline_name: str = ""
