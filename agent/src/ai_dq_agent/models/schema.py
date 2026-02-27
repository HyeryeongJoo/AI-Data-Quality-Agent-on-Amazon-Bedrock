"""Schema and rule definition models."""

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


class ErrorType(StrEnum):
    """Categories of data quality errors."""

    OUT_OF_RANGE = "out_of_range"
    FORMAT_INCONSISTENCY = "format_inconsistency"
    TEMPORAL_VIOLATION = "temporal_violation"
    CROSS_COLUMN_INCONSISTENCY = "cross_column_inconsistency"


class ColumnDef(BaseModel):
    """Schema definition for a single column."""

    name: str
    data_type: Literal["string", "integer", "float", "boolean", "datetime"]
    nullable: bool = True
    allowed_values: list[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None
    format_pattern: str | None = None
    related_columns: list[str] = Field(default_factory=list)
    description: str = ""


class TemporalRelation(BaseModel):
    """Defines a temporal ordering constraint between two columns."""

    earlier_column: str
    later_column: str
    description: str = ""


class CrossColumnRelation(BaseModel):
    """Defines a semantic relationship between two columns."""

    source_column: str
    target_column: str
    relation_type: str
    description: str = ""


class SchemaInfo(BaseModel):
    """Complete schema information for the validation target."""

    table_name: str
    columns: list[ColumnDef]
    primary_key: list[str]
    temporal_relations: list[TemporalRelation] = Field(default_factory=list)
    cross_column_relations: list[CrossColumnRelation] = Field(default_factory=list)
    sample_count: int = 0


class RuleMapping(BaseModel):
    """Definition of a single validation rule."""

    rule_id: str
    error_type: ErrorType
    description: str
    target_columns: list[str]
    validation_tool: str
    params: dict[str, Any] = Field(default_factory=dict)
    severity: Literal["critical", "warning", "info"] = "warning"
    enabled: bool = True
