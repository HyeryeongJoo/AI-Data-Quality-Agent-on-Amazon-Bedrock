"""Pydantic models for the DQ Agent Web API."""

from typing import Literal

from pydantic import BaseModel

DEFAULT_ANOMALY_METHODS = ["zscore", "iqr", "isolation_forest", "conditional", "correlation", "rare_combination"]


class RunValidationRequest(BaseModel):
    s3_data_path: str = "s3://dq-agent-staging-dev-joohyery/sample/data.jsonl"
    dry_run: bool = False
    pipeline_version: Literal["v1", "v2"] = "v1"
    anomaly_methods: list[str] | None = None
