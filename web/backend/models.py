"""Pydantic models for the DQ Agent Web API."""

from pydantic import BaseModel


class RunValidationRequest(BaseModel):
    s3_data_path: str = "s3://dq-agent-staging-dev-joohyery/sample/data.jsonl"
    dry_run: bool = False
