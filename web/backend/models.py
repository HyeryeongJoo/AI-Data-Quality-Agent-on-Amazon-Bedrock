"""Pydantic models for the DQ Agent Web API."""

from pydantic import BaseModel


class RunValidationRequest(BaseModel):
    s3_data_path: str = ""
    dry_run: bool = False
