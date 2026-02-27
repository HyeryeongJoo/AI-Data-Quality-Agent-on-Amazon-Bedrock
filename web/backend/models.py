"""Pydantic models for the DQ Agent Web API."""

import os

from pydantic import BaseModel


class RunValidationRequest(BaseModel):
    s3_data_path: str = f"s3://{os.environ.get('S3_STAGING_BUCKET', 'my-dq-staging-bucket')}/{os.environ.get('S3_SAMPLE_KEY', 'sample/data.jsonl')}"
    dry_run: bool = False
