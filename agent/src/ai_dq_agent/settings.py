"""Application settings using pydantic-settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """AI DQ Agent configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Environment
    env: Literal["dev", "staging", "prod"] = "dev"
    aws_region: str = "us-east-1"

    # DynamoDB Tables
    dynamodb_table_name: str = "delivery-data-dev"
    dynamodb_state_table: str = "dq-agent-state-dev"
    dynamodb_correction_table: str = "dq-agent-corrections-dev"
    dynamodb_cache_table: str = "dq-agent-cache-dev"
    dynamodb_quarantine_table: str = "dq-agent-quarantine-dev"

    # S3 Buckets
    s3_staging_bucket: str = ""
    s3_report_bucket: str = ""
    s3_rules_bucket: str = ""
    s3_rules_key: str = "rules/delivery_rules.yaml"

    # Amazon Bedrock
    bedrock_model_id: str = "global.anthropic.claude-sonnet-4-6"
    agent_model_id: str = "global.anthropic.claude-sonnet-4-6"

    # Slack
    slack_bot_token: str = ""
    slack_channel_id: str = ""

    # Address API
    address_api_key: str = ""
    address_api_url: str = "https://business.juso.go.kr/addrlink/addrLinkApi.do"

    # Processing Limits
    chunk_size: int = Field(default=100_000, ge=1_000, le=1_000_000)
    llm_batch_size: int = Field(default=50, ge=1, le=200)
    llm_max_items: int = Field(default=10_000, ge=100, le=100_000)
    pipeline_timeout_minutes: int = Field(default=30, ge=5, le=1440)
    approval_timeout_hours: int = Field(default=24, ge=1, le=168)
    max_retries: int = Field(default=3, ge=1, le=10)

    @field_validator("slack_bot_token")
    @classmethod
    def validate_slack_token(cls, v: str) -> str:
        if v and not v.startswith(("xoxb-", "xoxp-")):
            raise ValueError("slack_bot_token must start with 'xoxb-' or 'xoxp-'")
        return v


@lru_cache
def get_settings() -> Settings:
    """Return a cached singleton Settings instance."""
    return Settings()
