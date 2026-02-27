"""AWS client factory with singleton management."""

from functools import lru_cache

import boto3
from botocore.config import Config

from ai_dq_agent.settings import get_settings

_boto3_config = Config(
    retries={"mode": "standard", "max_attempts": 3},
    connect_timeout=5,
    read_timeout=30,
)

_bedrock_config = Config(
    retries={"mode": "standard", "max_attempts": 3},
    connect_timeout=10,
    read_timeout=300,
)


@lru_cache
def get_dynamodb_client():
    """Return a cached DynamoDB client."""
    settings = get_settings()
    return boto3.client("dynamodb", region_name=settings.aws_region, config=_boto3_config)


@lru_cache
def get_dynamodb_resource():
    """Return a cached DynamoDB resource for high-level Table API."""
    settings = get_settings()
    return boto3.resource("dynamodb", region_name=settings.aws_region, config=_boto3_config)


@lru_cache
def get_s3_client():
    """Return a cached S3 client."""
    settings = get_settings()
    return boto3.client("s3", region_name=settings.aws_region, config=_boto3_config)


@lru_cache
def get_bedrock_client():
    """Return a cached Bedrock Runtime client."""
    settings = get_settings()
    return boto3.client("bedrock-runtime", region_name=settings.aws_region, config=_bedrock_config)


def get_bedrock_boto_config() -> Config:
    """Return the botocore Config for Bedrock clients (used by Strands BedrockModel)."""
    return _bedrock_config
