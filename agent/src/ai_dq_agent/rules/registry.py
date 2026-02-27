"""Rule registry: loads, validates, and queries validation rules."""

import logging
from importlib import resources
from typing import Any

import boto3
import yaml

from ai_dq_agent.models.schema import ErrorType, RuleMapping

logger = logging.getLogger(__name__)


class RuleRegistry:
    """Manages validation rules loaded from YAML."""

    def __init__(self, rules: list[RuleMapping]) -> None:
        self._rules = rules

    @property
    def rules(self) -> list[RuleMapping]:
        return list(self._rules)

    def get_all_enabled(self) -> list[RuleMapping]:
        """Return all enabled rules."""
        return [r for r in self._rules if r.enabled]

    def get_rules_by_error_type(self, error_type: ErrorType) -> list[RuleMapping]:
        """Filter rules by error type."""
        return [r for r in self._rules if r.enabled and r.error_type == error_type]

    def get_rules_by_column(self, column: str) -> list[RuleMapping]:
        """Filter rules whose target_columns contain the given column."""
        return [r for r in self._rules if r.enabled and column in r.target_columns]

    def get_rules_by_tool(self, tool: str) -> list[RuleMapping]:
        """Filter rules by validation tool name."""
        return [r for r in self._rules if r.enabled and r.validation_tool == tool]


def _parse_rules(content: str) -> list[RuleMapping]:
    """Parse YAML content into a list of RuleMapping objects."""
    data = yaml.safe_load(content)
    if not isinstance(data, dict) or "rules" not in data:
        raise ValueError("Invalid rule registry format: missing 'rules' key")
    return [RuleMapping(**rule) for rule in data["rules"]]


def load_from_s3(bucket: str, key: str, region: str | None = None) -> RuleRegistry:
    """Load rules from S3, falling back to default rules on failure."""
    try:
        kwargs: dict[str, Any] = {}
        if region:
            kwargs["region_name"] = region
        s3 = boto3.client("s3", **kwargs)
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        rules = _parse_rules(content)
        logger.info("Loaded %d rules from s3://%s/%s", len(rules), bucket, key)
        return RuleRegistry(rules)
    except Exception:
        logger.warning("Failed to load rules from S3, falling back to defaults", exc_info=True)
        return load_default()


def load_default() -> RuleRegistry:
    """Load the bundled default rules."""
    ref = resources.files("ai_dq_agent.rules").joinpath("default_rules.yaml")
    content = ref.read_text(encoding="utf-8")
    rules = _parse_rules(content)
    logger.info("Loaded %d default rules", len(rules))
    return RuleRegistry(rules)
