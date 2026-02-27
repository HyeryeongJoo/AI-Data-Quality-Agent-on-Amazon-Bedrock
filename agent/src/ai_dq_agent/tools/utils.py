"""Shared utility functions for all tool modules."""

import json
import logging
import random
import time

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retryable exception codes per AWS service (LC-04)
# ---------------------------------------------------------------------------

DYNAMODB_RETRYABLE = (
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "InternalServerError",
)

S3_RETRYABLE = (
    "SlowDown",
    "ServiceUnavailable",
    "InternalError",
)

BEDROCK_RETRYABLE = (
    "ThrottlingException",
    "ModelTimeoutException",
    "ServiceUnavailableException",
)


def is_retryable_client_error(error: ClientError, retryable_codes: tuple[str, ...]) -> bool:
    """Check if a botocore ClientError is retryable."""
    return error.response["Error"]["Code"] in retryable_codes


# ---------------------------------------------------------------------------
# Retry with exponential backoff (DP-01)
# ---------------------------------------------------------------------------


def retry_with_backoff(
    func,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: tuple = (),
):
    """Execute *func* with exponential backoff retry on specified exceptions."""
    for attempt in range(max_retries + 1):
        try:
            return func()
        except retryable_exceptions as e:
            if attempt == max_retries:
                raise
            delay = min(base_delay * (2**attempt) + random.uniform(0, 0.5), max_delay)
            logger.warning("Retry %d/%d after %.1fs: %s", attempt + 1, max_retries, delay, e)
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Rate limiter (DP-06)
# ---------------------------------------------------------------------------


class RateLimiter:
    """Simple token-bucket style rate limiter."""

    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._last_call = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def serialize_record_id(record: dict, primary_key: list[str]) -> str:
    """Serialize primary key values of a record to a stable JSON string."""
    pk_values = {k: record.get(k) for k in primary_key}
    return json.dumps(pk_values, sort_keys=True, ensure_ascii=False)


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Parse ``s3://bucket/key/path`` into *(bucket, key)* tuple."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path (must start with s3://): {s3_path}")
    without_scheme = s3_path[5:]
    slash_idx = without_scheme.find("/")
    if slash_idx == -1:
        return without_scheme, ""
    return without_scheme[:slash_idx], without_scheme[slash_idx + 1 :]


def mask_sensitive(value: str, visible: int = 5) -> str:
    """Mask a sensitive string for safe logging."""
    if len(value) <= visible:
        return "****"
    return value[:visible] + "****"
