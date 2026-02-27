"""HTTP client for external API calls."""

from functools import lru_cache

import httpx



@lru_cache
def get_http_client() -> httpx.Client:
    """Return a cached httpx Client for Address API calls."""
    return httpx.Client(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_connections=10),
    )
