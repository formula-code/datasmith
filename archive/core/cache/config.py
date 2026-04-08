"""Cache configuration defaults."""

from __future__ import annotations

import os

from datasmith import logger

_cache_location = os.getenv("CACHE_LOCATION")
if not _cache_location:
    logger.warning("CACHE_LOCATION environment variable not set. Using default 'cache.db'.")
    _cache_location = "cache.db"

CACHE_LOCATION: str = _cache_location

__all__ = ["CACHE_LOCATION"]
