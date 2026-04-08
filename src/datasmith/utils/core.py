"""Core configuration, logging, and retry utilities."""

from __future__ import annotations

import functools
import logging
import sys
import time
from typing import Any, Callable, TypeVar

from pydantic_settings import BaseSettings

F = TypeVar("F", bound=Callable[..., Any])


class Settings(BaseSettings):
    """Application settings loaded from environment / tokens.env."""

    supabase_url: str = ""
    supabase_key: str = ""
    gh_tokens: str = ""
    dspy_model: str = ""
    dspy_api_key: str = ""
    dspy_api_base: str = ""
    dspy_max_tokens: int = 16000
    dockerhub_username: str = ""
    dockerhub_token: str = ""
    hf_token_path: str = ""

    model_config = {"env_file": "tokens.env", "env_file_encoding": "utf-8", "extra": "ignore"}


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger under the ``datasmith`` namespace."""
    full_name = f"datasmith.{name}" if name else "datasmith"
    logger = logging.getLogger(full_name)
    if not logging.getLogger("datasmith").handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
        root = logging.getLogger("datasmith")
        root.addHandler(handler)
        root.setLevel(logging.INFO)
    return logger


def with_backoff(max_retries: int = 3, base_delay: float = 1.0) -> Callable[[F], F]:
    """Decorator: retry with exponential backoff on transient failures."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt == max_retries:
                        raise
                    time.sleep(delay)
                    delay *= 2
            raise last_exc  # type: ignore[misc]  # unreachable but satisfies mypy

        return wrapper  # type: ignore[return-value]

    return decorator
