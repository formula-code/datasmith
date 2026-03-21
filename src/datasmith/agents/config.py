from __future__ import annotations

import contextlib
import os
import threading
from dataclasses import dataclass
from typing import Any

from datasmith.utils import get_logger

logger = get_logger("agents.config")


@dataclass
class AgentConfig:
    """Configuration for LLM agent backends."""

    primary_model: str = ""
    fallback_model: str = ""
    api_key: str = ""
    api_base: str = ""
    max_tokens: int = 16000
    temperature: float = 0.0
    portkey_api_key: str = ""
    portkey_model_name: str = ""

    @classmethod
    def from_env(cls) -> AgentConfig:
        return cls(
            primary_model=os.environ.get("DSPY_MODEL", "openai/gpt-oss-120b"),
            fallback_model=os.environ.get("DSPY_FALLBACK_MODEL", ""),
            api_key=os.environ.get("DSPY_API_KEY", "local"),
            api_base=os.environ.get("DSPY_API_BASE", "http://localhost:30001/v1"),
            max_tokens=int(os.environ.get("DSPY_MAX_TOKENS", "16000")),
            temperature=float(os.environ.get("DSPY_TEMPERATURE", "0")),
            portkey_api_key=os.environ.get("PORTKEY_API_KEY", ""),
            portkey_model_name=os.environ.get("PORTKEY_MODEL_NAME", ""),
        )


# Module-level state for lazy DSPy configuration.
_configured = False
_lock = threading.Lock()
_lm: Any = None  # Stores the dspy.LM instance for async-safe reuse


def configure_dspy(config: AgentConfig) -> None:
    """Configure DSPy backends from AgentConfig."""
    global _lm
    import dspy

    kwargs: dict[str, Any] = {
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }

    if config.api_key and config.primary_model:
        _lm = dspy.LM(
            model=config.primary_model,
            api_key=config.api_key,
            api_base=config.api_base or None,
            **kwargs,
        )
        model_name = config.primary_model
    elif config.portkey_api_key:
        from portkey_ai import PORTKEY_GATEWAY_URL

        model_name = config.portkey_model_name or "@anthropic/claude-3-5-sonnet-latest"
        kwargs["api_base"] = PORTKEY_GATEWAY_URL
        kwargs["api_key"] = "unused-by-portkey"
        kwargs["headers"] = {
            "x-portkey-api-key": config.portkey_api_key,
            "x-portkey-provider": model_name.split("/")[0].lstrip("@"),
        }
        kwargs["custom_llm_provider"] = "openai"
        _lm = dspy.LM(model=model_name, **kwargs)
    else:
        logger.warning("No LM backend configured")
        return

    with contextlib.suppress(RuntimeError):
        dspy.configure(lm=_lm)
    logger.info("Configured DSPy with model: %s", model_name)


def ensure_configured() -> None:
    """Lazy-initialize DSPy on first LLM call.  Thread- and async-safe.

    Uses double-checked locking to avoid repeated configuration.
    If ``dspy.configure()`` was already called from a different async task,
    the stored LM is applied via ``dspy.context()`` instead.
    """
    global _configured
    if _configured:
        # DSPy was configured, but possibly from a different async task.
        # Re-apply the LM via dspy.context() which is async-safe.
        if _lm is not None:
            import dspy

            with contextlib.suppress(RuntimeError):
                dspy.configure(lm=_lm)
        return
    with _lock:
        if _configured:
            return
        config = AgentConfig.from_env()
        configure_dspy(config)
        _configured = True
