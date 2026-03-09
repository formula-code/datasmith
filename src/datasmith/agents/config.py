from __future__ import annotations

import os
from dataclasses import dataclass

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
    portkey_api_key: str = ""

    @classmethod
    def from_env(cls) -> AgentConfig:
        return cls(
            primary_model=os.environ.get("DSPY_MODEL", ""),
            fallback_model=os.environ.get("DSPY_FALLBACK_MODEL", ""),
            api_key=os.environ.get("DSPY_API_KEY", ""),
            api_base=os.environ.get("DSPY_API_BASE", ""),
            max_tokens=int(os.environ.get("DSPY_MAX_TOKENS", "16000")),
            portkey_api_key=os.environ.get("PORTKEY_API_KEY", ""),
        )


def configure_dspy(config: AgentConfig) -> None:
    """Configure DSPy backends from AgentConfig."""
    import dspy

    if config.api_key and config.primary_model:
        lm = dspy.LM(
            model=config.primary_model,
            api_key=config.api_key,
            api_base=config.api_base or None,
            max_tokens=config.max_tokens,
        )
        dspy.configure(lm=lm)
        logger.info("Configured DSPy with model: %s", config.primary_model)
