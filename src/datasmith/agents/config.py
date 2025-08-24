import logging
import os

import dspy
from portkey_ai import PORTKEY_GATEWAY_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_agent_backends() -> None:
    model = os.getenv("DSPY_MODEL_NAME")
    backend_url = os.getenv("DSPY_URL")
    kwargs: dict[str, str | dict[str, str]] = {"model_type": "chat"}
    if portkey_api_key := os.getenv("PORTKEY_API_KEY"):
        api_key = "unused-by-portkey"
        model = os.getenv("PORTKEY_MODEL_NAME", "@anthropic/claude-3-5-sonnet-latest")
        backend_url = PORTKEY_GATEWAY_URL
        kwargs["headers"] = {"x-portkey-api-key": portkey_api_key}
        kwargs["custom_llm_provider"] = "openai"
    elif anthropic_api_key := os.getenv("ANTHROPIC_API_KEY"):
        api_key = anthropic_api_key
        model = os.getenv("ANTHROPIC_MODEL_NAME", "anthropic/claude-3-opus-20240229")
        backend_url = None
    elif vllm_api_key := os.getenv("DSPY_API_KEY"):
        api_key = vllm_api_key
    else:
        logger.warning("NO API KEY SET")
        return

    if not model or not api_key:
        logger.warning("Environment variables for DSPY model or API key are not set.")
        return

    lm = dspy.LM(model=model, api_base=backend_url, api_key=api_key, **kwargs)  # pyright: ignore[reportArgumentType]
    dspy.configure(lm=lm)
