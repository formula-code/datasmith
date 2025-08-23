import logging
import os

import dspy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_agent_backends() -> None:
    model = os.getenv("DSPY_MODEL_NAME")
    backend_url = os.getenv("DSPY_URL")
    if anthropic_api_key := os.getenv("ANTHROPIC_API_KEY"):
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

    lm = dspy.LM(model=model, api_base=backend_url, api_key=api_key, model_type="chat")
    dspy.configure(lm=lm)
