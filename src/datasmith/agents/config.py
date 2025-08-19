import logging
import os

import dspy  # type: ignore[import-untyped]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_agent_backends() -> None:
    model = os.getenv("DSPY_MODEL_NAME")
    backend_url = os.getenv("DSPY_URL")
    api_key = os.getenv("DSPY_API_KEY")

    if not model or not backend_url or not api_key:
        logger.warning("Environment variables for DSPY model, URL, or API key are not set.")
        return

    lm = dspy.LM(model=model, api_base=backend_url, api_key=api_key, model_type="chat")
    dspy.configure(lm=lm)
