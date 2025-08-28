import os

import dotenv

from datasmith.agents.config import configure_agent_backends
from datasmith.logging_config import configure_logging

# Configure logging with the centralized configuration
logger = configure_logging()


def setup_environment() -> None:
    # Load environment variables from .env file if it exists
    if os.path.exists("tokens.env"):
        dotenv.load_dotenv("tokens.env")
    else:
        logger.warning("No tokens.env file found. Skipping environment variable setup.")

    # Initialize agent backends
    configure_agent_backends()


setup_environment()
