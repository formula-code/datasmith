import os

from datasmith.logging_config import configure_logging

# Configure logging with the centralized configuration
logger = configure_logging()


def setup_environment() -> None:
    if os.path.exists("tokens.env"):
        with open("tokens.env", encoding="utf-8") as f:
            lines = f.readlines()
            tokens = {line.split("=")[0].strip(): line.split("=")[1].strip() for line in lines if "=" in line}
        os.environ.update(tokens)
    else:
        logger.warning("No tokens.env file found. Skipping environment variable setup.")


setup_environment()
