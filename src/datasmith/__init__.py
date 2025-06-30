import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_environment() -> None:
    if os.path.exists("tokens.env"):
        with open("tokens.env", encoding="utf-8") as f:
            lines = f.readlines()
            tokens = {line.split("=")[0].strip(): line.split("=")[1].strip() for line in lines if "=" in line}
        os.environ.update(tokens)
    else:
        logger.warning("No tokens.env file found. Skipping environment variable setup.")


setup_environment()
