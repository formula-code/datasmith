"""DataSmith — toolchain for building the FormulaCode benchmark dataset."""

from __future__ import annotations

import os

import dotenv

__version__ = "0.1.0"


def setup_environment() -> None:
    """Load environment variables from tokens.env if present."""
    if os.path.exists("tokens.env"):
        dotenv.load_dotenv("tokens.env")


setup_environment()

# Public API — lazy imports to avoid circular dependencies
__all__ = [
    "__version__",
    "setup_environment",
]
