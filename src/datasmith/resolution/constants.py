"""Constants used in dependency resolution."""

from __future__ import annotations

import os
import re
from pathlib import Path

# Regular expressions
ASV_REGEX = re.compile(r"(^|/)\.?asv[^/]*\.jsonc?$")
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

# File names
PYPROJECT = "pyproject.toml"
SETUP_CFG = "setup.cfg"
SETUP_PY = "setup.py"

# Cache location for SQLite caches
CACHE_LOCATION: str = os.getenv("CACHE_LOCATION", "cache.db")

# Git cache directory
GIT_CACHE_DIR = Path(os.getenv("GIT_CACHE_DIR", str(Path(CACHE_LOCATION).parent / "git"))).expanduser()
GIT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
