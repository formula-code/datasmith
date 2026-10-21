"""Writes /workspace/asv.conf.json with absolute paths, so `asv run --config=/workspace/asv.conf.json` works from any folder."""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lsv_init import _load_jsonc, find_asv_config  # noqa: E402

conf = find_asv_config()
cfg = _load_jsonc(conf)
defaults = {"repo": None, "benchmark_dir": "benchmarks", "env_dir": "env", "results_dir": "results", "html_dir": "html"}
for key, default in defaults.items():
    value = cfg.get(key) or default
    if value and "://" not in value:
        cfg[key] = os.path.normpath(os.path.join(conf.parent, value))
with open("/workspace/asv.conf.json", "w") as f:
    json.dump(cfg, f, indent=2)
