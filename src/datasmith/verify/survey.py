"""Survey folder layout: task ids, state files, oracle records, task folders and the review file."""

from __future__ import annotations

import csv
import json
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REVIEW_FIELDS = ("task", "decision", "reviewer", "date", "data", "note")
DECISIONS = ("approved", "not_approved", "pending")
# End states of a task that has no usable oracle measurement.
UNMEASURED = ("uncovered", "base_edited", "patch_failed", "no_benchmarks", "image_failed", "error", "too_slow")

_DISPLAY = re.compile(r"([\w.-]+)/([\w.-]+)#(\d+)")


def task_id(display: str) -> str:
    """``owner/repo#N`` -> ``owner__repo__N``."""
    m = _DISPLAY.fullmatch(display.strip())
    if not m:
        raise ValueError(f"not an owner/repo#N id: {display!r}")
    return "__".join(m.groups())


def display_id(task: str) -> str:
    """``owner__repo__N`` -> ``owner/repo#N``."""
    owner, rest = task.split("__", 1)
    repo, num = rest.rsplit("__", 1)
    return f"{owner}/{repo}#{num}"


def repo_of(task: str) -> str:
    return task.split("__")[1]


@dataclass(frozen=True)
class Survey:
    root: Path

    @property
    def review_path(self) -> Path:
        return self.root / "manual_review.csv"

    @property
    def rule_path(self) -> Path:
        return self.root / "threshold_rule.json"

    def state(self, task: str) -> dict[str, Any]:
        path = self.root / "state" / f"{task}.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def save_state(self, task: str, state: dict[str, Any]) -> None:
        path = self.root / "state" / f"{task}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps({**state, "task": task, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        )
        tmp.replace(path)

    def states(self) -> dict[str, dict[str, Any]]:
        return {p.stem: json.loads(p.read_text()) for p in sorted((self.root / "state").glob("*.json"))}

    def records(self) -> dict[str, dict[str, Any]]:
        """Oracle records with at least one measured run, keyed by task."""
        out = {}
        for path in sorted((self.root / "oracle").glob("*.json")):
            try:
                rec = json.loads(path.read_text())
            except json.JSONDecodeError:
                continue
            if any(run.get("log_speedups") for run in rec.get("runs", [])):
                out[rec["task"]] = rec
        return out

    def task_dir(self, task: str) -> Path:
        return self.root / "pinned" / task

    def reviews(self) -> list[dict[str, str]]:
        if not self.review_path.exists():
            return []
        with self.review_path.open() as f:
            return [{k: row.get(k, "") or "" for k in REVIEW_FIELDS} for row in csv.DictReader(f)]

    def write_reviews(self, rows: list[dict[str, str]]) -> None:
        """Write the review file, keeping one timestamped backup of the previous version."""
        if self.review_path.exists():
            shutil.copy2(
                self.review_path, self.review_path.with_name(f"manual_review.csv.bak-{time.strftime('%Y%m%d-%H%M%S')}")
            )
        tmp = self.review_path.with_suffix(".csv.tmp")
        with tmp.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=REVIEW_FIELDS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        tmp.replace(self.review_path)
