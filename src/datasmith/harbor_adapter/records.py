"""Build a FormulaCodeRecord from a datasmith Supabase row dict.

Upstream harbor's ``adapters/formulacode/run_adapter.py:to_record`` expects
a pandas DataFrame row with column names from the FormulaCode HuggingFace
schema. Datasmith instead owns the source of truth directly in Supabase,
so we take a plain ``pull_requests`` row and map it into the shape the
adapter wants. The FK into ``candidate_containers`` is implicit via
``(owner, repo, merge_commit_sha)``.
"""

from __future__ import annotations

from typing import Any

from datasmith.harbor_adapter.adapter import FormulaCodeRecord
from datasmith.harbor_adapter.utils import normalize_difficulty


def to_record(pr: dict[str, Any]) -> FormulaCodeRecord:
    """Convert a pull_requests row into a FormulaCodeRecord.

    Required keys on ``pr``:
        owner, repo, issue_number, merge_commit_sha, base_sha,
        container_name, patch, rendered_problem

    Optional keys: classification, difficulty, merged_at
    """
    owner = pr["owner"]
    repo = pr["repo"]
    issue_number = int(pr["issue_number"])
    return FormulaCodeRecord(
        container_name=str(pr.get("container_name") or ""),
        patch=str(pr.get("patch") or ""),
        owner=owner,
        repo=repo,
        issue_number=issue_number,
        gt_hash=str(pr.get("merge_commit_sha") or ""),
        base_commit=str(pr.get("base_sha") or ""),
        instructions=str(pr.get("rendered_problem") or ""),
        date=(str(pr["merged_at"]) if pr.get("merged_at") else None),
        classification=str(pr.get("classification") or "optimization"),
        difficulty=normalize_difficulty(pr.get("difficulty")),
        repo_name=f"{owner}/{repo}",
    )
