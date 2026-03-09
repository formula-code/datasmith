from __future__ import annotations

import os
import tempfile
from pathlib import Path

from datasmith.github.models import FormulaCodeRecord
from datasmith.publish.records import records_to_parquet
from datasmith.utils import get_logger

logger = get_logger("publish.huggingface")


class HuggingFacePublisher:
    """Publish FormulaCode records to HuggingFace as versioned Parquet datasets."""

    def __init__(
        self,
        repo_id: str = "formulacode/formulacode",
        token_path: str = "",
    ) -> None:
        self._repo_id = repo_id
        self._token_path = token_path or os.environ.get("HF_TOKEN_PATH", "/mnt/sdd3/llama_atharvas/huggingface/token")
        self._token: str | None = None

    def _get_token(self) -> str:
        if self._token is None:
            path = Path(self._token_path)
            if path.exists():
                self._token = path.read_text().strip()
            else:
                self._token = os.environ.get("HF_TOKEN", "")
            if not self._token:
                raise ValueError(f"HuggingFace token not found at {self._token_path} or HF_TOKEN env var")
        return self._token

    def publish(self, records: list[FormulaCodeRecord], version: str) -> None:
        """Upload records as Parquet to HuggingFace Hub."""
        from huggingface_hub import HfApi

        token = self._get_token()
        parquet_bytes = records_to_parquet(records)
        if not parquet_bytes:
            logger.warning("No records to publish")
            return

        api = HfApi(token=token)

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, f"{version}.parquet")
            with open(parquet_path, "wb") as f:
                f.write(parquet_bytes)

            api.upload_file(
                path_or_fileobj=parquet_path,
                path_in_repo=f"data/{version}.parquet",
                repo_id=self._repo_id,
                repo_type="dataset",
                commit_message=f"Add {version} data ({len(records)} records)",
            )

        logger.info("Published %d records as %s to %s", len(records), version, self._repo_id)

    def create_dataset_card(self, version: str) -> str:
        """Generate a YAML dataset card string."""
        card = f"""---
language:
- en
license: apache-2.0
tags:
- code
- benchmarks
- performance
- optimization
pretty_name: FormulaCode
size_categories:
- 1K<n<10K
---

# FormulaCode Dataset

Performance optimization benchmark dataset.

## Version: {version}

## Schema

| Field | Type | Description |
|-------|------|-------------|
| task_id | string | Unique task identifier (owner__repo-issue_number) |
| owner | string | Repository owner |
| repo | string | Repository name |
| issue_number | int | PR number |
| gt_hash | string | Ground truth merge commit SHA |
| base_commit | string | Base commit SHA |
| date | string | Merge date |
| instructions | string | Task instructions / problem statement |
| classification | string | Optimization type category |
| difficulty | string | easy / medium / hard |
| container_name | string | Docker container name |
| patch | string | Ground truth patch |
| image_name | string | Docker image name |
"""
        return card
