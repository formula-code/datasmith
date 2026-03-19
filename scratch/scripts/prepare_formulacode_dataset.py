"""Enrich the FormulaCode master parquet and optionally upload to Hugging Face.

Derives container_name, queries Docker Hub for available images, normalizes
difficulty/classification, generates task IDs, filters, and sorts — so that
downstream consumers (terminal-bench, HF datasets) get a ready-to-use dataset.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import pandas as pd
import requests

from datasmith.logging_config import configure_logging

logger = configure_logging()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enrich a FormulaCode commits parquet for terminal-bench.",
    )
    p.add_argument("--input", type=Path, required=True, help="Raw parquet file.")
    p.add_argument("--output", type=Path, required=True, help="Output enriched parquet.")
    p.add_argument(
        "--dockerhub-repository",
        default="formulacode/all",
        help="Docker Hub repository (namespace/repo).",
    )
    p.add_argument(
        "--filter-by",
        type=Path,
        default=None,
        help="JSON file with (repo_name, sha) keys to keep.",
    )
    p.add_argument(
        "--limit-per-repo",
        type=int,
        default=-1,
        help="Max tasks per repo family (-1 = no limit).",
    )
    p.add_argument(
        "--upload-to-hf",
        type=str,
        default=None,
        metavar="REPO_ID",
        help=(
            "Upload the output parquet to a Hugging Face dataset repo "
            "(e.g. 'formulacode/formulacode-all'). "
            "Requires HF_TOKEN in tokens.env or environment. "
            "Uploads monthly configs (e.g. '2024.01') plus a 'default' "
            "config containing all data."
        ),
    )
    p.add_argument(
        "--hf-verified-filter",
        type=Path,
        default=None,
        help=(
            "Path to a JSON filter file (same format as --filter-by). "
            "When combined with --upload-to-hf, adds a 'verified' config."
        ),
    )
    p.add_argument(
        "--hf-commit-message",
        type=str,
        default=None,
        help="Commit message for the HF upload (default: auto-generated).",
    )
    p.add_argument(
        "--hf-date-column",
        type=str,
        default="pr_merged_at",
        help="Column to derive monthly splits from (default: pr_merged_at).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing output.",
    )
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Load / validate
# ---------------------------------------------------------------------------


def load_and_validate(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported format: {path.suffix}")

    required = {"repo_name", "pr_base_sha", "pr_merge_commit_sha"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


# ---------------------------------------------------------------------------
# Derive container_name  (ported from dataset.py:38-43)
# ---------------------------------------------------------------------------


def derive_container_name(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["container_name"] = (df["repo_name"].str.replace("/", "-") + "-" + df["pr_base_sha"] + ":final").str.lower()
    return df


# ---------------------------------------------------------------------------
# Normalize columns
# ---------------------------------------------------------------------------

_DIFFICULTY_LABELS: tuple[str, ...] = ("easy", "medium", "hard", "unknown")


def _levenshtein_distance(source: str, target: str) -> int:
    if source == target:
        return 0
    if not source:
        return len(target)
    if not target:
        return len(source)

    previous = list(range(len(target) + 1))
    for i, s_char in enumerate(source, start=1):
        current = [i]
        for j, t_char in enumerate(target, start=1):
            insert_cost = current[j - 1] + 1
            delete_cost = previous[j] + 1
            replace_cost = previous[j - 1] + (s_char != t_char)
            current.append(min(insert_cost, delete_cost, replace_cost))
        previous = current
    return previous[-1]


def normalize_difficulty(raw: str | None) -> str:
    """Normalize to one of easy/medium/hard/unknown with fuzzy matching."""
    value = (raw or "").strip()
    # Strip enum-style prefix like ``DifficultyLevel.``
    value = value.split(".")[-1].strip().lower()
    if not value:
        return "unknown"
    if value in _DIFFICULTY_LABELS:
        return value

    best_label = _DIFFICULTY_LABELS[0]
    best_distance = _levenshtein_distance(value, best_label)
    for label in _DIFFICULTY_LABELS[1:]:
        distance = _levenshtein_distance(value, label)
        if distance < best_distance:
            best_distance = distance
            best_label = label
    return best_label


def normalize_classification(raw: str | None) -> str:
    """Normalize classification: strip OptimizationType. prefix, lowercase."""
    value = (raw or "").strip()
    value = value.split(".")[-1].strip().lower()
    if not value:
        return "uncategorized"
    # Keep snake_case as-is
    return value


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "difficulty" in df.columns:
        df["difficulty"] = df["difficulty"].apply(normalize_difficulty)
    else:
        df["difficulty"] = "unknown"

    if "classification" in df.columns:
        df["classification"] = df["classification"].apply(normalize_classification)
    else:
        df["classification"] = "uncategorized"
    return df


# ---------------------------------------------------------------------------
# Sort by date  (ported from dataset.py:46-49)
# ---------------------------------------------------------------------------


def sort_by_date(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "date" not in df.columns and "pr_merged_at" in df.columns:
        df["date"] = df["pr_merged_at"]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(by="date", ascending=True)
    return df


# ---------------------------------------------------------------------------
# Filter-by keys  (ported from dataset.py:52-63)
# ---------------------------------------------------------------------------


def load_filter_keys(filter_path: Path) -> list[tuple[str, str]]:
    filter_dict = json.loads(filter_path.read_text())
    return list(map(eval, filter_dict.keys()))


def apply_filter_by(df: pd.DataFrame, valid_keys: list[tuple[str, str]]) -> pd.DataFrame:
    mask = df[["repo_name", "pr_merge_commit_sha"]].apply(tuple, axis=1).isin(valid_keys)
    return df[mask]


# ---------------------------------------------------------------------------
# Docker Hub image resolution  (ported from dockerhub.py)
# ---------------------------------------------------------------------------


def _fetch_dockerhub_api(url: str, timeout: int = 10, max_retries: int = 3) -> dict:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                if attempt < max_retries - 1:
                    logger.warning("Rate limited. Waiting %ds before retry...", retry_after)
                    time.sleep(retry_after)
                    continue
                msg = f"Docker Hub rate limit exceeded. Retry after {retry_after}s"
                raise RuntimeError(msg)  # noqa: TRY301
            if response.status_code == 404:
                msg = f"Docker Hub repository not found: {url}"
                raise ValueError(msg)  # noqa: TRY301
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt < max_retries - 1:
                wait_time = (2**attempt) * 0.5
                logger.warning("Network error (attempt %d/%d): %s", attempt + 1, max_retries, exc)
                time.sleep(wait_time)
            continue
        except ValueError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch Docker Hub API: {exc}") from exc

    raise RuntimeError(f"Failed to fetch Docker Hub API after {max_retries} attempts: {last_error}")


def _get_available_images_from_dockerhub(
    repository: str,
    page_size: int = 100,
) -> set[str]:
    if "/" not in repository:
        raise ValueError(f"Repository must be 'namespace/repo', got: {repository}")

    page_size = min(page_size, 100)
    available: set[str] = set()
    page = 1

    while True:
        url = f"https://hub.docker.com/v2/repositories/{repository}/tags/?page_size={page_size}&page={page}"
        try:
            data = _fetch_dockerhub_api(url)
        except ValueError:
            logger.exception("Repository '%s' not found on Docker Hub", repository)
            raise

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            tag_name = item.get("name")
            if tag_name:
                available.add(f"{repository}:{tag_name}")

        if not data.get("next"):
            break
        page += 1
        time.sleep(0.1)

    logger.info("Found %d tags in %s", len(available), repository)
    return available


def _dockerhub_ref_to_container_name(image_ref: str, repository: str) -> str | None:
    prefix = f"{repository}:"
    if not image_ref.startswith(prefix):
        return None
    tag = image_ref[len(prefix) :]
    if "--" not in tag:
        return None
    base_name, variant = tag.rsplit("--", 1)
    if not base_name or not variant:
        return None
    return f"{base_name}:{variant}"


def resolve_dockerhub_images(repository: str) -> dict[str, str]:
    """Return {container_name: full_dockerhub_ref} for all tags in *repository*."""
    available = _get_available_images_from_dockerhub(repository)
    image_map: dict[str, str] = {}
    for img in available:
        container_name = _dockerhub_ref_to_container_name(img, repository)
        if container_name:
            image_map[container_name] = img
    return image_map


# ---------------------------------------------------------------------------
# Apply image filter  (ported from dataset.py:66-72)
# ---------------------------------------------------------------------------


def apply_image_filter(df: pd.DataFrame, image_map: dict[str, str]) -> pd.DataFrame:
    df = df[df["container_name"].isin(set(image_map.keys()))].copy()
    df["image_name"] = df["container_name"].map(image_map)
    return df


# ---------------------------------------------------------------------------
# Limit per repo  (ported from dataset.py:75-87)
# ---------------------------------------------------------------------------


def limit_per_repo(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if limit <= 0:
        return df
    repo_names = df["container_name"].replace(":pkg", "").replace(":final", "").str.split("-").str[:-1].str.join("-")
    return df.groupby(repo_names).head(limit)


# ---------------------------------------------------------------------------
# Regenerate task IDs  (ported from dataset.py:90-104)
# ---------------------------------------------------------------------------


def regenerate_task_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    counts: dict[str, int] = {}
    for i, row in df.iterrows():
        splits = row["container_name"].replace(":pkg", "").replace(":final", "").split("-")
        ow_splits = splits[:-1]
        owner = "-".join(ow_splits[: len(ow_splits) // 2])
        repo = "-".join(ow_splits[len(ow_splits) // 2 :])
        base_id = f"{owner}_{repo}"
        cnt = counts.get(base_id, 0) + 1
        counts[base_id] = cnt
        df.at[i, "task_id"] = f"{base_id}_{cnt}"
    return df


# ---------------------------------------------------------------------------
# Hugging Face upload
# ---------------------------------------------------------------------------

_DATASMITH_ROOT = Path(__file__).resolve().parents[2]
_DATASET_CARD_TEMPLATE = _DATASMITH_ROOT / "DATASET_CARD.md"

# Columns to include when uploading to Hugging Face.
_HF_COLUMNS: list[str] = [
    "task_id",
    "repo_name",
    "container_name",
    "image_name",
    "difficulty",
    "classification",
    "patch",
    "final_md",
    "pr_merged_at",
    "pr_merge_commit_sha",
    "pr_base_sha",
]


def _build_dataset_card(
    configs: list[str],
    *,
    total_rows: int,
    verified_rows: int | None = None,
    default_config: str = "default",
) -> str:
    """Build a HF dataset card README.md with YAML front matter and body."""
    parquet_name = "train-00000-of-00001.parquet"

    # --- YAML front matter ---
    lines = ["---"]
    lines.append("configs:")
    for cfg in configs:
        lines.append(f'  - config_name: "{cfg}"')
        lines.append("    data_files:")
        lines.append("      - split: train")
        lines.append(f'        path: "{cfg}/{parquet_name}"')
    lines.append(f'default_config_name: "{default_config}"')
    lines.append("task_categories:")
    lines.append("  - text-generation")
    lines.append("tags:")
    lines.append("  - code")
    lines.append("  - performance-optimization")
    lines.append("  - benchmark")
    lines.append("language:")
    lines.append("  - en")
    lines.append("size_categories:")
    lines.append("  - 1K<n<10K")
    lines.append("---")
    lines.append("")

    # --- Dataset card body from template ---
    month_configs = [c for c in configs if c not in ("default", "verified")]

    verified_row = ""
    if verified_rows is not None:
        verified_row = f"| `verified` | Human-validated subset | {verified_rows} |"

    body = _DATASET_CARD_TEMPLATE.read_text()
    body = body.format(
        total_rows=total_rows,
        num_months=len(month_configs),
        verified_row=verified_row,
    )

    lines.append(body)
    return "\n".join(lines)


def upload_to_huggingface(
    df: pd.DataFrame,
    repo_id: str,
    *,
    date_column: str = "pr_merged_at",
    commit_message: str | None = None,
    extra_configs: dict[str, pd.DataFrame] | None = None,
) -> str:
    """Upload a DataFrame to a HF dataset repo with monthly configs.

    Creates one config per YYYY-MM, a ``default`` config with all rows,
    and any additional configs passed via *extra_configs*.  A README.md
    with explicit YAML metadata is generated so that HF correctly
    recognises every config.

    Returns the commit URL.
    """
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq
    from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi

    token = os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError("HF_TOKEN not set. Add it to tokens.env or export it in the environment.")

    api = HfApi(token=token)
    api.create_repo(repo_id, repo_type="dataset", exist_ok=True)

    if commit_message is None:
        commit_message = "Update dataset configs via prepare_formulacode_dataset"

    # Only keep key columns (intersected with what's actually present).
    keep = [c for c in _HF_COLUMNS if c in df.columns]
    df = df[keep].copy()
    if extra_configs:
        extra_configs = {name: edf[[c for c in keep if c in edf.columns]].copy() for name, edf in extra_configs.items()}
    logger.info("Uploading %d columns: %s", len(keep), ", ".join(keep))

    dates = pd.to_datetime(df[date_column], errors="coerce")
    df = df.copy()
    df["_month"] = dates.dt.to_period("M").astype(str)

    # Use the full dataframe's schema so every monthly parquet has
    # consistent column types (avoids null-type columns in small months).
    full_table = pa.Table.from_pandas(df.drop(columns=["_month"]), preserve_index=False)
    schema = full_table.schema

    parquet_name = "train-00000-of-00001.parquet"
    operations: list[CommitOperationAdd | CommitOperationDelete] = []
    config_names: list[str] = []

    # Delete all existing parquet files to avoid stale configs
    try:
        existing_files = api.list_repo_files(repo_id, repo_type="dataset")
        for f in existing_files:
            if f.endswith(".parquet"):
                operations.append(CommitOperationDelete(path_in_repo=f))
    except Exception:  # noqa: S110
        pass  # repo may be empty

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # Monthly configs
        for month, group in sorted(df.groupby("_month")):
            month_dir = tmp_path / month
            month_dir.mkdir()
            out = month_dir / parquet_name
            table = pa.Table.from_pandas(group.drop(columns=["_month"]), preserve_index=False).cast(schema)
            pq.write_table(table, out)
            operations.append(
                CommitOperationAdd(
                    path_in_repo=f"{month}/{parquet_name}",
                    path_or_fileobj=str(out),
                )
            )
            config_names.append(month)
            logger.info("  config %s: %d rows", month, len(group))

        # Default config (all data)
        default_dir = tmp_path / "default"
        default_dir.mkdir()
        out = default_dir / parquet_name
        pq.write_table(full_table, out)
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"default/{parquet_name}",
                path_or_fileobj=str(out),
            )
        )
        config_names.append("default")
        logger.info("  config default: %d rows (all)", len(df))

        # Extra configs (e.g. "verified")
        if extra_configs:
            for name, extra_df in extra_configs.items():
                cfg_dir = tmp_path / name
                cfg_dir.mkdir()
                out = cfg_dir / parquet_name
                table = pa.Table.from_pandas(extra_df, preserve_index=False).cast(schema)
                pq.write_table(table, out)
                operations.append(
                    CommitOperationAdd(
                        path_in_repo=f"{name}/{parquet_name}",
                        path_or_fileobj=str(out),
                    )
                )
                config_names.append(name)
                logger.info("  config %s: %d rows", name, len(extra_df))

        # README dataset card
        verified_rows = len(extra_configs["verified"]) if extra_configs and "verified" in extra_configs else None
        readme = _build_dataset_card(
            config_names,
            total_rows=len(df),
            verified_rows=verified_rows,
        )
        readme_path = tmp_path / "README.md"
        readme_path.write_text(readme)
        operations.append(
            CommitOperationAdd(
                path_in_repo="README.md",
                path_or_fileobj=str(readme_path),
            )
        )

        info = api.create_commit(
            repo_id=repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=commit_message,
        )

    return info.commit_url


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    logger.info("Loading %s ...", args.input)
    df = load_and_validate(args.input)
    logger.info("Loaded %d rows", len(df))

    df = derive_container_name(df)
    df = normalize_columns(df)
    df = sort_by_date(df)

    if args.filter_by:
        keys = load_filter_keys(args.filter_by)
        logger.info("Filtering by %d keys from %s", len(keys), args.filter_by)
        df = apply_filter_by(df, keys)
        logger.info("After filter-by: %d rows", len(df))

    logger.info("Resolving Docker Hub images from %s ...", args.dockerhub_repository)
    image_map = resolve_dockerhub_images(args.dockerhub_repository)
    logger.info("Found %d container_name -> image mappings", len(image_map))

    before = len(df)
    df = apply_image_filter(df, image_map)
    logger.info(
        "Image filter: kept %d / %d rows (%d removed)",
        len(df),
        before,
        before - len(df),
    )

    if args.limit_per_repo > 0:
        df = limit_per_repo(df, args.limit_per_repo)
        logger.info("After limit-per-repo(%d): %d rows", args.limit_per_repo, len(df))

    df = regenerate_task_ids(df)

    logger.info("Final dataset: %d rows", len(df))
    logger.info(
        "Columns: %s",
        ", ".join(
            c for c in ["container_name", "image_name", "task_id", "difficulty", "classification"] if c in df.columns
        ),
    )

    if args.dry_run:
        logger.info("[DRY RUN] Would write to %s", args.output)
        print(df[["task_id", "container_name", "image_name", "difficulty", "classification"]].head(20).to_string())
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    logger.info("Wrote enriched parquet to %s", args.output)

    if args.upload_to_hf:
        extra_configs: dict[str, pd.DataFrame] | None = None
        if args.hf_verified_filter and args.hf_verified_filter.exists():
            vkeys = load_filter_keys(args.hf_verified_filter)
            df_verified = apply_filter_by(df, vkeys)
            logger.info("Verified config: %d rows from %s", len(df_verified), args.hf_verified_filter)
            extra_configs = {"verified": df_verified}

        logger.info("Uploading to HF repo %s (monthly configs) ...", args.upload_to_hf)
        url = upload_to_huggingface(
            df,
            args.upload_to_hf,
            date_column=args.hf_date_column,
            commit_message=args.hf_commit_message,
            extra_configs=extra_configs,
        )
        logger.info("Upload complete: %s", url)


if __name__ == "__main__":
    main()
