# Legacy Registry Migration & Verification — Replication Steps

## Overview

This documents the end-to-end process for migrating 77 pre-synthesized Docker build contexts from the legacy `context_registry_final_filtered.json` into the new DataSmith system and running verification on them.

**Source**: `scratch/artifacts/pipeflush/context_registry_final_filtered.json`
**Target**: `dataset/formulacode_verified_new/<owner>_<repo>/<sha>/`

## Prerequisites

- Docker 27+ running locally
- Python 3.10+ with the project's venv (`make install`)
- At least 100GB free disk space (Docker images are large)
- DockerHub credentials in env vars (only if pushing with `--push`)

## Step 1: Fix DockerContext model

The `DockerContext` model in `src/datasmith/docker/context.py` was updated to:
- Add `build_final_sh` field (maps to on-disk `docker_build_final.sh`)
- Fix file names: `from_directory()`/`to_tar_bytes()` use `docker_build_*.sh` (not `build_*.sh`)
- Add `to_directory(path)` method for writing contexts to disk
- Add `from_legacy_dict(data)` classmethod for mapping old registry field names
- Use `ClassVar` dicts (`_FILE_MAP`, `_LEGACY_MAP`) to DRY up mappings

Verify with:
```bash
uv run pytest tests/docker/test_context.py -xvs  # 14 tests pass
```

## Step 2: Migrate legacy registry to task directories

```bash
# Dry run — shows what would be created
python scratch/scripts/migrate_legacy_registry.py --dry-run

# Actual migration — creates ~58 task directories
python scratch/scripts/migrate_legacy_registry.py
```

**Output**: 58 new directories in `dataset/formulacode_verified_new/`, each with:
- `Dockerfile`, `docker_build_base.sh`, `docker_build_env.sh`, `docker_build_pkg.sh`
- `docker_build_run.sh`, `docker_build_final.sh`, `profile.sh`, `run_tests.sh`, `entrypoint.sh`
- `task.txt` (raw Task key string from the registry)

Skips:
- 15 entries already present in `formulacode_verified/` or `formulacode_verified_new/`
- 4 default/invalid entries

## Step 3: Verify tasks

The `dataset/verify.py` script builds Docker images, runs ASV profiling, and executes pytest for each task.

### Single task
```bash
python dataset/verify.py --task dataset/formulacode_verified_new/networkx_networkx/<sha>
```

### All tasks under one repo
```bash
python dataset/verify.py --task dataset/formulacode_verified_new/networkx_networkx
```

### Full batch (all repos)
```bash
# 2 workers recommended (each task uses significant CPU/memory for Docker builds)
python dataset/verify.py --task dataset/formulacode_verified_new --workers 2

# Retry previously failed tasks
python dataset/verify.py --task dataset/formulacode_verified_new --workers 2 --retry-failures

# Also push final images to DockerHub
python dataset/verify.py --task dataset/formulacode_verified_new --workers 2 --push
```

### Verification stages per task

1. **Build** (~2-10 min): `docker buildx build` with target `run` stage
   - Build args: `REPO_URL`, `COMMIT_SHA`, `ENV_PAYLOAD`, `PY_VERSION`
   - Uses the task directory as build context
2. **Profile** (~5-55 min): Runs `/profile.sh` in container (ASV benchmarks)
   - rc=124 (timeout) treated as success
3. **Tests** (~2-20 min): Runs `/run_tests.sh` in container (pytest)
4. **Push** (optional): Builds `final` stage and pushes to DockerHub

### Output per task

- **Success**: `verification_success.json` with `{image, owner, repo, sha}`
- **Failure**: `failure.json` with `{stage, return_code, stdout, stderr}`
- **Aggregate**: `all_verification_successes.jsonl` in the root verified directory

### Iterative fixing

When a task fails, the standard cycle is:
1. Read `failure.json` for the error stage and details
2. Edit `docker_build_pkg.sh` (most common) or `docker_build_run.sh`
3. Delete `failure.json`
4. Rerun `python dataset/verify.py --task <task_dir>`

See `dataset/CLAUDE.md` for detailed fixing guidance.

## Step 4: Seed Synthesizer cache (optional)

Seeds the Supabase `build_attempts` table so the Synthesizer's `_find_similar()` can reuse legacy scripts for future synthesis runs.

```bash
# Dry run
python scratch/scripts/seed_synthesizer_cache.py --dry-run

# Actual seeding (requires Supabase running)
python scratch/scripts/seed_synthesizer_cache.py
```

Inserts ~73 rows with `model="legacy-registry"`, `ok=True`, `issue_number=0` (sentinel).

## Files modified/created

| File | Action |
|------|--------|
| `src/datasmith/docker/context.py` | Modified — added `build_final_sh`, fixed file names, added methods |
| `tests/docker/test_context.py` | Modified — updated file names, added new test classes |
| `dataset/verify.py` | Rewritten — uses new DockerContext + python-on-whales |
| `scratch/scripts/migrate_legacy_registry.py` | Created — migration script |
| `scratch/scripts/seed_synthesizer_cache.py` | Created — Supabase seeding script |

## Timing expectations

| Phase | Duration |
|-------|----------|
| Migration (Step 2) | ~5 seconds |
| Single task verification | 10-80 minutes |
| Full batch (58 tasks, 2 workers) | ~10-15 hours |
| Seeding (Step 4) | ~5 seconds |

## Known failure patterns

| Pattern | Repos affected | Fix |
|---------|---------------|-----|
| Missing `libGL.so.1` | dipy | Add `apt-get install -y libgl1` to `docker_build_pkg.sh` |
| Build arg too long | Qiskit (large env_payload) | Truncate or pre-install deps |
| Missing build deps | Various | Add to `docker_build_pkg.sh` |
| ASV benchmark import errors | Various | Install missing runtime deps |
