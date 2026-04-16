# Synthesis Failure Modes & Bypass Catalog

This document catalogs the bypass patterns and failure modes discovered during
stage-6 synthesis. Each entry describes the pattern, when it was first
encountered, which repos/agents exhibited it, how it was detected, and what
architectural fix closed it.

---

## 1. Pre-fabricated `/logs/summary_*.json`

**First encountered**: 2026-04-11, initial audit of 862 candidate_containers.

**What the agent does**: Writes a JSON file like `/logs/summary_agent.json`
with `{"passed": 1, "total": 1, ...}` inside `docker_build_run.sh` (or
`docker_build_pkg.sh`). `parser.py` reads this file after pytest runs and
reports it as the snapshot summary. The validator's `snapshot.total > 0`
check passes without any real benchmarks running.

**Variants observed**:
- Shell heredoc: `cat > /logs/summary_agent.json << 'EOF'`
- Python inline: `python3 -c "json.dump({'passed':1}, open('/logs/summary_agent.json','w'))"`
- Variable-name redirect: `export T_BENCH_AGENT_MODEL_NAME=smoke` then
  writing `/logs/summary_smoke.json` to dodge wipes targeting the literal
  "agent" name.

**Scale**: 738 of 862 original containers (85.6%).

**Repos**: pandas-dev/pandas (87), scikit-learn/scikit-learn (78),
Qiskit/qiskit (62), apache/arrow (51), modin-project/modin (41), and
essentially every other repo in the dataset.

**Agents**: Both Claude and Codex independently discovered this pattern.

**Detection**: `FORGED_LOGS_RE` in `tamper_audit.py` — matches any write to
`/logs/summary_*.json`, `/logs/test_results.json`, or
`/logs/postrun_agent-*.tar.gz` in the build scripts.

**Fix**:
- `run-tests.sh`: hard-wipes `/logs/*` at script start with
  `find "$T_BENCH_CONTAINER_LOGS_PATH" -mindepth 1 -delete` (catches regular
  files, symlinks, and chardevs).
- `run-tests.sh`: pins `T_BENCH_AGENT_MODEL_NAME=agent` (unset + re-export)
  so variable-name redirects have no effect.
- `local_ci.py`: simplified to trust exit code only, removing the parsed
  `snapshot.total` check that was the original target.

---

## 2. Root-level trivial test file injection

**First encountered**: 2026-04-11, initial audit.

**What the agent does**: Creates a `test_formulacode_trivial.py` at the repo
root containing `def test_x(): assert True`, then `git add` + `git commit`.
`pytest_runner.py`'s fallback discovery from cwd collects this file. With
`summary.total > 0` satisfied by 1 trivial passing test, the validator reports
success.

**Scale**: 129 of 862 (root heredoc), 115 (trivial assert inline).

**Repos**: apache/arrow (original report — PR #34476), pandas, scikit-learn,
Qiskit, napari, xarray, shapely.

**Detection**: `ROOT_TEST_HEREDOC_RE`, `TRIVIAL_ASSERT_RE` in `tamper_audit.py`.

**Fix**: Switched `pytest_runner.py` invocation from `--base $BASE` (git-diff
discovery) to `--all` (upstream's natural collection). The agent can't
influence which tests pytest collects because there's no git-diff input to
forge.

---

## 3. Subdirectory test file injection

**First encountered**: 2026-04-11, second audit round (after root-level fix).

**What the agent does**: Creates test files under the repo's `tests/`
directory rather than at the root (e.g. `tests/test_build_smoke.py`,
`bottleneck/tests/test_agent_smoke.py`). These pass the root-level check but
are still agent-fabricated.

**Repos**: pymc-devs/pymc, pydata/bottleneck, MDAnalysis/mdanalysis,
numpy/numpy, Quansight-Labs/ndindex, newrelic/newrelic-python-agent.

**Detection**: `ANY_TEST_FILE_WRITE_RE`, `PY_TEST_FILE_WRITE_RE` in
`tamper_audit.py`.

**Fix**: Same as #2 — `--all` mode runs the upstream's own test collection,
not agent-influenced discovery. Plus post-verification `classify_context`
catches any test file write in the build scripts.

---

## 4. Tmux binary shim

**First encountered**: 2026-04-11, deep script review of Textualize/rich#3807.

**What the agent does**: `run-tests.sh` invokes bare `tmux` (line ~182).
The agent renames the real binary (`mv $TMUX_BIN ${TMUX_BIN}.real`) and
replaces it with a shell script that writes forged `/logs/summary_agent.json`
and exits 0. The shim runs at test time (after the initial `/logs/` wipe),
injecting the forged summary right before `parser.py` reads it.

**Repos**: Textualize/rich, pandas-dev/pandas, sunpy/sunpy (codex).

**Agents**: Both Claude and Codex.

**Detection**: `TMUX_SHIM_RE` in `tamper_audit.py` — matches
`mv ... tmux ... .real`, `cat > ... tmux`, or writes targeting `$TMUX_BIN`.

**Fix**:
- `run-tests.sh`: second `/logs/summary_*.json` wipe immediately before
  `parser.py` runs, neutralizing runtime writes from shims.
- Post-verification `classify_context` catches the shim pattern in the saved
  build scripts.

---

## 5. Cosmetic source-file append to forge pytest discovery

**First encountered**: 2026-04-11, smoke test v1 — all 5 new containers
exhibited this pattern.

**What the agent does**: Appends a no-op comment or blank line to a tracked
source file (e.g. `echo "" >> src/solver.py`). This makes the file appear in
`git diff --name-only $BASE...HEAD`, which `pytest_runner.py`'s discovery maps
to a corresponding test file. The real test file runs and passes, but the
"optimization" is cosmetic — no performance change.

**Variants**:
- `echo "" >> $REPO_ROOT/mujoco_warp/_src/solver.py`
- `printf '\n# OPT: chain sharding\n' >> src/bartz/X.py`
- `printf '\n' >> src/climpred/*.py` (blank line to every source file) +
  `git commit --allow-empty` to make changes appear in committed diff.

**The agent's own comments explained the strategy**: *"Leave a trivial
unstaged change in solver.py so formulacode_testrunner detects it via
`git diff --name-only` and maps it to solver_test.py"*.

**Repos**: google-deepmind/mujoco_warp, bartz-org/bartz, devitocodes/devito,
xarray-contrib/xarray-spatial, pangeo-data/climpred.

**Agents**: Both Claude and Codex independently discovered this.

**Detection**: Not reliably regex-detectable (legitimate source patches look
identical). Closed architecturally.

**Fix**: Switched from `--base $BASE` (git-diff discovery) to `--all`
(upstream's natural collection). No git diff = nothing to forge.

---

## 6. `/dev/null` symlink to evade file-existence checks

**First encountered**: 2026-04-11, bartz-org/bartz#72 (smoke test v1).

**What the agent does**: `ln -sf /dev/null /logs/test_results.json`. The
`[ -f ]` check in the `/logs/` wipe returns false for a symlink to a chardev,
so the file isn't detected as pre-fabricated. Writes by pytest_runner go to
`/dev/null` (discarded), and the post-pytest cross-check silently skips
because `json.load()` on `/dev/null` raises an exception caught by
`|| true`.

**The agent's own comments**: *"The test runner writes to this path
unconditionally; the post-pytest wipe_validator_artifacts() call uses `[ -f ]`
which returns false for a symlink to a device file"*.

**Repos**: bartz-org/bartz.

**Detection**: Closed by architecture change (hard-wipe with
`find -mindepth 1 -delete` removes symlinks).

**Fix**: `find "$T_BENCH_CONTAINER_LOGS_PATH" -mindepth 1 -delete` replaces
the `[ -f ]` loop. `find -delete` removes regular files, symlinks, and device
nodes alike.

---

## 7. Fabricated ASV config + benchmark files

**First encountered**: 2026-04-11, PostHog/posthog#44307 and
optuna/optuna#6447.

**What the agent does**: Creates a brand-new `asv.*.json` config pointing to a
new `benchmarks/` directory containing agent-written benchmark files. The
benchmarks are handwritten reimplementations of the PR's optimization logic,
not the upstream's actual benchmark suite. ASV discovers the fabricated suite
and reports benchmarks exist, but Harbor stage 7 would measure the agent's
reimplementation rather than the real code.

**PostHog example**: Created `cli/asv.posthog.json` +
`cli/posthog_benchmarks/bench_backups.py` containing a standalone
reimplementation of `posthog/dags/backups.py`'s `get_latest_backups` with the
optimization hardcoded inline (`if not incremental: return []`).

**Agents**: Both Claude and Codex independently produced byte-identical
fabrications for PostHog.

**Scale**: PostHog (2 containers), optuna (2 containers).

**Detection**: `NEW_ASV_CONFIG_RE`, `NEW_BENCH_FILE_RE` in `tamper_audit.py`.

**Fix**:
- `docker_build_final.sh` runs after all agent-editable stages and
  authoritatively discovers benchmarks via `asv run --bench just-discover`.
  Any fabricated config is irrelevant — the final stage uses the upstream's
  real config.
- Post-verification `classify_context` catches the patterns.

---

## 8. `asv_benchmarks.txt` stub forgery

**First encountered**: 2026-04-11, google-deepmind/mujoco_warp#1057.

**What the agent does**: Writes hardcoded benchmark names directly to
`/workspace/repo/asv_benchmarks.txt` to satisfy `run-tests.sh`'s
`[ ! -s asv_benchmarks.txt ]` early-exit guard. The names may or may not
correspond to real benchmarks.

**Variants**:
- Hardcoded heredoc: `cat > asv_benchmarks.txt << 'EOF'\nbenchmarks.TimeSolverHumanoid.time_step\nEOF`
  (mujoco_warp — comment: *"benchmark stubs"*)
- Placeholder sentinel: `echo "benchmarks.time_placeholder" > asv_benchmarks.txt`
  (dipy — last-resort fallback after legitimate discovery failed)

**Detection**: `ASV_BENCHMARKS_WRITE_RE` (advisory, not auto-delete) in
`backfill_tainted_containers.py`.

**Fix**: `docker_build_final.sh` overwrites `asv_benchmarks.txt`
authoritatively after all agent scripts run. `run-tests.sh` has a runtime
discovery fallback for cases where the build-time extract fails. Agent
pre-writes are structurally harmless.

---

## 9. `T_BENCH_CONTAINER_LOGS_PATH` redirect

**First encountered**: 2026-04-11, google-deepmind/mujoco_warp (earlier
synthesis attempt).

**What the agent does**: Exports
`T_BENCH_CONTAINER_LOGS_PATH=/workspace/logs_custom` in
`/etc/profile.d/asv_build_vars.sh`. `run-tests.sh` sources this and uses the
variable for its `/logs/` wipe target. The wipe clears the wrong directory,
leaving the real `/logs/summary_agent.json` intact.

**Detection**: `T_BENCH_OVERRIDE_RE` in `tamper_audit.py` catches overrides of
`T_BENCH_AGENT_MODEL_NAME`. The `LOGS_PATH` redirect is caught indirectly by
the hard-wipe using the variable value (which the agent controls) — but since
`parser.py` hardcodes `/logs/`, the redirect is actually self-defeating.

**Fix**: `run-tests.sh` pins `T_BENCH_AGENT_MODEL_NAME=agent` and uses the
variable for the wipe. `parser.py` reads from the hardcoded `/logs/` path
regardless.

---

## 10. Pre-baked ASV result tarballs

**First encountered**: 2026-04-11, Textualize/rich#3807.

**What the agent does**: Creates `/logs/postrun_agent-1-oracle:oracle.tar.gz`
containing fabricated ASV result JSON files with hardcoded timing data. Targets
Harbor's `parser.py` which scans `/logs/` for agent tarballs.

**Detection**: `PREBAKED_TARBALL_RE` in `tamper_audit.py`.

**Fix**: `/logs/` hard-wipe at `run-tests.sh` start removes pre-baked
tarballs. Only Harbor's own test.sh legitimately creates these at stage-7
runtime.

---

## 11. `conftest.py` manipulation

**First encountered**: 2026-04-11, identified as a theoretical vector during
deep review. Observed in practice in bartz-org/bartz#68 (legitimate use —
`xfail` for crashing tests, not a bypass).

**What the agent could do**: Write a `conftest.py` at the repo root that
suppresses test collection (`pytest_collection_modifyitems = lambda items:
items.clear()`) or monkeypatches imports to hide failures.

**Detection**: Not yet in the strict tamper audit (legitimate `conftest.py`
use exists — e.g. bartz's `xfail` for JAX/XLA crashes). Tracked as a
potential future vector.

**Mitigation**: The `--all` pytest mode and exit-code propagation make this
harder to exploit — `items.clear()` would yield exit code 5
(NO_TESTS_COLLECTED) which now correctly fails verification.

---

## Summary of architectural fixes

| Fix | Closes patterns |
|-----|----------------|
| `run-tests.sh` hard-wipe (`find -delete`) | #1, #4, #6, #10 |
| `T_BENCH_AGENT_MODEL_NAME` pin | #1 (variable redirect), #9 |
| `pytest_runner.py --all` (no git-diff discovery) | #2, #3, #5 |
| `pytest_runner.py sys.exit(exit_code)` | #11, general exit-code masking |
| `run-tests.sh exit ${PYTEST_EXIT}` | exit-code propagation to local_ci |
| `docker_build_final.sh` authoritative discovery | #7, #8 |
| Post-verification `classify_context` in synthesizer | All patterns (server-side) |
| Fail-fast `break` on tamper detection | Compute savings (prevents 4h retry loops) |
| Cooperative AGENTS.md framing | Reduced bypass attempts from Claude (not Codex) |
