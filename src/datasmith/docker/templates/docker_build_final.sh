#!/usr/bin/env bash
set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [ASV_BENCHMARKS_FALLBACK]

Options:
  ASV_BENCHMARKS_FALLBACK  Optional fallback file with pre-computed benchmark
                           names (one per line).  Used only when live ASV
                           discovery fails.
  -h, --help               Show this help message and exit

Description:
  Finalises the Docker image by activating the conda environment, installing
  runtime dependencies, and discovering ASV benchmarks.

  Benchmarks are discovered via \`asv run --bench just-discover\` which scans
  the codebase for benchmark classes/functions.  The result is written to
  /workspace/repo/asv_benchmarks.txt and exported as ASV_BENCHMARKS in
  /etc/profile.d/asv_build_vars.sh so downstream scripts (run-tests.sh,
  profile.sh) can use them.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  show_help
  exit 0
fi

FALLBACK_FILE="${1:-}"

cd /workspace/repo || exit 1

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
# Defense in depth: never let a bare `fc_note ...` call abort this script
# under `set -euo pipefail` if the profile.d helper failed to source.
command -v fc_note >/dev/null 2>&1 || fc_note() { :; }
cat <<'EOF' >> ~/.bashrc
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
EOF


set +u
micromamba shell init --shell bash
echo "micromamba activate $ENV_NAME" >> ~/.bashrc
eval "$(micromamba shell hook --shell=bash)"
micromamba activate "$ENV_NAME" || true
set -u

micromamba run -n "$ENV_NAME" uv pip install git+https://github.com/formula-code/snapshot-tester.git || true
# LSV powers measure.sh's impacted-benchmark selection at container-run time.
# Installed here (per-build, NOT in the cached base image) so the same
# selector runs at stage 6 as at stage 7, where harbor's setup.sh installs it.
micromamba run -n "$ENV_NAME" uv pip install --no-cache git+https://github.com/formula-code/lsv.git || true
micromamba run -n "$ENV_NAME" uv pip install -q --upgrade coverage || true

# ── Discover ASV benchmarks ──────────────────────────────────────────
# Step 1: `asv run --bench just-discover` scans the codebase for benchmark
#         classes/functions and writes benchmarks.json (no benchmarks are
#         actually executed).
# Step 2: Benchmarks.load() reads benchmarks.json and we dump the names
#         one-per-line to asv_benchmarks.txt.
echo "[final] Discovering ASV benchmarks from ${CONF_NAME:-<unknown>} ..."
REAL_CONF="$(realpath "$CONF_NAME")"
CONF_DIR="$(dirname "$REAL_CONF")"

# asv must be run from the directory containing the config, and Benchmarks.load()
# resolves `results_dir` relative to the current working directory — so the
# extract step must also run with cwd == CONF_DIR.  Keep both inside the pushd.
pushd "$CONF_DIR" > /dev/null
micromamba run -n "$ENV_NAME" asv machine --yes --config "$REAL_CONF" \
  --machine dockertest --num_cpu 1 --ram 4GB 2>&1 | tail -1 || true
micromamba run -n "$ENV_NAME" asv run --bench just-discover \
  --config "$REAL_CONF" --python=same --machine=dockertest || true

# Extract benchmark names from the generated benchmarks.json. Using the
# conda env python (not base) so asv/asv_runner versions match what
# Benchmarks.load() wrote in the previous step.
micromamba run -n "$ENV_NAME" python - "$REAL_CONF" /workspace/repo/asv_benchmarks.txt <<'EXTRACT_EOF' || true
import sys
from asv.config import Config
from asv.benchmarks import Benchmarks

conf = Config.load(sys.argv[1])
bm = Benchmarks.load(conf)
with open(sys.argv[2], "w") as f:
    for b in sorted(bm._all_benchmarks.keys()):
        f.write(b + "\n")
EXTRACT_EOF
popd > /dev/null

# Fall back to the pre-computed file if live discovery produced nothing.
fc_note discovery_fallback_used=0
if [ ! -s /workspace/repo/asv_benchmarks.txt ] && [ -n "$FALLBACK_FILE" ] && [ -s "$FALLBACK_FILE" ]; then
    echo "[final] ASV discovery produced no results; falling back to pre-computed benchmarks."
    cp "$FALLBACK_FILE" /workspace/repo/asv_benchmarks.txt
    fc_note discovery_fallback_used=1
fi

if [ -s /workspace/repo/asv_benchmarks.txt ]; then
    COUNT=$(wc -l < /workspace/repo/asv_benchmarks.txt)
    echo "[final] Discovered $COUNT ASV benchmarks."
    fc_note discovered_n="$COUNT"
else
    echo "[final] WARNING: No ASV benchmarks discovered."
    fc_note discovered_n=0
fi

# ── Resolve benchmark directory ──────────────────────────────────────
ABS_BENCHMARK_DIR=$(micromamba run -n "$ENV_NAME" python -c 'import os,asv; from pathlib import Path; cp=Path(os.environ["CONF_NAME"]).resolve(); cfg=asv.config.Config.load(cp); raw=os.path.expandvars(str(cfg.benchmark_dir)); b=Path(raw).expanduser(); b=(cp.parent/b) if not b.is_absolute() else b; print(b.resolve())')

# ── Persist to asv_build_vars.sh for downstream scripts ──────────────
cat >>/etc/profile.d/asv_build_vars.sh <<VARS
export BENCHMARK_DIR=$ABS_BENCHMARK_DIR
export ASV_BENCHMARKS=/workspace/repo/asv_benchmarks.txt
VARS

fc_note benchmark_dir="$ABS_BENCHMARK_DIR"
if [ -f "$ABS_BENCHMARK_DIR/__init__.py" ]; then
    fc_note benchmark_dir_init_present=1
else
    fc_note benchmark_dir_init_present=0
fi
fc_note cpu_cap="${LOKY_MAX_CPU_COUNT:-${N_JOBS_MAX:-}}"
fc_note rounds="${DATASMITH_LSV_ROUNDS:-5}"

# Identity, derived from the repo's own remote. Purely descriptive — no invariant
# depends on these — but without them a manifest cannot be attributed to a task
# when read back out of a published image.
_ORIGIN="$(git -C /workspace/repo remote get-url origin 2>/dev/null || true)"
if [ -n "$_ORIGIN" ]; then
    fc_note owner="$(printf '%s' "$_ORIGIN" | sed -E 's#.*[:/]([^/]+)/[^/]+(\.git)?$#\1#')"
    # NOTE: was `sed -E 's#.*/([^/]+?)(\.git)?$#\1#'`. POSIX ERE (sed -E) has no
    # non-greedy quantifier, so the `?` on `[^/]+?` is a silent no-op: `[^/]+`
    # greedily consumes "requests.git" whole and `(\.git)?` matches nothing,
    # leaving the .git suffix in `repo` on every build (verified below).
    # `basename ... .git` strips the suffix unconditionally and correctly, with
    # no regex to get wrong.
    fc_note repo="$(basename "$_ORIGIN" .git)"
fi

# Scan every baked script for credential literals.  grep -q returns 1 on no
# match, which is the clean case.
if grep -rqE 'sb_secret_|service_role|SUPABASE_[A-Z_]*KEY=[A-Za-z0-9]' \
     /docker_build_final.sh /run-tests.sh /profile.sh 2>/dev/null; then
    fc_note secrets_scan_clean=0
else
    fc_note secrets_scan_clean=1
fi

echo "Docker ASV benchmark finalization complete."
