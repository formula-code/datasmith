#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo || exit 1

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

TASK_ID="{{ task_id }}"
export TASK_ID

# Harbor's [verifier.env] section only reaches test.sh, not setup.sh, so we
# bake the agent name directly into setup.sh at render time. lsv_init.py
# reads this to decide whether to capture the oracle snapshot baseline.
export HARBOR_AGENT_NAME="oracle"

setup_start=$(date +%s)
# Track which phase we're currently in so the exit trap can report WHERE
# setup crashed, not just the exit code. Updated at every phase boundary
# below.
SETUP_PHASE="init"
lsv_init_start=""
lsv_init_end=""
mkdir -p /logs/artifacts

# Crash trap — always writes setup_status.json even if setup.sh exits 1.
# Without this, a failing lsv_init leaves no record of WHY setup failed and
# parser.py has to guess. The trap runs on any exit (normal or error), so we
# write the timings file here too instead of at the happy-path bottom.
_write_setup_status() {
  local ec=$?
  local setup_end
  setup_end=$(date +%s)
  local lsv_init_s=0
  if [ -n "${lsv_init_start}" ] && [ -n "${lsv_init_end}" ]; then
    lsv_init_s=$((lsv_init_end - lsv_init_start))
  elif [ -n "${lsv_init_start}" ]; then
    lsv_init_s=$((setup_end - lsv_init_start))
  fi
  cat > /logs/artifacts/setup_status.json <<JSONEOF
{
  "exit_code": ${ec},
  "failed_phase": "${SETUP_PHASE}",
  "succeeded": $([ ${ec} -eq 0 ] && echo true || echo false)
}
JSONEOF
  cat > /logs/artifacts/setup_timings.json <<JSONEOF
{
  "setup_total_s": $((setup_end - setup_start)),
  "lsv_init_s": ${lsv_init_s}
}
JSONEOF
  # Preserve the original exit code: without this, the trap's last command
  # (a successful `cat > ... <<JSONEOF`) resets $? to 0 and bash exits with
  # that, so Harbor records setup.sh as "completed successfully" even when
  # lsv_init crashed. test.sh then runs against a broken environment and
  # silently produces an empty reward.
  exit $ec
}
trap _write_setup_status EXIT

SETUP_PHASE="list_files"
# Show the entire file directory
echo "[$(ts)] [setup] Get all file directory..."
ls -R .

SETUP_PHASE="source_asv_env"
# ── Source ASV environment ───────────────────────────────────────────────
echo "[$(ts)] [setup] Sourcing ASV environment..."
set +u
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"
micromamba activate "$ENV_NAME" || true
set -u

SETUP_PHASE="install_deps"
# ── Install dependencies ────────────────────────────────────────────────
echo "[$(ts)] [setup] Installing LSV + snapshot-tool..."

micromamba run -n "$ENV_NAME" uv pip install -q --upgrade asv_runner || true
micromamba run -n "$ENV_NAME" uv pip install -q coverage || true
micromamba run -n "$ENV_NAME" uv pip install -q --no-cache \
    "git+https://github.com/formula-code/lsv.git" || true
micromamba run -n "$ENV_NAME" uv pip install -q --no-cache \
    "git+https://github.com/formula-code/snapshot-tester.git" || true
micromamba run -n "$ENV_NAME" uv pip install -q -U jinja2 || true

SETUP_PHASE="extra_setup_commands"
{{ extra_setup_commands }}

SETUP_PHASE="lsv_init"
# ── LSV Phase 1: initialize_diffcheck ────────────────────────────────────
# Helpers live at /opt/lsv (baked into the image by Dockerfile) because
# Harbor uploads /tests/ AFTER setup.sh runs.
echo "[$(ts)] [setup] Starting LSV init..."
lsv_init_start=$(date +%s)
set +e
python /opt/lsv/lsv_init.py --rounds {{ rounds }}
lsv_exit=$?
set -e
lsv_init_end=$(date +%s)
if [ $lsv_exit -ne 0 ]; then
  echo "[$(ts)] [setup] FATAL: lsv_init.py failed with exit code $lsv_exit (137=OOM killed). Aborting task."
  # Propagate the real exit code (e.g. 137 for OOM) so the trap captures
  # the truth instead of masking it with a generic 1. Without this,
  # setup_status.json always says exit_code=1 regardless of the real
  # cause.
  exit $lsv_exit
fi
echo "[$(ts)] [setup] LSV init complete."
SETUP_PHASE="complete"
