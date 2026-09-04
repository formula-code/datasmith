#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo || exit 1

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

OWNER="{{ owner }}"
REPO="{{ repo }}"
ISSUE_NUMBER="{{ issue_number }}"
# TASK_ID retained as a single-column alias (= issue_number) for back-compat
# with anything that still reads $TASK_ID. The canonical id is the triple.
TASK_ID="${ISSUE_NUMBER}"
export OWNER REPO ISSUE_NUMBER TASK_ID

# Harbor's [verifier.env] section only reaches test.sh, not setup.sh, so we
# bake the agent name directly into setup.sh at render time. lsv_init.py reads
# this to decide whether to CAPTURE the oracle snapshot baseline during setup.
#
# In PRODUCTION RL this MUST be non-oracle: lsv_init.py's capture is "oracle
# only -- production runs download pre-built snapshots" (lsv_init.py:500). The
# correctness gate's real baseline is downloaded by test.sh from db at verify
# time (keyed on test.sh's own AGENT_KEY, independent of this value), and
# snapshot-tool verify re-captures the current state fresh -- so a setup-time
# capture is pure redundant work (e.g. ~30s/trial on shapely). Leaving this
# "oracle" was a leftover from the offline oracle-baseline recipe. Set it back
# to "oracle" ONLY when generating that baseline, never for a training run.
export HARBOR_AGENT_NAME="agent"

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
# ── Dependencies ────────────────────────────────────────────────────────
# LSV + snapshot-tool + asv_runner/coverage/jinja2 are BAKED into the image
# by the Dockerfile now, so all 8-48 trials reuse one install instead of
# pip-cloning from GitHub every trial (~10-40s/trial). Fallback: if a base
# image predates the bake, install them here so the trial still works.
if micromamba run -n "$ENV_NAME" bash -c 'command -v snapshot-tool >/dev/null 2>&1 && python -c "import asv.contrib.lightspeed"' 2>/dev/null; then
  echo "[$(ts)] [setup] LSV + snapshot-tool already baked into image; skipping install."
else
  echo "[$(ts)] [setup] Installing LSV + snapshot-tool (not baked)..."
  micromamba run -n "$ENV_NAME" uv pip install -q --upgrade asv_runner coverage jinja2 \
      "git+https://github.com/formula-code/lsv.git" \
      "git+https://github.com/formula-code/snapshot-tester.git" || true
fi

SETUP_PHASE="extra_setup_commands"
{{ extra_setup_commands }}

SETUP_PHASE="lsv_init"
# ── LSV Phase 1: initialize_diffcheck ────────────────────────────────────
# Helpers live at /opt/lsv (baked into the image by Dockerfile) because
# Harbor uploads /tests/ AFTER setup.sh runs.
echo "[$(ts)] [setup] Starting LSV init..."
lsv_init_start=$(date +%s)
set +e
# measure gate: serialize LSV baseline measure (fail-open); release AFTER capturing $?
_MG_URL="${MEASURE_GATE_URL:-http://172.17.0.1:8266}"
_MG_SID="mg-$(hostname 2>/dev/null || echo h)-$$-${RANDOM}"
curl -fsS -m 2000 -X POST "${_MG_URL}/acquire?sid=${_MG_SID}" >/dev/null 2>&1 || true
python /opt/lsv/lsv_init.py --rounds {{ rounds }}
lsv_exit=$?
curl -fsS -m 5 -X POST "${_MG_URL}/release?sid=${_MG_SID}" >/dev/null 2>&1 || true
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
