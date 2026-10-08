#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo || exit 1

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

# DB rows are keyed by (owner, repo, issue_number); TASK_ID is the harbor task id.
OWNER="{{ owner }}"
REPO="{{ repo }}"
ISSUE_NUMBER="{{ issue_number }}"
TASK_ID="{{ task_id }}"
export OWNER REPO ISSUE_NUMBER TASK_ID

# Runs first in test.sh, in the verifier container, which starts from the task image (Harbor separate verifier).
# lsv_init.py captures the oracle snapshot baseline only when this is "oracle"; test.sh sets it from the run config.
export HARBOR_AGENT_NAME="${AGENT_KEY:-agent}"

setup_start=$(date +%s)
# The exit trap reports which phase failed.
SETUP_PHASE="init"
lsv_init_start=""
lsv_init_end=""
# Harbor copies the agent's /logs/artifacts into this container; nothing the agent wrote there may be read.
rm -rf /logs/artifacts
mkdir -p /logs/artifacts

# parser.py and lsv_measure.py read setup_status.json, so write it (and timings) on every exit.
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
  # Otherwise the trap's last command resets $? to 0 and a crashed setup reads as success.
  exit $ec
}
trap _write_setup_status EXIT

SETUP_PHASE="source_asv_env"
# The pipeline may point TMPDIR at a folder on the host; tempfile falls back to /tmp while it is missing.
[ -n "${TMPDIR:-}" ] && mkdir -p "$TMPDIR"
# Verifier deps are installed at image build (environment/Dockerfile); only activate the env here.
echo "[$(ts)] [prepare] Activating ASV environment..."
set +u
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
set -u

if [ -z "${ENV_NAME:-}" ]; then
  echo "[$(ts)] [prepare] FATAL: ENV_NAME is empty after sourcing ASV profile scripts." >&2
  exit 1
fi

eval "$(micromamba shell hook --shell=bash)"
micromamba activate "$ENV_NAME"

SETUP_PHASE="probe_image_deps"
# Fail fast on a stale image. LSV installs as asv.contrib.lightspeed (there is no top-level `lsv`).
micromamba run -n "$ENV_NAME" python -c "import asv_runner, coverage, jinja2" \
  || { echo "[$(ts)] [prepare] FATAL: image verifier deps missing in '$ENV_NAME' — rebuild the task image." >&2; exit 1; }
micromamba run -n "$ENV_NAME" python -c "import asv.contrib.lightspeed" \
  || { echo "[$(ts)] [prepare] FATAL: LSV (asv.contrib.lightspeed) missing in '$ENV_NAME' — rebuild the task image." >&2; exit 1; }
command -v snapshot-tool >/dev/null 2>&1 \
  || micromamba run -n "$ENV_NAME" bash -c 'command -v snapshot-tool >/dev/null 2>&1' \
  || { echo "[$(ts)] [prepare] FATAL: snapshot-tool missing in '$ENV_NAME' — the correctness gate would be inert; rebuild the task image." >&2; exit 1; }

SETUP_PHASE="project_imports"
# A second installed copy of the project (a wheel pulled in as a dependency) hides the repo from benchmark processes.
python /tests/project_imports.py --remove || echo "[$(ts)] [prepare] WARN: project import check failed." >&2

SETUP_PHASE="asv_machine"
# `asv run` without --machine looks up the hostname in ~/.asv-machine.json and stops if it is missing; results go to dockertest.
python -c 'import socket; from asv.machine import Machine, MachineCollection; MachineCollection.save(socket.gethostname(), {**Machine.get_defaults(), "machine": "dockertest"})' \
  || echo "[$(ts)] [prepare] WARN: asv machine registration failed; agents must pass --machine." >&2

SETUP_PHASE="asv_config"
python /tests/write_asv_conf.py || echo "[$(ts)] [prepare] WARN: could not write /workspace/asv.conf.json." >&2

SETUP_PHASE="baseline_commit"
# The grader diffs against a commit of this starting tree, so the image's own edits and untracked benchmark suites
# are not agent work.
if [ "$(cat /opt/fc_baseline_sha 2>/dev/null)" != "$(git rev-parse HEAD)" ]; then
{% filter indent(2) %}
{% include "shared/stage_tree.sh" %}
{% endfilter %}
  # A commit with many loose objects starts a background gc, which deletes objects while base_copy moves the repo.
  git config gc.auto 0
  git -c user.name=fc -c user.email=fc@local commit -q --no-verify --allow-empty -m fc-baseline
  git rev-parse HEAD > /opt/fc_baseline_sha
  chmod 444 /opt/fc_baseline_sha
fi

SETUP_PHASE="base_copy"
# lsv_measure.py times this unpatched copy (with its build) against the patched repo. Overlayfs cannot rename
# image directories, so both trees are copies. No mv of the image tree: on a merged directory with ~10^5 git objects,
# mv stopped on entries already gone (numpy#12445, pandas#40007); rm -rf skips them.
if [ "${FC_LSV_PAIRED:-1}" != "0" ]; then
  cd /
  rm -rf /workspace/.fc_base /workspace/.fc_new
  cp -a /workspace/repo /workspace/.fc_base
  cp -a /workspace/repo /workspace/.fc_new
  rm -rf /workspace/repo
  mv /workspace/.fc_new /workspace/repo
  [ "$(git -C /workspace/repo rev-parse HEAD)" = "$(git -C /workspace/.fc_base rev-parse HEAD)" ]
  cd /workspace/repo
fi

SETUP_PHASE="lsv_init"
# ── LSV Phase 1: initialize_diffcheck ────────────────────────────────────
echo "[$(ts)] [prepare] Starting LSV init..."
lsv_init_start=$(date +%s)
set +e
# Serialize the measure through the host gate (rl/measure_gate.py); fail-open. SID uses the kernel uuid
# because $$ is the same in every container. --connect-timeout keeps a dropped-packet host from stalling -m.
_MG_URL="${MEASURE_GATE_URL:-}"
_MG_SID="mg-init-$(cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "$(hostname 2>/dev/null || echo h)-$$-${RANDOM}")"
# Logged like test.sh's acquires ("<step> 1|0") so the pipeline can drop ungated trials.
_mg_resp="$(curl -fsS --connect-timeout "${MEASURE_GATE_CONNECT_TIMEOUT:-5}" -m "${MEASURE_GATE_ACQUIRE_WAIT:-7200}" -X POST "${_MG_URL}/acquire?sid=${_MG_SID}" 2>/dev/null)"
case "${_mg_resp}" in *'"acquired": true'*) echo "init 1" ;; *) echo "init 0" ;; esac >> /logs/artifacts/measure_gate.txt
python /tests/lsv_init.py{% if rounds is not none %} --rounds {{ rounds }}{% endif %}
lsv_exit=$?
curl -fsS --connect-timeout "${MEASURE_GATE_CONNECT_TIMEOUT:-5}" -m 5 -X POST "${_MG_URL}/release?sid=${_MG_SID}" >/dev/null 2>&1 || true
set -e
lsv_init_end=$(date +%s)
if [ $lsv_exit -ne 0 ]; then
  echo "[$(ts)] [prepare] FATAL: lsv_init.py failed with exit code $lsv_exit (137=OOM killed). Aborting task."
  exit $lsv_exit
fi
echo "[$(ts)] [prepare] LSV init complete."

SETUP_PHASE="agent_tree"
# The agent's tree is the base commit plus tree.diff; ignored files (the build) stay as the image has them.
# A missing or broken tree.diff leaves the starting tree, so the trial has no patch (no_patch).
# Symlinks, *.pth, site/usercustomize.py and package metadata (entry points) run code outside the patched sources.
_refused="$( { git apply --summary /fc_submission/tree.diff 2>/dev/null | grep -E ' 120000( |$)' || true; \
  git apply --numstat -z /fc_submission/tree.diff 2>/dev/null | tr '\0' '\n' | sed -E 's/^[0-9-]+\t[0-9-]+\t//' \
    | grep -E '(^|/)(site|user)customize\.py$|\.pth$|\.(dist|egg)-info/' || true; } )"
if [ -n "${_refused}" ]; then
  echo "[$(ts)] [prepare] tree.diff refused: ${_refused}" >&2
  echo tree_diff_refused > /logs/artifacts/agent_tree.txt
  printf '%s\n' "${_refused}" > /logs/artifacts/tree_diff_refused.txt
elif [ -s /fc_submission/tree.diff ]; then
  git read-tree -u --reset {{ base_commit }}
  if ! git apply --binary --whitespace=nowarn /fc_submission/tree.diff; then
    echo "[$(ts)] [prepare] tree.diff does not apply; using the starting tree." >&2
    echo tree_diff_failed > /logs/artifacts/agent_tree.txt
    git read-tree -u --reset "$(cat /opt/fc_baseline_sha)"
  fi
  # The index matches fc-baseline again, as when the agent edited on top of it in one container.
  git read-tree "$(cat /opt/fc_baseline_sha)"
fi
SETUP_PHASE="complete"
