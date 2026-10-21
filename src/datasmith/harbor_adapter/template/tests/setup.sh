#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo || exit 1

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }

# Runs in the agent container before the agent. It only prepares the agent's tools: grading runs in a fresh
# container from the task image (Harbor separate verifier), where tests/prepare.sh does the rest.
# The pipeline may point TMPDIR at a folder on the host; tempfile falls back to /tmp while it is missing.
[ -n "${TMPDIR:-}" ] && mkdir -p "$TMPDIR"
# Verifier deps are installed at image build (environment/Dockerfile); only activate the env here.
echo "[$(ts)] [setup] Activating ASV environment..."
set +u
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
set -u

if [ -z "${ENV_NAME:-}" ]; then
  echo "[$(ts)] [setup] FATAL: ENV_NAME is empty after sourcing ASV profile scripts." >&2
  exit 1
fi

eval "$(micromamba shell hook --shell=bash)"
micromamba activate "$ENV_NAME"

# A second installed copy of the project (a wheel pulled in as a dependency) hides the repo from benchmark processes.
python /tests/project_imports.py --remove || echo "[$(ts)] [setup] WARN: project import check failed." >&2

# `asv run` without --machine looks up the hostname in ~/.asv-machine.json and stops if it is missing; results go to dockertest.
python -c 'import socket; from asv.machine import Machine, MachineCollection; MachineCollection.save(socket.gethostname(), {**Machine.get_defaults(), "machine": "dockertest"})' \
  || echo "[$(ts)] [setup] WARN: asv machine registration failed; agents must pass --machine." >&2

# instruction.md names these. /tests is removed before the agent runs, so the rebuild script is copied out.
install -m 755 /tests/rebuild.sh /usr/local/bin/rebuild-repo
python /tests/write_asv_conf.py || echo "[$(ts)] [setup] WARN: could not write /workspace/asv.conf.json." >&2

{{ extra_setup_commands | default("") }}

# The starting tree, as a tree object only (no commit): the collect step diffs the agent's tree against it.
(
  export GIT_INDEX_FILE="$(mktemp -d)/index"
  git read-tree HEAD
{% filter indent(2) %}
{% include "shared/stage_tree.sh" %}
{% endfilter %}
  git write-tree > /opt/fc_start_tree
) || echo "[$(ts)] [setup] WARN: starting tree not recorded; the diff is taken against the base commit." >&2
