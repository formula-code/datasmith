#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[build] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.11}}"
EXTRAS="${ALL_EXTRAS:+[test]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="$version" \
    pip \
    git \
    "hatchling>=1.8.0" \
    "hatch-vcs>=0.3.0" \
    pytest \
    numpy \
    dask \
    attrs \
    pydantic \
    pytest-cov \
    wrapt \
    msgspec \
    toolz

  # Editable install with all test dependencies
  log "Installing package in editable mode with test extras"
  micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT[test]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" python -c "import psygnal; print('Successfully imported psygnal')"

  if [[ "${RUN_PYTEST_SMOKE:-}" == "1" ]]; then
    log "Running pytest smoke tests"
    micromamba run -n "$ENV_NAME" pytest "$REPO_ROOT/tests" -v --color=yes
  fi

  echo "::import_name=psygnal::env=${ENV_NAME}"
done

log "All builds complete ✅"
