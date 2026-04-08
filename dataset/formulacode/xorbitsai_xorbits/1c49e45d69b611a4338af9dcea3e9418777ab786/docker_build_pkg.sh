#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

# Define log function if not already defined
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
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

  IMP="xorbits"
  log "Using import name: $IMP"

  # Install core dependencies first
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=$version" \
    pip setuptools wheel packaging \
    numpy \
    "pandas>=1.5.1" \
    "scipy>=1.9.2" \
    "cython>=0.29.33" \
    "requests>=2.4.0" \
    "cloudpickle>=2.2.1"

  # Install development dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    git conda mamba "libmambapy<=1.9.9" \
    compilers meson-python cmake ninja pkg-config tomli \
    pytest

  # Install pip-specific dependencies
  micromamba run -n "$ENV_NAME" pip install oldest-supported-numpy xoscar>=0.0.8

  # Editable install from python/ subdirectory
  log "Editable install with build isolation"
  cd "$REPO_ROOT/python"
  micromamba run -n "$ENV_NAME" pip install -e .
  cd "$REPO_ROOT"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
