#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="kartothek"
  log "Using import name: $IMP"

  # Create environment with Python and basic tools
  micromamba create -y -n "$ENV_NAME" -c conda-forge \
    "python=$version" pip git conda mamba "libmambapy<=1.9.9"

  # Install core dependencies with specific versions
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "numpy!=1.15.0,!=1.16.0" \
    "pandas>=0.23.0,!=1.0.0" \
    "pyarrow>=0.17.1,!=1.0.0,<4" \
    "dask[dataframe]" \
    "msgpack-python>=0.5.2" \
    simplejson \
    simplekv \
    storefact \
    toolz \
    urlquote \
    zstandard \
    attrs \
    click \
    decorator \
    prompt-toolkit \
    pyyaml \
    setuptools \
    setuptools_scm

  # Install typing_extensions for Python < 3.8
  if [[ "${version}" < "3.8" ]]; then
    micromamba run -n "$ENV_NAME" pip install "typing_extensions"
  fi

  # Install the package in editable mode
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" pip install -v -e "$REPO_ROOT"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
