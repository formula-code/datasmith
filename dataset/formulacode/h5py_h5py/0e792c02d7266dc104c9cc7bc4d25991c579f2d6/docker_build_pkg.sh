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
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="h5py"
  log "Using import name: $IMP"

  # Install build dependencies including HDF5
  log "Installing build dependencies"
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "python=$version" \
    numpy scipy cython \
    compilers meson-python cmake ninja pkg-config \
    hdf5 pkgconfig

  # Install build requirements from pyproject.toml
  log "Installing build requirements"
  micromamba run -n "$ENV_NAME" pip install \
    "Cython>=0.29.31,<4" \
    "numpy>=2.0.0,<3" \
    "pkgconfig" \
    "setuptools>=77"

  # Set build environment variables
  export HDF5_DIR="$(micromamba run -n "$ENV_NAME" pkg-config --variable=prefix hdf5)"
  log "HDF5_DIR set to: $HDF5_DIR"

  # Editable install with no build isolation
  log "Performing editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"$EXTRAS

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
