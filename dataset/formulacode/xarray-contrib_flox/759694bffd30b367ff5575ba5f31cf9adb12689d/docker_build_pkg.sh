#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs
set -euo pipefail

# Helper functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
# Only use Python 3.11+ as required by the package
TARGET_VERSIONS="${PY_VERSION:-3.11}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set" >&2
  exit 1
fi

# Fix setup.cfg before building
log "Fixing setup.cfg requirements syntax..."
sed -i "s/numpy_groupies >= '0.9.15'/numpy_groupies >=0.9.15/" "${REPO_ROOT}/setup.cfg"

# Fix numpy 2.0 compatibility in xrdtypes.py
log "Updating xrdtypes.py for numpy 2.0 compatibility..."
sed -i "s/np.unicode_/np.str_/" "${REPO_ROOT}/flox/xrdtypes.py"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" \
    pip \
    setuptools \
    "setuptools_scm>=7.0" \
    git \
    compilers \
    pkg-config

  # Install runtime dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "pandas>=2.1" \
    "numpy>=1.26" \
    "numpy_groupies>=0.9.19" \
    "scipy>=1.12" \
    toolz

  # Install optional dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    cachey \
    dask \
    numba \
    numbagg \
    xarray \
    netCDF4

  # Install the package with all extras
  log "Installing package with extras..."
  micromamba run -n "$ENV_NAME" python -m pip install -v -e "${REPO_ROOT}[all,test]"

  # Basic import test
  log "Testing imports..."
  micromamba run -n "$ENV_NAME" python -c "import flox; print(f'flox version: {flox.__version__}')"

  log "Build successful in env: $ENV_NAME"
done

log "All builds completed successfully ✅"
