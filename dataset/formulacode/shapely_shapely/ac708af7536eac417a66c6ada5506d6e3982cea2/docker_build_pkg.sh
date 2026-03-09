#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

# Define log function
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

# Conservative default parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Default import name from repo facts
  IMP="shapely"
  log "Using import name: $IMP"

  # Install build dependencies including GEOS
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=1.21" "cython>=3.0" \
    pytest pytest-cov \
    compilers cmake ninja pkg-config \
    geos>=3.9

  # Set GEOS environment variables
  export GEOS_CONFIG=$(micromamba run -n "$ENV_NAME" which geos-config)
  export GEOS_INCLUDE_PATH=$(micromamba run -n "$ENV_NAME" geos-config --includes)
  export GEOS_LIBRARY_PATH=$(micromamba run -n "$ENV_NAME" geos-config --libs)

  # Editable install with no build isolation
  log "Installing package in editable mode..."
  micromamba run -n "$ENV_NAME" python -m pip install -v -e ".[test]"

  # Health checks
  log "Running smoke checks..."
  micromamba run -n "$ENV_NAME" python -c "import ${IMP}; print('Successfully imported ${IMP}')"

  if [[ "${RUN_PYTEST_SMOKE:-}" == "1" ]]; then
    log "Running pytest smoke tests..."
    micromamba run -n "$ENV_NAME" pytest --pyargs "${IMP}.tests" -v
  fi

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
