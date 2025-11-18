#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

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

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# Increase timeout for network operations
export PIP_DEFAULT_TIMEOUT=120
export PIP_CONNECT_TIMEOUT=120

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # Install build dependencies with retries
  for i in {1..3}; do
    if micromamba install -y -n "$ENV_NAME" -c conda-forge pip git conda mamba "libmambapy<=1.9.9" \
          numpy scipy cython joblib fakeredis threadpoolctl pytest \
          compilers meson-python cmake ninja pkg-config tomli versioneer; then
      break
    else
      warn "Install attempt $i failed, retrying..."
      sleep 5
    fi
  done

  # Generate version info before editable install
  log "Generating version info"
  micromamba run -n "$ENV_NAME" python generate_version.py -o pandas/_version.py

  # Editable install with no build isolation and minimal dependencies
  log "Editable install with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"; then
      warn "Failed to install with --no-build-isolation, trying with --no-deps"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation --no-deps -v -e "$REPO_ROOT"
  fi

  # Basic health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" python -c "import pandas; print(f'Successfully imported pandas version {pandas.__version__}')"

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
