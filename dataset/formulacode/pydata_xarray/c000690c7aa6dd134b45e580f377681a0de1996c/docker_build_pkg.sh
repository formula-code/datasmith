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

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-xarray}"
  log "Using import name: $IMP"

  # Install core build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge pip git conda mamba "libmambapy<=1.9.9" \
        numpy scipy cython joblib fakeredis threadpoolctl pytest \
        compilers meson-python cmake ninja pkg-config tomli

  # Install core package dependencies
  log "Installing core package dependencies with numpy<2.0"
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "numpy<2.0" "packaging>=23.2" "pandas>=2.1"

  # Editable install without extras to avoid parsing issues
  log "Performing editable install"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Install basic optional dependencies that are commonly needed
  log "Installing basic optional dependencies"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v \
        "scipy" "bottleneck" "numbagg" "numba>=0.54" "flox" "opt_einsum"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
