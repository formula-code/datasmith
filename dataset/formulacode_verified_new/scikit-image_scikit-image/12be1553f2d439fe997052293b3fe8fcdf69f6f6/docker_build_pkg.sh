#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

# Define log function
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.11}}"
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

  IMP="${IMPORT_NAME:-skimage}"
  log "Using import name: $IMP"

  # Install build dependencies first
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=$version" \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=2.0" \
    "scipy>=1.11.4" \
    "cython>=3.0.8" \
    "pythran>=0.16" \
    "meson-python>=0.16" \
    "networkx>=3.0" \
    "pillow>=10.1" \
    "imageio>=2.33" \
    "tifffile>=2022.8.12" \
    "packaging>=21" \
    "lazy-loader>=0.4" \
    cmake ninja pkg-config \
    pytest compilers

  # Set build environment variables
  export OPENBLAS_NUM_THREADS=1
  export MKL_NUM_THREADS=1
  export OMP_NUM_THREADS=1

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
      warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete "