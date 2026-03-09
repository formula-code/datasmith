#!/usr/bin/env bash
# Purpose: Build/install pandas in micromamba envs
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.8}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
    exit 1
fi

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    IMP="${IMPORT_NAME:-pandas}"
    log "Using import name: $IMP"

    # Install build dependencies via conda-forge
    # Note: This pandas version (Jan 2023) requires numpy <2.0 for binary compatibility
    # We override the env_payload versions which may be too new

    # Remove scipy first if it exists (it may be built for numpy 2.x)
    micromamba remove -y -n "$ENV_NAME" scipy 2>/dev/null || true

    # Install numpy and cython
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "numpy>=1.21.6,<2.0" \
        "cython>=0.29.33,<3"

    # Install scipy compatible with numpy 1.x
    micromamba install -y -n "$ENV_NAME" -c conda-forge "scipy>=1.7,<1.12"

    # Initialize git repo for versioneer
    cd "$REPO_ROOT"
    if [[ ! -d .git ]]; then
        git init
        git add .
        git config --global user.email "builder@example.com"
        git config --global user.name "Builder"
        git commit -m "Initial commit"
    fi

    # Build and install
    log "Building pandas"
    micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation --no-deps -v -e .
      # Ensure NumPy/SciPy ABI consistency for downstream test imports.
      micromamba remove -y -n "$ENV_NAME" numpy scipy || true
      micromamba install -y -n "$ENV_NAME" -c conda-forge "numpy<2" "scipy<1.12"


    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import numpy; print(f'numpy version: {numpy.__version__}')"
    micromamba run -n "$ENV_NAME" python -c "import ${IMP}; print(f'${IMP} version: {${IMP}.__version__}')"

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
