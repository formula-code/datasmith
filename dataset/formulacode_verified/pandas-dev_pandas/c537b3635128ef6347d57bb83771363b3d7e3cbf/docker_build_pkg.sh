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

    # Install runtime and build dependencies via conda-forge
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip git \
        "setuptools>=61.0.0" \
        "wheel" \
        "Cython>=0.29.33,<3" \
        "numpy=1.23.5" \
        "python-dateutil>=2.8.2" \
        "pytz>=2020.1" \
        "tzdata>=2022.1" \
        "versioneer[toml]" \
        compilers pkg-config

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
    micromamba run -n "$ENV_NAME" python -m pip install --no-deps -v -e .

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import numpy as np; import ${IMP}; print(f'numpy version: {np.__version__}'); print(f'${IMP} version: {${IMP}.__version__}')"

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
