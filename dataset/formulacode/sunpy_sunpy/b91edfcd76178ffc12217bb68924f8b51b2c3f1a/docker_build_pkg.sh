#!/usr/bin/env bash
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
TARGET_VERSIONS="3.11"  # Python 3.11 required per pyproject.toml
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    log "Error: No Python version specified" >&2
    exit 1
fi

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    IMP="sunpy"  # Known import name from repo facts
    log "Using import name: $IMP"

    # Create fresh environment with base dependencies
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" pip git conda mamba "libmambapy<=1.9.9"

    # Install build dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        setuptools wheel numpy cython \
        compilers meson-python cmake ninja pkg-config \
        extension-helpers

    # Install runtime dependencies via conda
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "astropy>=6.1.0" \
        "numpy>=1.25.0" \
        "packaging>=23.2" \
        "parfive>=2.1.0" \
        "pyerfa>=2.0.1.1" \
        "requests>=2.32.0" \
        "fsspec>=2023.6.0"

    # Install parfive[ftp] via pip to ensure extras are properly handled
    micromamba run -n "$ENV_NAME" python -m pip install "parfive[ftp]>=2.1.0"

    # Install additional dependencies for testing
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        pytest scipy matplotlib

    # Editable install with no build isolation
    log "Installing package in editable mode..."
    PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" \
        python -m pip install -v -e "$REPO_ROOT"$EXTRAS

    # Health checks
    log "Running smoke checks..."
    micromamba run -n "$ENV_NAME" asv_smokecheck.py \
        --import-name "$IMP" \
        --repo-root "$REPO_ROOT" \
        ${RUN_PYTEST_SMOKE:+--pytest-smoke}

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds completed successfully ✅"
