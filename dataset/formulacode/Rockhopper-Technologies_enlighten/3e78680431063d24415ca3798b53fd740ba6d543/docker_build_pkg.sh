#!/usr/bin/env bash
set -euo pipefail

# Helper functions
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

# Load common environment
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    log "No Python version specified, using default 3.10"
    TARGET_VERSIONS="3.10"
fi

# Build & test across environments
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Install build dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        python="$version" \
        pip \
        blessed \
        "prefixed>=0.3.2"

    # Install the package in editable mode
    log "Installing package in editable mode..."
    micromamba run -n "$ENV_NAME" python -m pip install -e "$REPO_ROOT"

    # Basic import test
    log "Testing import..."
    micromamba run -n "$ENV_NAME" python -c "import enlighten; print('enlighten version:', enlighten.__version__)"

    # Record the environment details
    echo "::import_name=enlighten::env=${ENV_NAME}"
done

log "Build completed successfully ✅"
