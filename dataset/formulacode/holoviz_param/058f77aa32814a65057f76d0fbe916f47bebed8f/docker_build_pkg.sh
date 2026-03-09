#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[build] $*" >&2
}

# Source common helpers if they exist
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.9}}"

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Create and activate environment
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" pip \
        hatchling hatch-vcs

    # Install the package in editable mode
    log "Installing param in editable mode..."
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"

    # Basic import test
    log "Testing import..."
    micromamba run -n "$ENV_NAME" python -c "import param; print(param.__version__)"

    # Install test dependencies if needed
    if [[ "${RUN_TESTS:-}" == "1" ]]; then
        log "Installing test dependencies..."
        micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT[tests]"

        log "Running tests..."
        micromamba run -n "$ENV_NAME" pytest tests/
    fi

    echo "::import_name=param::env=${ENV_NAME}"
done

log "All builds complete ✅"
