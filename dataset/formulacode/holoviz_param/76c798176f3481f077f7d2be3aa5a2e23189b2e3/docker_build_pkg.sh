#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
    TARGET_VERSIONS="3.10"  # Default to 3.10 if no version specified
fi
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Create and activate environment
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        "python=$version" pip git \
        hatchling hatch-vcs \
        pytest pytest-asyncio \
        numpy pandas ipython

    # Install the package in editable mode
    log "Installing package in editable mode..."
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"

    # Install test dependencies
    log "Installing test dependencies..."
    micromamba run -n "$ENV_NAME" pip install pytest numpy

    # Basic import test
    log "Running basic import test..."
    micromamba run -n "$ENV_NAME" python -c "import param; print(param.__version__)"

    # Run a minimal test to verify installation
    if [[ -d "$REPO_ROOT/tests" ]]; then
        log "Running minimal test suite..."
        micromamba run -n "$ENV_NAME" pytest "$REPO_ROOT/tests/testversion.py" -v
    fi

    echo "::import_name=param::env=$ENV_NAME"
done

log "All builds completed successfully "
