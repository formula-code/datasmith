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
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.11}}"
EXTRAS="${ALL_EXTRAS:+[test]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
    exit 1
fi
###### END SETUP CODE ######

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Install build dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=$version" \
        pip \
        "hatchling>=1.8.0" \
        hatch-vcs \
        pytest \
        mypy \
        mypy_extensions \
        pydantic \
        wrapt \
        numpy \
        dask \
        attrs \
        pyinstaller \
        pytest-cov \
        msgspec \
        toolz

    # Install the package in development mode
    log "Installing package in development mode..."
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"$EXTRAS

    # Run basic import test
    log "Testing import..."
    micromamba run -n "$ENV_NAME" python -c "import psygnal; print(psygnal.__file__)"

    # Run tests if test extra was installed
    if [[ "$EXTRAS" == *"test"* ]]; then
        log "Running tests..."
        micromamba run -n "$ENV_NAME" pytest "$REPO_ROOT/tests" -v || true
    fi

    echo "::import_name=psygnal::env=${ENV_NAME}"
done

log "All builds complete ✅"
