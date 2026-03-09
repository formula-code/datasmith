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
TARGET_VERSIONS="${PY_VERSION:-3.9 3.10 3.11 3.12}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    echo "Error: No Python versions specified" >&2
    exit 1
fi

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    IMP="${IMPORT_NAME:-param}"
    log "Using import name: $IMP"

    # Create fresh environment
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip pytest \
        hatchling hatch-vcs

    # Editable install with build dependencies in place
    log "Installing in editable mode"
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"

    # Basic import test
    log "Running smoke test"
    micromamba run -n "$ENV_NAME" python -c "import $IMP; print($IMP.__version__)"

    # Run basic pytest if available
    if [[ "${RUN_PYTEST_SMOKE:-}" == "1" ]]; then
        log "Running pytest smoke test"
        micromamba run -n "$ENV_NAME" pip install "pytest>=7.0"
        micromamba run -n "$ENV_NAME" pytest -v "$REPO_ROOT/tests" -k "not test_serialization"
    fi

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
