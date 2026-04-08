#!/usr/bin/env bash
set -euo pipefail

# Define log function
log() {
    echo "[build] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
    echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
    exit 1
fi
###### END SETUP CODE ######

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    IMP="${IMPORT_NAME:-param}"
    log "Using import name: $IMP"

    # Install build dependencies and test requirements
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        python="$version" \
        pip git \
        hatchling hatch-vcs \
        pytest pytest-asyncio pytest-cov

    # Editable install with test dependencies
    log "Installing package with test dependencies"
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT[tests]"

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
