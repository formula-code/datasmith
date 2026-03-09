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
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"
EXTRAS="${ALL_EXTRAS:+[tests]}"

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

    IMP="${IMPORT_NAME:-param}"
    log "Using import name: $IMP"

    # Install build dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip git \
        hatchling hatch-vcs \
        pytest pytest-asyncio pytest-cov

    # Editable install
    log "Installing package in editable mode"
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"$EXTRAS

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
