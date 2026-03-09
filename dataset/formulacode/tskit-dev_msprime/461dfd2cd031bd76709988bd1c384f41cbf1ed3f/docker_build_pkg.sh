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

    # Initialize git submodules
    log "Updating submodules"
    git -C "$REPO_ROOT" submodule update --init --recursive

    # Install build dependencies
    log "Installing build dependencies"
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=$version" \
        pip setuptools wheel \
        "numpy>=2" \
        setuptools_scm \
        compilers pkg-config \
        gsl \
        "tskit>=0.5.2" \
        demes \
        "stdpopsim==0.1.2"

    # Build and install in development mode
    log "Installing package in development mode"
    micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"

    # Verify import works
    log "Verifying import"
    micromamba run -n "$ENV_NAME" python -c "import msprime; print(msprime.__version__)"

    echo "::import_name=msprime::env=${ENV_NAME}"
done

log "All builds completed successfully ✅"
