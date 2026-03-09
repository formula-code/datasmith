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
    TARGET_VERSIONS="3.10"  # Default if not specified
fi
###### END SETUP CODE ######

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Install build dependencies with numpy<2.0
    log "Installing build dependencies..."
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        python="$version" \
        "numpy<2.0" \
        pip \
        setuptools \
        wheel \
        cython \
        scikit-learn \
        pytest \
        coverage \
        git

    # Install additional dependencies
    log "Installing additional dependencies..."
    micromamba run -n "$ENV_NAME" python -m pip install \
        "affinegap>=1.3" \
        "categorical-distance>=1.9" \
        "doublemetaphone" \
        "highered>=0.2.0" \
        "simplecosine>=1.2" \
        "haversine>=0.4.1" \
        "BTrees>=4.1.4" \
        "zope.index" \
        "dedupe_Levenshtein_search" \
        "typing_extensions"

    # Install package in editable mode
    log "Installing package in editable mode..."
    cd "$REPO_ROOT"
    micromamba run -n "$ENV_NAME" python -m pip install -e .

    # Run basic import test
    log "Testing import..."
    micromamba run -n "$ENV_NAME" python -c "import dedupe"

    log "Build successful for Python $version"
done

log "All builds completed successfully ✅"
