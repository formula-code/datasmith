#!/usr/bin/env bash
set -euo pipefail

# Utility functions
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

# Source common helpers
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="3.11"  # Package requires Python >= 3.11

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Create and activate environment
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        python="$version" \
        pip git conda mamba "libmambapy<=1.9.9" \
        setuptools wheel \
        scipy \
        cloudpickle \
        "sortedcontainers>=2.0" \
        "sortedcollections>=1.1" \
        "loky>=2.9" \
        versioningit

    # Install the package in editable mode
    log "Installing package in editable mode"
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT[test]"

    # Run import check
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import adaptive; print(adaptive.__version__)"

    # Optional: Run basic tests if pytest is available
    if [[ "${RUN_PYTEST_SMOKE:-}" == "1" ]]; then
        log "Running pytest smoke tests"
        micromamba run -n "$ENV_NAME" pytest -v adaptive/tests/test_learner1D.py -k "test_simple"
    fi

    echo "::import_name=adaptive::env=${ENV_NAME}"
done

log "All builds complete ✅"
