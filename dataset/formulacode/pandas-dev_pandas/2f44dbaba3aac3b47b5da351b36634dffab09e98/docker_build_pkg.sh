#!/usr/bin/env bash
# Purpose: Build/install pandas using setuptools in micromamba envs
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
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.8}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

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

    IMP="${IMPORT_NAME:-pandas}"
    log "Using import name: $IMP"

    # Install runtime and build dependencies via conda-forge
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip git \
        "setuptools>=61.0.0" \
        "cython>=0.29.32,<3" \
        "numpy>=1.21.0" \
        "python-dateutil>=2.8.2" \
        "pytz>=2020.1" \
        "versioneer[toml]" \
        wheel

    # Initialize git repo for versioneer
    cd "$REPO_ROOT"
    if [[ ! -d .git ]]; then
        git init
        git add .
        git config --global user.email "builder@example.com"
        git config --global user.name "Builder"
        git commit -m "Initial commit"
    fi

    # Install oldest-supported-numpy but ensure it doesn't downgrade numpy
    micromamba run -n "$ENV_NAME" python -m pip install --no-deps "oldest-supported-numpy>=2022.8.16"

    # Verify numpy version meets minimum requirements
    micromamba run -n "$ENV_NAME" python -c "import numpy; assert numpy.lib.NumpyVersion(numpy.__version__) >= numpy.lib.NumpyVersion('1.20.3'), f'numpy version {numpy.__version__} is too old'"

    # Build and install pandas
    log "Building pandas"
    micromamba run -n "$ENV_NAME" python -m pip install --no-deps --no-build-isolation -v -e .

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import ${IMP}; print(f'${IMP} version: {${IMP}.__version__}')"

    if [[ -n "${RUN_PYTEST_SMOKE:-}" ]]; then
        micromamba run -n "$ENV_NAME" python -m pytest -v pandas/tests/test_optional.py
    fi

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
