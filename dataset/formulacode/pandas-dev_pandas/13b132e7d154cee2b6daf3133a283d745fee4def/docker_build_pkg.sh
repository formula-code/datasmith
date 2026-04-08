#!/usr/bin/env bash
# Purpose: Build/install pandas using meson build system in micromamba envs
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

    # Install build dependencies via conda-forge
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip git conda mamba "libmambapy<=1.9.9" \
        "meson-python==0.13.1" \
        "meson==1.2.1" \
        "ninja" \
        "cython==0.29.33" \
        "numpy>=1.22.4,<2" \
        "python-dateutil>=2.8.2" \
        "pytz>=2020.1" \
        "versioneer[toml]" \
        "wheel" \
        compilers pkg-config \
        pytest hypothesis

    # Set compiler environment variables
    export CC=$(micromamba run -n "$ENV_NAME" which gcc)
    export CXX=$(micromamba run -n "$ENV_NAME" which g++)

    # Initialize git repo for versioneer
    cd "$REPO_ROOT"
    if [[ ! -d .git ]]; then
        git init
        git add .
        git config --global user.email "builder@example.com"
        git config --global user.name "Builder"
        git commit -m "Initial commit"
    fi

    # Generate version info before build
    micromamba run -n "$ENV_NAME" python generate_version.py --print || true

    # Install numpy first to ensure headers are available
    micromamba run -n "$ENV_NAME" python -m pip install --no-deps "numpy>=1.22.4,<2"

    # Build and install with meson
    log "Building with meson-python"
    micromamba run -n "$ENV_NAME" python -m pip install --no-deps --no-build-isolation -v -e .

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import pandas; print(f'pandas version: {pandas.__version__}')"

    if [[ -n "${RUN_PYTEST_SMOKE:-}" ]]; then
        micromamba run -n "$ENV_NAME" python -m pytest -v pandas/tests/test_optional.py
    fi

    echo "::import_name=pandas::env=${ENV_NAME}"
done

log "All builds complete ✅"
