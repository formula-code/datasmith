#!/usr/bin/env bash
# Purpose: Build/install pandas in micromamba envs
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
        "python-dateutil>=2.8.2" \
        "pytz>=2020.1" \
        "versioneer[toml]" \
        wheel \
        compilers pkg-config \
        libstdcxx-ng libgcc-ng

    # Align optional deps with NumPy < 2.0 for this pandas commit
    micromamba run -n "$ENV_NAME" python -m pip uninstall -y \
        numpy scipy numba llvmlite pyarrow tables bottleneck blosc2 numexpr fastparquet \
        pyreadstat matplotlib xarray || true
    micromamba run -n "$ENV_NAME" python -m pip install --upgrade --force-reinstall --no-deps "numpy==1.26.4"
    micromamba run -n "$ENV_NAME" python -m pip install --upgrade --force-reinstall --no-deps "scipy==1.11.4"
    micromamba run -n "$ENV_NAME" python -m pip install --upgrade --force-reinstall --no-deps "matplotlib==3.8.4"

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

    # Build and install with pip - simplified without problematic flags
    log "Building with pip"
    for attempt in {1..3}; do
        if micromamba run -n "$ENV_NAME" python -m pip install --no-deps --no-build-isolation -v -e .; then
            break
        fi
        if [[ $attempt -eq 3 ]]; then
            log "Failed to build pandas after 3 attempts"
            exit 1
        fi
        log "Build attempt $attempt failed, retrying..."
        sleep 5
    done

    # Health checks
    log "Running smoke checks"
    micromamba run -n "$ENV_NAME" python -c "import ${IMP}; print(f'${IMP} version: {${IMP}.__version__}')"

    if [[ -n "${RUN_PYTEST_SMOKE:-}" ]]; then
        micromamba run -n "$ENV_NAME" python -m pytest -v pandas/tests/test_optional.py
    fi

    echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
