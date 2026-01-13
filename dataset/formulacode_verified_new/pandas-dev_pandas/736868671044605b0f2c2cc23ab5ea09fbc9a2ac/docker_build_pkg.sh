#!/usr/bin/env bash
# Purpose: Build/install pandas using meson-python in ASV micromamba envs
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# Set build parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Clean any previous builds
  cd "$REPO_ROOT"
  rm -rf build/ dist/ *.egg-info/

  # Install exact build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="${version}" \
    pip \
    git \
    "meson==1.2.1" \
    "meson-python==0.13.1" \
    "Cython==3.0.5" \
    "numpy>=1.22.4,<2.0.0" \
    "python-dateutil>=2.8.2" \
    "pytz>=2020.1" \
    "tzdata>=2022.7" \
    wheel \
    ninja \
    pkg-config \
    compilers \
    pytest \
    "versioneer[toml]"

  # Install test dependencies
  log "Installing test dependencies"
  micromamba run -n "$ENV_NAME" python -m pip install \
    "hypothesis>=6.46.1" \
    "pytest>=7.3.2" \
    "pytest-xdist>=2.2.0"

  # Set environment variables
  export OPENBLAS_NUM_THREADS=1
  export MKL_NUM_THREADS=1
  export OMP_NUM_THREADS=1
  export PYTHONPATH="${REPO_ROOT}"
  export SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0"

  # Build and install
  log "Building pandas with meson-python"
  cd "$REPO_ROOT"

  # Install build dependencies first
  micromamba run -n "$ENV_NAME" python -m pip install --no-deps "versioneer[toml]"

  # Build and install using meson-python with specific error handling
  if ! micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e .; then
    log "Build failed, retrying with debug output..."
    export PYTHONVERBOSE=1
    export MESONPY_DEBUG=1
    micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e .
  fi

  # Health check
  log "Running import check"
  micromamba run -n "$ENV_NAME" python -c "import pandas; print(pandas.__version__)"

  # Run basic test if pytest is available
  if micromamba run -n "$ENV_NAME" python -c "import pytest" 2>/dev/null; then
    log "Running minimal test suite"
    cd "$REPO_ROOT"
    micromamba run -n "$ENV_NAME" python -c "import pandas; pandas.test(extra_args=['-m not slow and not network and not db', '--skip-slow', '--skip-network', '--skip-db', '-n auto'])" || true
  fi

  echo "::import_name=pandas::env=${ENV_NAME}"
done

log "All builds complete ✅"