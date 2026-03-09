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

# Conservative default parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install build dependencies with exact versions
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="${version}" \
    pip git \
    "meson==1.2.1" \
    "meson-python==0.13.1" \
    "Cython==3.0.5" \
    "numpy>=1.22.4,<2" \
    python-dateutil \
    pytz \
    tzdata \
    wheel \
    ninja \
    pkg-config \
    compilers \
    pytest \
    versioneer

  # Set environment variables
  export OPENBLAS_NUM_THREADS=1
  export MKL_NUM_THREADS=1
  export OMP_NUM_THREADS=1
  export PYTHONPATH="${REPO_ROOT}"
  export SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0"

  # Build and install
  log "Building pandas with meson-python"
  cd "$REPO_ROOT"

  # Clean any previous builds
  rm -rf build/ dist/ *.egg-info
  find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

  # Initialize git repo if needed (for versioneer)
  if [[ ! -d .git ]]; then
    git init
    git add .
    git config --global user.email "build@example.com"
    git config --global user.name "Build Script"
    git commit -m "Initial commit"
  fi

  # Build and install using meson-python
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e .

  # Health check
  log "Running import check"
  micromamba run -n "$ENV_NAME" python -c "import pandas; print(pandas.__version__)"

  echo "::import_name=pandas::env=${ENV_NAME}"
done

log "All builds complete ✅"
