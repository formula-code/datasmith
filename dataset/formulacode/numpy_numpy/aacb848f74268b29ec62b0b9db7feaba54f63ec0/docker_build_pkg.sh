#!/usr/bin/env bash
set -euo pipefail

# Source common setup
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

# Define logging function
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*"
}

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

# Set build parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git \
    "cython>=0.29.21,<3.0" \
    "setuptools>=42" \
    compilers pkg-config \
    openblas 'blas=*=openblas' \
    cmake ninja

  # Set compiler flags and environment
  export CFLAGS="-O3 -march=native -fPIC"
  export CXXFLAGS="$CFLAGS"
  export LDFLAGS="-shared"
  export NPY_BLAS_ORDER="openblas"
  export NPY_LAPACK_ORDER="openblas"

  # Build and install
  cd "$REPO_ROOT"
  log "Building numpy from source"
  micromamba run -n "$ENV_NAME" python setup.py build_ext --inplace
  micromamba run -n "$ENV_NAME" python setup.py install

  # Verify installation
  log "Verifying numpy installation"
  micromamba run -n "$ENV_NAME" python -c "import numpy; print('NumPy version:', numpy.__version__)"

  # Run basic test
  log "Running basic numpy test"
  micromamba run -n "$ENV_NAME" python -c "import numpy; numpy.test(verbose=2, raise_warnings='error')" || true

  echo "::import_name=numpy::env=${ENV_NAME}"
done

log "All builds completed successfully ✅"
