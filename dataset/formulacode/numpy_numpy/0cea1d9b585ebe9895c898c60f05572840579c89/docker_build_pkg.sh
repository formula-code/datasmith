#!/usr/bin/env bash
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Set build parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install system dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" \
    "setuptools<49.2.0" \
    "wheel==0.36.2" \
    "Cython>=0.29.21,<3.0" \
    "pip" \
    "git" \
    "openblas" \
    "liblapack" \
    "compilers" \
    "gcc_linux-64" \
    "gxx_linux-64" \
    "gfortran_linux-64" \
    "pkg-config" \
    "make" \
    "cmake" \
    "ninja"

  # Set compiler environment variables
  export CC="gcc"
  export CXX="g++"
  export FC="gfortran"
  export CFLAGS="-O3 -march=nocona -mtune=haswell"
  export CXXFLAGS="$CFLAGS"
  export FFLAGS="$CFLAGS"
  export LDFLAGS="-Wl,-O1 -Wl,--hash-style=gnu"

  # Enable legacy setuptools behavior
  export SETUPTOOLS_ENABLE_FEATURES="legacy-editable"

  # Set NumPy specific build flags
  export NPY_LAPACK_ORDER="openblas,lapack,atlas"
  export NPY_BLAS_ORDER="openblas,blas,atlas"
  export OPENBLAS_NUM_THREADS=1
  export BLIS_NUM_THREADS=1

  # Clean any previous builds
  cd "$REPO_ROOT"
  rm -rf build/ dist/ *.egg-info/

  # Build and install numpy
  log "Building numpy with --no-deps"
  micromamba run -n "$ENV_NAME" python -m pip install --no-deps --no-build-isolation -v -e .

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" python -c "import numpy; print(numpy.__version__)"

  echo "::import_name=numpy::env=${ENV_NAME}"
done

log "All builds complete ✅"
