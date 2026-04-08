#!/usr/bin/env bash
set -euo pipefail

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

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-shapely}"
  log "Using import name: $IMP"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    numpy cython cmake ninja pkg-config \
    compilers pytest

  # Set up GEOS installation
  export GEOS_VERSION="3.11.1"
  export GEOS_INSTALL="/opt/geos"
  mkdir -p "$GEOS_INSTALL"
  export GEOS_BUILD="/tmp/geosbuild"
  mkdir -p "$GEOS_BUILD"
  cd "$GEOS_BUILD"

  # Download and extract GEOS
  curl -OL "http://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2"
  tar xfj "geos-${GEOS_VERSION}.tar.bz2"
  cd "geos-${GEOS_VERSION}"

  # Build GEOS with modern CMake settings
  mkdir build && cd build
  cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$GEOS_INSTALL" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DBUILD_TESTING=OFF \
    -DCMAKE_POLICY_VERSION=3.5

  cmake --build . -j "$CMAKE_BUILD_PARALLEL_LEVEL"
  cmake --install .

  # Set environment variables for build
  export GEOS_CONFIG="${GEOS_INSTALL}/bin/geos-config"
  export LD_LIBRARY_PATH="${GEOS_INSTALL}/lib:${LD_LIBRARY_PATH:-}"
  export PATH="${GEOS_INSTALL}/bin:${PATH}"

  # Return to repo directory
  cd "$REPO_ROOT"

  # Editable install with build dependencies
  log "Editable install with --no-build-isolation"
  micromamba run -n "$ENV_NAME" pip install --no-build-isolation -v -e .${EXTRAS}

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
