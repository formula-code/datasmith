#!/usr/bin/env bash
set -euo pipefail

# Load common helpers
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"

# Conservative parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  echo "==> Building in env: $ENV_NAME (python=$version)"

  # Clean existing numpy if present
  micromamba run -n "$ENV_NAME" python -m pip uninstall -y numpy scipy || true

  # Install build dependencies with pinned numpy
  micromamba install -y -n "$ENV_NAME" \
    python="${version}" \
    pip git \
    "meson==1.2.1" \
    "meson-python==0.13.1" \
    "Cython~=3.0.5" \
    "numpy=1.26.4" \
    python-dateutil \
    pytz \
    tzdata \
    wheel \
    ninja \
    pkg-config \
    compilers \
    pytest \
    "versioneer[toml]"

  # Set environment variables
  export OPENBLAS_NUM_THREADS=1
  export MKL_NUM_THREADS=1
  export OMP_NUM_THREADS=1

  # Build and install
  cd "$REPO_ROOT"
  rm -rf build/ dist/ .mesonpy_build/

  # Install build dependencies first
  micromamba run -n "$ENV_NAME" python -m pip install --no-deps "versioneer[toml]"

  # Build and install using meson-python
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e .

  # Health check
  echo "Running import check"
  micromamba run -n "$ENV_NAME" python -c "import pandas; print(pandas.__version__)"

  # Run basic tests if pytest is available
  if micromamba run -n "$ENV_NAME" python -c "import pytest" 2>/dev/null; then
    echo "Running minimal test suite"
    cd "$REPO_ROOT"
    micromamba run -n "$ENV_NAME" python -c "import pandas; pandas.test(extra_args=['-m not slow and not network and not db', '--skip-slow', '--skip-network', '--skip-db', '-n auto'])" || true
  fi

  echo "::import_name=pandas::env=${ENV_NAME}"
done

echo "All builds complete ✅"
