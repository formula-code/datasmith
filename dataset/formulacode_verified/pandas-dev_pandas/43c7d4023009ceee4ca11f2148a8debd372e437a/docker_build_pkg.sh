#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

# Load micromamba environment
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"

# Set build parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install core dependencies first
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
    pytest-xdist \
    pytest-cov \
    hypothesis \
    "versioneer[toml]" \
    scipy \
    matplotlib \
    numexpr \
    bottleneck \
    lxml \
    openpyxl \
    xlsxwriter \
    sqlalchemy \
    pyarrow \
    fastparquet \
    pandas-gbq \
    tabulate \
    tzlocal \
    psycopg2 \
    pyreadstat \
    s3fs \
    beautifulsoup4 \
    html5lib \
    numba \
    pytables

  # Set environment variables
  export OPENBLAS_NUM_THREADS=1
  export MKL_NUM_THREADS=1
  export OMP_NUM_THREADS=1
  export PYTHONPATH="${REPO_ROOT}"
  export SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0"

  # Clean any previous builds
  cd "$REPO_ROOT"
  rm -rf build/ dist/ *.egg-info/
  find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

  # Build and install using meson-python
  log "Building pandas with meson-python"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e .[test]

  # Verify installation by importing pandas and checking version
  log "Running import check"
  timeout 30 micromamba run -n "$ENV_NAME" python -c "
import sys
import pandas as pd
print(f'Successfully imported pandas version: {pd.__version__}')
sys.exit(0)
" || {
    echo "Import check failed or timed out"
    exit 1
}

  # Run basic test
  log "Running minimal test suite"
  cd "$REPO_ROOT"
  timeout 60 micromamba run -n "$ENV_NAME" python -c "
import pandas as pd
pd.test(extra_args=['-m not slow and not network and not db', '--skip-slow', '--skip-network', '--skip-db'])
" || true

  echo "::import_name=pandas::env=${ENV_NAME}"
done

log "All builds complete ✅"
