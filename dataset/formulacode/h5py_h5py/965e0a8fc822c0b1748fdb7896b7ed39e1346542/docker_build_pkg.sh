#!/usr/bin/env bash
set -euo pipefail

# Helper functions
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
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
###### END SETUP CODE ######

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="h5py"
  log "Using import name: $IMP"

  # Install build dependencies
  log "Installing build dependencies..."
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=$version" \
    pip git \
    "numpy>=2.0.0" \
    "cython>=0.29.31,<4" \
    "setuptools>=77" \
    pkgconfig \
    compilers cmake ninja pkg-config \
    "hdf5>=1.10.4" \
    pytest

  # Ensure pip is up to date
  micromamba run -n "$ENV_NAME" python -m pip install --upgrade pip wheel

  # Install the package in editable mode
  log "Installing h5py in editable mode..."
  micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"

  # Run smoke tests
  log "Running smoke tests..."
  micromamba run -n "$ENV_NAME" python -c "import h5py; print(h5py.__version__)"

  # Run additional smoke tests if configured
  if [[ -n "${RUN_PYTEST_SMOKE:-}" ]]; then
    log "Running pytest smoke tests..."
    micromamba run -n "$ENV_NAME" pytest -v "$REPO_ROOT/h5py/tests/test_h5py.py" -k "not test_unicode"
  fi

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
