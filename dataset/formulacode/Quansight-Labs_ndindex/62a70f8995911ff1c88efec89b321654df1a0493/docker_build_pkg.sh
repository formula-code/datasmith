#!/usr/bin/env bash
set -euo pipefail

# Helper function
log() {
    echo "[build] $*" >&2
}

# Source helper scripts if they exist
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="$version" \
    pip \
    cython \
    numpy \
    setuptools \
    versioneer \
    pytest \
    hypothesis \
    matplotlib \
    packaging \
    pyflakes \
    pytest-cov \
    pytest-doctestplus \
    pytest-xdist \
    pytest-flakes \
    scikit-image \
    sympy \
    compilers

  # Build and install in development mode
  log "Installing in editable mode with extras"
  micromamba run -n "$ENV_NAME" python -m pip install -e "$REPO_ROOT[arrays]"

  # Verify import works
  log "Verifying installation"
  micromamba run -n "$ENV_NAME" python -c "import ndindex; print(ndindex.__version__)"

  # Run basic tests without coverage
  log "Running basic tests"
  micromamba run -n "$ENV_NAME" python -m pytest "$REPO_ROOT/ndindex/tests/test_ndindex.py" -v --no-cov

  echo "::import_name=ndindex::env=${ENV_NAME}"
done

log "All builds complete ✅"
