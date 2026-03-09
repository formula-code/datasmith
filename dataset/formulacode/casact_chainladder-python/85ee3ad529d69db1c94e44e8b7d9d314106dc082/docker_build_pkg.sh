#!/usr/bin/env bash
set -euo pipefail

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

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install all dependencies first via micromamba
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip \
    numpy \
    "pandas>=2.0" \
    "scikit-learn>1.4.2" \
    "numba>0.54" \
    "sparse>=0.9" \
    matplotlib \
    dill \
    patsy \
    pytest \
    ipython

  # Install the package itself
  log "Installing chainladder package"
  micromamba run -n "$ENV_NAME" pip install --no-deps -e "$REPO_ROOT"

  # Simple smoke test
  log "Running basic import test"
  micromamba run -n "$ENV_NAME" python -c "import chainladder; print(chainladder.__version__)"

done

log "All builds complete ✅"
