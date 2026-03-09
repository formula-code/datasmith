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

  # Set import name explicitly for chainladder
  IMP="chainladder"
  log "Using import name: $IMP"

  # Install base dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge pip git conda mamba "libmambapy<=1.9.9" \
    numpy scipy cython joblib pytest compilers cmake ninja pkg-config

  # Install specific dependencies required by chainladder
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "scikit-learn>1.4.2" \
    "pandas>=2.0" \
    "numba>0.54" \
    "sparse>=0.9" \
    matplotlib \
    dill \
    patsy

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT"

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
