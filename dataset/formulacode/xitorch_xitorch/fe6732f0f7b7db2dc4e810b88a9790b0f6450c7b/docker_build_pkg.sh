#!/usr/bin/env bash
set -euo pipefail

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="3.9"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="xitorch"
  log "Using import name: $IMP"

  # Create fresh environment
  micromamba create -y -n "$ENV_NAME" -c conda-forge "python=$version"

  # Install base build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git \
    "numpy>=1.8.2" \
    "scipy>=1.1.0" \
    pytest \
    compilers cmake ninja pkg-config

  # Install PyTorch 1.9 CPU version from pytorch channel
  micromamba install -y -n "$ENV_NAME" -c pytorch -c conda-forge "pytorch=1.9.*=*cpu*"

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" python -c "import $IMP; print($IMP.__version__)"

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
