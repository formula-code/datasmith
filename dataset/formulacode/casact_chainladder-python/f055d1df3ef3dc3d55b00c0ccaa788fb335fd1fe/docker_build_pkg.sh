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

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  # Install base dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    numpy scipy cython joblib pytest \
    compilers meson-python cmake ninja pkg-config tomli

  # Install specific dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pandas \
    "scikit-learn>=0.23" \
    "numba>0.54" \
    matplotlib \
    dill \
    patsy

  # Install specific sparse version
  micromamba run -n "$ENV_NAME" pip install "sparse>=0.13.0"

  # Install package in editable mode
  log "Installing package in editable mode"
  micromamba run -n "$ENV_NAME" pip install --no-deps -e "$REPO_ROOT"

  # Smoke test with error handling
  log "Running smoke test"
  if ! micromamba run -n "$ENV_NAME" python -c "import chainladder; print(f'chainladder version: {chainladder.__version__}')"; then
    warn "Smoke test failed for $ENV_NAME"
    # Continue with other versions instead of failing immediately
    continue
  fi

  echo "::import_name=chainladder::env=${ENV_NAME}"
done

log "All builds complete ✅"
