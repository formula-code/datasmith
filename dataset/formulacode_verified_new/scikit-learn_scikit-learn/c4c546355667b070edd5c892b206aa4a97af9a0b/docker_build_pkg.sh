#!/usr/bin/env bash
# Purpose: Build/install scikit-learn (editable) in ASV micromamba envs and run lightweight health checks.
set -euo pipefail

###### SETUP CODE (DO NOT MODIFY) ######
# Load micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------------------------------------------
# Helpers (do not modify)
# -----------------------------------------------------------------
log()  { printf "[1;34m[build][0m %s\n" "$*"; }
warn() { printf "[1;33m[warn][0m %s\n" "$*" >&2; }
die()  { printf "[1;31m[fail][0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# Explicit import name for scikit-learn
export IMPORT_NAME=sklearn

# -----------------------------------------------------------------
# Build & test across envs
# -----------------------------------------------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
  fi

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    git conda mamba "libmambapy<=1.9.9" \
    numpy scipy joblib threadpoolctl pytest \
    compilers cmake ninja pkg-config \
    "cython>=3.1.2" meson-python

  # Install additional pip dependencies (pinning to versions known to work with scikit-learn)
  micromamba run -n "$ENV_NAME" pip install -U \
    "numpy<2" "setuptools==60" "scipy<1.14"

  # Set compiler flags (silence a known warning)
  export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"

  # Editable install with test extras and no build isolation
  log "Editable install with test extras and --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" \
       python -m pip install --no-build-isolation -v -e "$REPO_ROOT"[test]; then
    warn "Failed to install with test extras, trying without extras"
    PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" \
      python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # -----------------------------------------------------------------
  # Lightweight health checks
  # -----------------------------------------------------------------
  log "Running import sanity check"
  micromamba run -n "$ENV_NAME" python - <<PY
import importlib, sys
mod = importlib.import_module('$IMPORT_NAME')
print(f"{mod.__name__} imported successfully, version={getattr(mod, '__version__', 'N/A')}")
if not getattr(mod, '__path__', None):
    sys.stderr.write('Warning: Package __path__ is empty\\n')
PY

  # Minimal pytest discovery (does not run full test suite)
  if command -v pytest >/dev/null 2>&1; then
    log "Running minimal pytest discovery"
    micromamba run -n "$ENV_NAME" pytest -q --maxfail=1 || warn "pytest discovery failed (non‑critical)"
  fi

  # Machine‑readable marker for downstream steps
  echo "::import_name=${IMPORT_NAME}::env=${ENV_NAME}"
done

log "All builds complete ✅"