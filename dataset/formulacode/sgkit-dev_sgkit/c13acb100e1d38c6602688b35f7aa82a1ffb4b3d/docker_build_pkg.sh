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

  if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
  fi

  IMP="$(detect_import_name || true)"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # Install build dependencies with version pins
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=1.21,<1.22" "scipy>=1.7,<1.8" cython joblib fakeredis threadpoolctl pytest \
    compilers meson-python cmake ninja pkg-config tomli \
    partd pandas "xarray>=2022.3,<2022.4" dask distributed dask-ml zarr numba typing-extensions fsspec

  # First install the base package without extras
  log "Installing base package without extras"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Install extras one by one
  log "Installing extras separately"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v "$REPO_ROOT[plink]"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v "$REPO_ROOT[vcf]"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v "$REPO_ROOT[bgen]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
