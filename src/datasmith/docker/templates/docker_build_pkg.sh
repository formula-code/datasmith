#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

REPO_ROOT=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"
###### END SETUP CODE ######

# =====================================================================
# Agent guidance
# =====================================================================
# GOAL: Install the project in EDITABLE mode into each asv_{version} env.
#
# You may:
# - Add conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (submodules, Cython generation, env vars).
# - Set CFLAGS/CXXFLAGS/LDFLAGS as needed.
# - Modify source files if needed (e.g. fix a missing #include).
# - Modify the external benchmarks section below (URLs, branches, paths).
#
# You must:
# - Keep this script idempotent.
# - Use editable install: `uv pip install -e .` or `pip install -e .`.
# - Not modify the SETUP CODE block above.

# =====================================================================
# External benchmarks (agent-editable)
# =====================================================================
# Some repos store ASV benchmarks in a separate repository.  Clone them
# here so they are baked into the Docker image and available at runtime.
# Modify URLs, branches, or destination paths as needed.
ORIGIN_URL=$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)

if [[ "$ORIGIN_URL" =~ github\.com/dask/dask(\.git)?$ ]]; then
    log "Cloning dask-benchmarks"
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/dask/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/dask/distributed(\.git)?$ ]]; then
    log "Cloning dask-benchmarks (distributed)"
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/distributed/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/joblib/joblib(\.git)?$ ]]; then
    log "Cloning joblib_benchmarks"
    git clone --depth 1 https://github.com/pierreglaser/joblib_benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/astropy/astropy(\.git)?$ ]]; then
    log "Cloning astropy-benchmarks"
    rm -rf "$REPO_ROOT/astropy-benchmarks"
    git clone -b main --depth 1 --single-branch \
        https://github.com/astropy/astropy-benchmarks.git "$REPO_ROOT/astropy-benchmarks"
fi

# =====================================================================
# Build & install across envs
# =====================================================================
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # -----------------------------------------------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------------------------------------------
  # Examples (uncomment if needed for this repo):
  # git -C "$REPO_ROOT" submodule update --init --recursive
  # micromamba install -y -n "$ENV_NAME" -c conda-forge openblas libopenmp
  # export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
  # -----------------------------------------------------------------

  log "Editable install with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
    warn "Failed with extras, retrying without"
    PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete"
