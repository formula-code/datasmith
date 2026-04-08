#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

REPO_ROOT=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
###### END SETUP CODE ######

# -----------------------------
# Agent guidance (read-first)
# -----------------------------
# GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
#       preferably with no build isolation, then run health checks.
#
# Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# - Add extra conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# - Run arbitrary micromamba/pip commands in the target env.
# - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo. (e.g. Add -Wno-error=incompatible-pointer-types to CFLAGS)
# - Change files in the repo if needed (e.g., fix a missing #include).
# - Anything else needed to get a successful editable install.
#
# MUST:
# - Keep this script idempotent.
# - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# - Do not modify the SETUP CODE or helper functions below.
# - Critically: Prepare for quick verification steps that will run after build:
#   - A lightweight profiling sanity check should be able to start without immediate error.
#   - A lightweight pytest sanity check should be able to start without immediate error.
#     * Some projects (e.g., SciPy) require running pytest from a subdir (e.g., `scipy/`).
#   - Install minimal test/benchmark extras or dev requirements so that import and basic pytest discovery succeed.
#   - Prefer fast readiness (lightweight deps) over exhaustive installs.
#
# DO NOT:
# - Change env names or Python versions outside MODEL EDIT AREA.
# - Use build isolation unless absolutely necessary.

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # -----------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------
  # Examples (uncomment if needed for this repo):
  # log "Updating submodules"
  # git -C "$REPO_ROOT" submodule update --init --recursive
  #
  # log "Installing extra system libs via conda-forge"
  # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
  #
  # log "Pre-generating Cython sources"
  # micromamba run -n "$ENV_NAME" python -m cython --version
  #
  # export CFLAGS="${CFLAGS:-}"
  # export CXXFLAGS="${CXXFLAGS:-}"
  # export LDFLAGS="${LDFLAGS:-}"
  # -----------------------------

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  # $EXTRAS is an optional argument to install all discovered extra dependencies.
  # It will be empty if pyproject.toml does not exist or has no [project.optional-dependencies].
  # In case setup.py is used, no need to append $EXTRAS.
  log "Editable install with --no-build-isolation"
  # try with $EXTRAS first, then without if it fails
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
    warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"
    PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
