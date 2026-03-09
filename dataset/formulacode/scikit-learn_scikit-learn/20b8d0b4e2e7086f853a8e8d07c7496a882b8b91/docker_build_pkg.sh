#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Agent-added modifications
# -----------------------------
# Ensure 'tests' optional dependencies are installed for robust smoke checks.
ALL_EXTRAS="tests"

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "[1;34m[build][0m %s\n" "$*"; }
warn() { printf "[1;33m[warn][0m %s\n" "$*" >&2; }
die()  { printf "[1;31m[fail][0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
  fi

  # Import name resolution (kept simple for the agent)
  IMP="$(detect_import_name || true)"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # Update pip, setuptools, wheel first for a robust environment
  log "Updating pip, setuptools, and wheel"
  micromamba run -n "$ENV_NAME" python -m pip install -U pip setuptools wheel

  # Install all build and core runtime dependencies via micromamba
  # This avoids conflicting pip installs/downgrades later.
  log "Installing build and core runtime dependencies via micromamba"
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    numpy scipy joblib threadpoolctl pytest \
    compilers cmake ninja pkg-config \
    "cython>=3.1.2" meson-python

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  # Calculate EXTRAS again to pick up the ALL_EXTRAS override
  EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
      warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  # Machine-readable markers
  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
