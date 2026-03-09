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

  # Force correct import name
  IMP="sourmash"
  log "Using import name: $IMP"

  # Install clang and Rust toolchain
  log "Installing clang and Rust toolchain"
  apt-get update && apt-get install -y libclang-dev
  micromamba install -y -n "$ENV_NAME" -c conda-forge maturin rust

  # Editable install with build isolation (required for maturin)
  log "Editable install with maturin"
  micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"$EXTRAS

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
