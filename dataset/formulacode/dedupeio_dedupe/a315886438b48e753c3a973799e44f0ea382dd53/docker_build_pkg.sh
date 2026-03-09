#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

# Define log function
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="dedupe"
  log "Using import name: $IMP"

  # Install build dependencies
  log "Installing build dependencies..."
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=1.20" scipy cython \
    scikit-learn \
    compilers meson-python cmake ninja pkg-config \
    setuptools wheel \
    affinegap categorical-distance doublemetaphone \
    highered simplecosine haversine BTrees zope.index \
    typing_extensions pytest

  # Ensure pip is up to date
  micromamba run -n "$ENV_NAME" python -m pip install --upgrade pip

  # Install additional dependencies that might not be in conda-forge
  micromamba run -n "$ENV_NAME" python -m pip install dedupe_Levenshtein_search

  # Editable install with no build isolation (needed for Cython)
  log "Editable install with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
      warn "Failed to install with --no-build-isolation$EXTRAS, trying without extras"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
  fi

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
