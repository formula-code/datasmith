#!/usr/bin/env bash
# Purpose: Build/install the repo in one or more ASV micromamba envs, then run health checks.
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
###### END SETUP CODE ######

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-pandas}"
  log "Using import name: $IMP"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge pip git conda mamba "libmambapy<=1.9.9" \
        numpy scipy cython joblib fakeredis threadpoolctl pytest versioneer \
        compilers meson-python cmake ninja pkg-config tomli

  # Generate version info first
  log "Generating version information"
  micromamba run -n "$ENV_NAME" python "$REPO_ROOT/generate_version.py" --outfile "$REPO_ROOT/_version_meson.py"

  # Build with meson
  log "Running meson setup"
  micromamba run -n "$ENV_NAME" meson setup "$REPO_ROOT" "$REPO_ROOT/build/cp310" -Dbuildtype=release -Db_ndebug=if-release -Db_vscrt=md

  # Regular install instead of editable
  log "Installing package"
  micromamba run -n "$ENV_NAME" python -m pip install -v "$REPO_ROOT"$EXTRAS

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"