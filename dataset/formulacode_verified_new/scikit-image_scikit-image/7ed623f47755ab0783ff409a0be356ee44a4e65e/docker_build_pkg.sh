#!/usr/bin/env bash
# Purpose: Build/install scikit-image in ASV micromamba environments and run health checks.
# This script pins NumPy to a compatible version and forces an older setuptools (<60) to avoid
# distutils import errors on Linux.
set -euo pipefail

###### SETUP CODE (DO NOT MODIFY) ######
# Load micromamba utilities and persisted build variables.
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
# Helper functions (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative parallelism (override if needed)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across environments
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # Install build and runtime dependencies.
  # Pin NumPy to <1.24 and setuptools to <60 to avoid distutils import issues.
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
        python="${version}" \
        pip git conda mamba "libmambapy<=1.9.9" \
        "numpy==1.23.5" "scipy==1.9.3" \
        "setuptools<60" "wheel" \
        cython joblib fakeredis threadpoolctl pytest \
        compilers meson-python cmake ninja pkg-config tomli pythran \
        gast beniget

  # Regular (non‑editable) install using the pinned build environment.
  log "Installing scikit-image (non‑editable) with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" \
        python -m pip install --no-build-isolation -v "$REPO_ROOT"$EXTRAS; then
      warn "Installation with extras failed; retrying without extras"
      PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" \
        python -m pip install --no-build-isolation -v "$REPO_ROOT"
  fi

  # Run smoke checks (import and optional pytest discovery)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py \
        --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
