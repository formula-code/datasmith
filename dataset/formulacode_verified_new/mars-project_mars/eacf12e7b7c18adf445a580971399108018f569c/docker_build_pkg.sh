#!/usr/bin/env bash
# Purpose: Build/install the repo (non‑editable) in one or more ASV micromamba envs, then run health checks.
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

# Ensure setuptools uses the stdlib distutils implementation
export SETUPTOOLS_USE_DISTUTILS=stdlib

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # -----------------------------
  # Repo‑specific tweaks (optional)
  # -----------------------------
  # Example: install extra system libs via conda‑forge
  # micromamba install -y -n "$ENV_NAME" -c conda-forge openblas libopenblas

  # -------------------------------------------------
  # Install build‑time dependencies that were missing
  # -------------------------------------------------
  log "Installing build dependencies (setuptools, wheel, Cython, pybind11, distutils shim)"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -q \
    "setuptools>=68" "wheel" "Cython" "pybind11" "distutils" || warn "Failed to install build deps"

  # -------------------------------------------------
  # Ensure NumPy version compatible with Mars (<2.0)
  # -------------------------------------------------
  log "Installing NumPy < 2.0 to satisfy Mars compatibility"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -q "numpy<2" || warn "Failed to install NumPy"

  # -------------------------------------------------
  # Install the package (non‑editable) with no build isolation.
  # -------------------------------------------------
  log "Installing package (non‑editable) with --no-build-isolation"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v "$REPO_ROOT"$EXTRAS; then
    warn "Base install failed"
    exit 1
  fi

  # -------------------------------------------------
  # Install development extras so that pytest and other test tools are available for the smoke check.
  # -------------------------------------------------
  log "Installing development extras ([dev])"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v "$REPO_ROOT"[dev]; then
    warn "Dev extras install failed (continuing without them)"
  fi

  # -------------------------------------------------
  # Health checks (import + optional pytest smoke)
  # -------------------------------------------------
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"