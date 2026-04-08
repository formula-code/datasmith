#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

# Define log function
log() {
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*"
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-3.11}"
EXTRAS="${ALL_EXTRAS:+[test,notebook,other]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-adaptive}"
  log "Using import name: $IMP"

  # Install build and runtime dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=$version" \
    pip git conda mamba "libmambapy<=1.9.9" \
    scipy \
    "sortedcollections>=1.1" \
    "sortedcontainers>=2.0" \
    cloudpickle \
    "loky>=2.9" \
    "setuptools>=69.0.0" \
    "versioningit>=3.0.0" \
    wheel \
    pytest \
    ipython \
    ipykernel \
    jupyter_client \
    holoviews \
    ipywidgets \
    bokeh \
    pandas \
    matplotlib \
    plotly \
    flaky \
    pytest-cov \
    pytest-randomly \
    pytest-timeout \
    pytest-xdist

  # Editable install with no build isolation since we manage deps
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
