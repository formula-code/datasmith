#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="3.11"  # Match pyproject.toml requirement
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-sunpy}"
  log "Using import name: $IMP"

  # Install build dependencies and test requirements
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="$version" \
    pip git conda mamba "libmambapy<=1.9.9" \
    setuptools>=62.1 \
    setuptools_scm \
    wheel \
    extension-helpers \
    "numpy>=2.0.0,<2.3" \
    astropy>=6.1.0 \
    packaging>=23.2 \
    parfive>=2.1.0 \
    pyerfa>=2.0.1.1 \
    requests>=2.32.0 \
    fsspec>=2023.6.0 \
    scipy>=1.11.0 \
    matplotlib>=3.8.0 \
    pytest>=7.4.0 \
    pytest-astropy>=0.11.0 \
    pytest-mpl>=0.17.0 \
    pytest-xdist>=3.4.0 \
    compilers \
    cmake \
    ninja \
    pkg-config

  # Install parfive with ftp support via pip
  micromamba run -n "$ENV_NAME" pip install "parfive[ftp]>=2.1.0"

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT[tests]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
