#!/usr/bin/env bash
set -euo pipefail

# Helper functions
log() {
    echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] $*" >&2
}

###### SETUP CODE ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="spatialdata"
  log "Using import name: $IMP"

  # Create fresh environment
  micromamba create -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" \
    pip git conda mamba "libmambapy<=1.9.9"

  # Install core dependencies with specific versions from conda-forge
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "numpy>=1.24" \
    "scipy>=1.10" \
    "pandas>=2.0" \
    "xarray>=2024.4.0,<2024.12.0" \
    "dask>=2024.4.1,<=2024.11.2" \
    "geopandas>=0.14" \
    "shapely>=2.0.1" \
    "scikit-image" \
    "networkx" \
    "numba>=0.55.0" \
    "zarr<3" \
    "anndata>=0.9.1" \
    "ome-zarr>=0.8.4" \
    "hatchling" \
    "hatch-vcs" \
    "setuptools" \
    "pytest" \
    "pytest-cov"

  # Pre-install some dependencies with specific versions
  micromamba run -n "$ENV_NAME" pip install \
    "xarray-dataclasses>=1.8.0,<2.0.0" \
    "spatial-image>=1.1.0" \
    "multiscale-spatial-image>=2.0.2" \
    "xarray-schema" \
    "xarray-spatial>=0.3.5" \
    "typing-extensions>=4.8.0"

  # Install package in editable mode with extras
  log "Installing package in editable mode"
  micromamba run -n "$ENV_NAME" pip install -e "${REPO_ROOT}[test,dev]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
