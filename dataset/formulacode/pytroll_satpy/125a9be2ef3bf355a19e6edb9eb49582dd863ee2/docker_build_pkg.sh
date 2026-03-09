#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.10}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# Build & test across envs
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-satpy}"
  log "Using import name: $IMP"

  # Install build dependencies and core runtime dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="$version" \
    pip git conda mamba "libmambapy<=1.9.9" \
    hatchling hatch-vcs \
    numpy dask xarray pillow pyproj pyresample pyyaml \
    platformdirs pooch pykdtree pyorbital trollimage \
    trollsift zarr donfig

  # Install optional dependencies that might be needed for tests
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pytest h5py netcdf4 rasterio bottleneck \
    python-geotiepoints defusedxml numba

  # Editable install with no build isolation
  log "Installing package in editable mode"
  micromamba run -n "$ENV_NAME" pip install --no-build-isolation -v -e "${REPO_ROOT}[tests]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
