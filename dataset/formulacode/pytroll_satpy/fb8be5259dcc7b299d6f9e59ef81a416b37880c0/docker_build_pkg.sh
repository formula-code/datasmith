#!/usr/bin/env bash
set -euo pipefail

# Helper functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

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

    IMP="${IMPORT_NAME:-satpy}"
    log "Using import name: $IMP"

    # Install core build dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        pip git conda mamba "libmambapy<=1.9.9" \
        "python=$version" \
        hatchling hatch-vcs

    # Install runtime dependencies
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        xarray dask distributed dask-image platformdirs \
        pillow matplotlib scipy pyyaml pyproj \
        netcdf4 h5py gdal rasterio bottleneck \
        defusedxml imageio zarr pytest pooch \
        python-geotiepoints trollimage trollsift \
        pyorbital pykdtree numba cython \
        pint-xarray h5netcdf

    # Editable install with no build isolation
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
