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
TARGET_VERSIONS="${PY_VERSION:-3.10}"
EXTRAS="${ALL_EXTRAS:+[test,dev]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  log "Error: No PY_VERSION set" >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="spatialdata"
  log "Using import name: $IMP"

  # Install build dependencies and runtime requirements
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    python="$version" \
    pip git conda mamba "libmambapy<=1.9.9" \
    numpy scipy dask xarray geopandas shapely scikit-image \
    networkx numba anndata pandas pyarrow rich setuptools \
    hatchling hatch-vcs pytest \
    compilers meson-python cmake ninja pkg-config

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
