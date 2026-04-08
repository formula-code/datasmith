#!/usr/bin/env bash
set -euo pipefail

# Helper functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

###### SETUP CODE (NOT TO BE MODIFIED) ######
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
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="shapely"
  log "Using import name: $IMP"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "python=${version}" \
    numpy cython \
    geos compilers pkg-config \
    pytest

  # Set GEOS paths for the build
  export GEOS_INCLUDE_PATH=$(micromamba run -n "$ENV_NAME" pkg-config --variable=includedir geos)
  export GEOS_LIBRARY_PATH=$(micromamba run -n "$ENV_NAME" pkg-config --variable=libdir geos)

  # Editable install with no build isolation
  log "Installing package in development mode..."
  micromamba run -n "$ENV_NAME" \
    pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS

  # Health checks
  log "Running smoke checks..."
  micromamba run -n "$ENV_NAME" python -c "import shapely; print(f'Shapely {shapely.__version__} imported successfully')"

  if [[ "${RUN_PYTEST_SMOKE:-}" == "1" ]]; then
    micromamba run -n "$ENV_NAME" pytest -v shapely/tests/test_basic.py
  fi

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
