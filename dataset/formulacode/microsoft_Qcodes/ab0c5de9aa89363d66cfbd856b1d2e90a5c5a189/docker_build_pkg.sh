#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

# Define log function if not already defined
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-3.11}"
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

  # Install base dependencies needed for building
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "python=${version}" \
    "setuptools>=77.0.0" \
    "versioningit>=2.2.1"

  # Install core dependencies from pyproject.toml
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "h5py>=3.8.0" \
    "ipywidgets>=8.0.0,<9.0.0" \
    "ipykernel>=6.12.0" \
    "jsonschema>=4.9.0" \
    "matplotlib>=3.6.0" \
    "networkx>=3.1" \
    "numpy>=1.22.4" \
    "pandas>=1.5.0" \
    "pyarrow>=11.0.0" \
    "pyvisa>=1.11.0,<1.16.0" \
    "ruamel.yaml>=0.16.0" \
    "tabulate>=0.9.0" \
    "tqdm>=4.59.0" \
    "xarray>=2023.08.0" \
    "broadbean>=0.11.0" \
    "h5netcdf>=0.14.1" \
    "uncertainties>=3.2.0" \
    "websockets>=11.0" \
    "cf_xarray>=0.8.4" \
    "opentelemetry-api>=1.17.0" \
    "pillow>=9.2.0" \
    "dask>=2022.1.0" \
    "typing_extensions>=4.6.0"

  # Editable install with no build isolation
  log "Installing package in editable mode..."
  micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT"

  # Basic import test
  log "Testing imports..."
  micromamba run -n "$ENV_NAME" python -c "import qcodes; print(f'Successfully imported qcodes {qcodes.__version__}')"

  echo "::import_name=qcodes::env=${ENV_NAME}"
done

log "All builds completed successfully ✅"
