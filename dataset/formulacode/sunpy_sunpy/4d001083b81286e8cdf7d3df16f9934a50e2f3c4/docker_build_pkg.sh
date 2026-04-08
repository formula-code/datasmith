#!/usr/bin/env bash
set -euo pipefail

###### SETUP CODE ######
# Define logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" >&2
}

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="3.11"  # Python >=3.11 required
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
  log "Error: No PY_VERSION set and ASV_PY_VERSIONS not found."
  exit 1
fi

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-sunpy}"
  log "Using import name: $IMP"

  # Install build dependencies from pyproject.toml
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" \
    pip git conda mamba "libmambapy<=1.9.9" \
    setuptools wheel "numpy>=2.0.0,<2.3" \
    extension-helpers setuptools-scm \
    compilers meson-python cmake ninja pkg-config

  # Install runtime dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "astropy>=6.0.0" \
    "parfive>=2.0.0" \
    "pyerfa>=2.0.1.1" \
    "requests>=2.28.0" \
    "fsspec>=2023.3.0" \
    "packaging>=23.0"

  # Install test dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pytest pytest-astropy

  # Editable install with build isolation (due to numpy version requirements)
  log "Installing package with build isolation"
  micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"$EXTRAS

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
