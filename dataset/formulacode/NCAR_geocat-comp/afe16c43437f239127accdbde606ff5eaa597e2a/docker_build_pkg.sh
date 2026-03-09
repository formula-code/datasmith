#!/usr/bin/env bash
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Helpers
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-geocat}"
  log "Using import name: $IMP"

  # Install core build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    "python=${version}" pip git conda mamba "libmambapy<=1.9.9" \
    numpy scipy cython joblib fakeredis threadpoolctl pytest \
    compilers meson-python cmake ninja pkg-config tomli \
    "setuptools_scm>=7" \
    cf_xarray>=0.3.1 cftime eofs metpy xskillscore packaging \
    matplotlib-base

  # Install the package without docs extras first
  log "Installing package without docs extras"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Install docs dependencies carefully
  log "Installing docs dependencies"
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    ipykernel ipython sphinx_rtd_theme jupyter_client \
    sphinx-book-theme myst-nb sphinx-design \
    geocat-datafiles geocat-viz nbsphinx netcdf4

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
