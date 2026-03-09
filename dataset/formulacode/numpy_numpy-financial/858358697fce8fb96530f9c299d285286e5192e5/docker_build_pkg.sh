#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

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

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="${IMPORT_NAME:-numpy_financial}"
  log "Using import name: $IMP"

  # Install build dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    "numpy>=1.23.5" scipy "cython>=3.0.9" \
    "meson-python>=0.15.0" cmake ninja pkg-config \
    pytest pytest-xdist hypothesis

  # Create backup of pyproject.toml
  cp "$REPO_ROOT/pyproject.toml" "$REPO_ROOT/pyproject.toml.bak"

  # Fix pyproject.toml format issues
  log "Fixing pyproject.toml format"
  sed -i '/^license = /d' "$REPO_ROOT/pyproject.toml"
  sed -i 's/authors = \["Travis E. Oliphant et al."\]/authors = [{name = "Travis E. Oliphant et al."}]/' "$REPO_ROOT/pyproject.toml"
  sed -i 's/maintainers = \["Numpy Financial Developers <numpy-discussion@python.org>"\]/maintainers = [{name = "Numpy Financial Developers", email = "numpy-discussion@python.org"}]/' "$REPO_ROOT/pyproject.toml"

  # Editable install with no build isolation
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"$EXTRAS

  # Restore original pyproject.toml
  mv "$REPO_ROOT/pyproject.toml.bak" "$REPO_ROOT/pyproject.toml"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
