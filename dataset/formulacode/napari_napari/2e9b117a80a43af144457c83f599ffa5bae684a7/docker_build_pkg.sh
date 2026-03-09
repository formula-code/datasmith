#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# Helper functions
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Set correct import name
export IMPORT_NAME="napari"

# -----------------------------
# Build & test across envs
# -----------------------------
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
  fi

  # Import name resolution (kept simple for the agent)
  IMP="$(detect_import_name || true)"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # Install base dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pip git conda mamba "libmambapy<=1.9.9" \
    numpy scipy cython joblib pytest \
    compilers meson-python cmake ninja pkg-config tomli

  # Install Qt and other required dependencies
  micromamba install -y -n "$ENV_NAME" -c conda-forge \
    pyqt qt appdirs dask imageio jsonschema magicgui \
    pandas pillow pint psutil pygments pyopengl pyyaml \
    scikit-image scipy tifffile vispy \
    app-model cachey certifi lazy_loader napari-console \
    napari-plugin-engine napari-svg npe2 numpydoc \
    psygnal pydantic superqt toolz tqdm typing_extensions wrapt

  # Editable install with all extras
  log "Installing napari with all extras"
  micromamba run -n "$ENV_NAME" pip install --no-build-isolation -v -e "$REPO_ROOT[all]"

  # Health checks
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
