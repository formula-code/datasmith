#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
command -v fc_note >/dev/null 2>&1 || fc_note() { :; }

REPO_ROOT=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"

log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"
###### END SETUP CODE ######

# =====================================================================
# Agent guidance
# =====================================================================
# GOAL: Install the project in EDITABLE mode into each asv_{version} env.
#
# You may:
# - Add conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (submodules, Cython generation, env vars).
# - Set CFLAGS/CXXFLAGS/LDFLAGS as needed.
# - Modify source files if needed (e.g. fix a missing #include).
# - Modify the external benchmarks section below (URLs, branches, paths).
#
# You must:
# - Keep this script idempotent.
# - Use editable install: `uv pip install -e .` or `pip install -e .`.
# - Not modify the SETUP CODE block above.

# =====================================================================
# External benchmarks (agent-editable)
# =====================================================================
# Some repos store ASV benchmarks in a separate repository.  Clone them
# here so they are baked into the Docker image and available at runtime.
# Modify URLs, branches, or destination paths as needed.
ORIGIN_URL=$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || true)

if [[ "$ORIGIN_URL" =~ github\.com/dask/dask(\.git)?$ ]]; then
    log "Cloning dask-benchmarks"
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/dask/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/dask/distributed(\.git)?$ ]]; then
    log "Cloning dask-benchmarks (distributed)"
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/distributed/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/joblib/joblib(\.git)?$ ]]; then
    log "Cloning joblib_benchmarks"
    git clone --depth 1 https://github.com/pierreglaser/joblib_benchmarks.git /tmp/benchmarks
    cp -rf /tmp/benchmarks/* "$REPO_ROOT/"
    rm -rf /tmp/benchmarks
elif [[ "$ORIGIN_URL" =~ github\.com/astropy/astropy(\.git)?$ ]]; then
    log "Cloning astropy-benchmarks"
    rm -rf "$REPO_ROOT/astropy-benchmarks"
    git clone -b main --depth 1 --single-branch \
        https://github.com/astropy/astropy-benchmarks.git "$REPO_ROOT/astropy-benchmarks"
fi

# =====================================================================
# Build backend
# =====================================================================
# Map a PEP 517 backend module onto the distribution that provides it. The
# fallback (top-level module, underscores to hyphens) is right for most of the
# long tail, so only the ones it gets wrong are listed.
backend_distribution() {
  case "$1" in
    setuptools*)      echo "setuptools wheel" ;;
    scikit_build_core*) echo "scikit-build-core" ;;
    mesonpy|meson*)   echo "meson-python" ;;
    poetry*)          echo "poetry-core" ;;
    flit_core*)       echo "flit-core" ;;
    pdm.backend*)     echo "pdm-backend" ;;
    pdm.pep517*)      echo "pdm-pep517" ;;
    sipbuild*)        echo "sip" ;;
    py_build_cmake*)  echo "py-build-cmake" ;;
    *)                echo "${1%%.*}" | tr '_' '-' ;;
  esac
}

env_numpy_version() {
  micromamba run -n "$1" python -c "import numpy; print(numpy.__version__)" 2>/dev/null || true
}

ensure_build_backend() {
  local env_name=$1
  [ -f "$REPO_ROOT/pyproject.toml" ] || return 0

  local backend
  backend=$(micromamba run -n "$env_name" python -c '
import sys
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        sys.exit(0)
try:
    with open("'"$REPO_ROOT"'/pyproject.toml", "rb") as fh:
        print(tomllib.load(fh).get("build-system", {}).get("build-backend", "") or "")
except Exception:
    pass
' 2>/dev/null | tr -d '[:space:]')

  [ -n "$backend" ] || return 0

  local module="${backend%%.*}"
  if micromamba run -n "$env_name" python -c "import $module" >/dev/null 2>&1; then
    log "Build backend '$backend' already importable"
    fc_note "build_backend=$backend"
    return 0
  fi

  local dist
  dist="$(backend_distribution "$backend")"
  log "Build backend '$backend' missing, installing: $dist"
  # shellcheck disable=SC2086
  if micromamba run -n "$env_name" python -m pip install $dist; then
    fc_note "build_backend=$backend"
  else
    warn "Could not install build backend '$dist'; the isolated fallback will have to resolve it"
  fi
}

# =====================================================================
# Build & install across envs
# =====================================================================
for version in $TARGET_VERSIONS; do
  ENV_NAME="asv_${version}"
  log "==> Building in env: $ENV_NAME (python=$version)"

  IMP="$(detect_import_name || true)"
  log "Using import name: $IMP"

  # -----------------------------------------------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------------------------------------------
  # Examples (uncomment if needed for this repo):
  # git -C "$REPO_ROOT" submodule update --init --recursive
  # micromamba install -y -n "$ENV_NAME" -c conda-forge openblas libopenmp
  # export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
  #
  # shapely: GEOS must come from conda-forge, not from the system.
  #
  # setup.py finds GEOS through `geos-config` on PATH. The base image already
  # has Ubuntu's at /usr/bin/geos-config, so GEOS *is* found -- it reports
  # `--version 3.10.2`, `--includes /usr/include` and
  # `--clibs -L/usr/lib/x86_64-linux-gnu -lgeos_c`. The compile, however, runs
  # under the conda toolchain (x86_64-conda-linux-gnu-cc) with its own
  # sysroot, so adding /usr/include drags Ubuntu's glibc headers into a
  # conda-sysroot build and it dies in bits/wchar2.h with
  #     error: implicit declaration of function '__mbsnrtowcs_chk'
  # long before it reaches any GEOS symbol. That message names glibc, not
  # GEOS, which is why this reads as a toolchain bug rather than a missing
  # dependency, and why the fix is not "install libgeos-dev".
  #
  # Installing geos into the env puts geos-config in the env's own bin --
  # which `micromamba run -n` puts ahead of /usr/bin -- so includes and libs
  # both resolve inside the conda prefix and the headers stay consistent.
  #
  # env_payload cannot carry it: shapely's asv.conf.json declares
  # `"environment_type": "conda"` with `"matrix": {"geos": []}`, and stage 4
  # only reads an asv matrix as PyPI names for the environment types in
  # DATASMITH_ASV_PIP_ENV_TYPES (virtualenv/venv/existing). Under a conda
  # matrix `geos` is a conda package, so the resolver correctly refuses to
  # invent a PyPI distribution for it -- it has to be installed here.
  #
  # Guarded on the origin URL, like the dask/joblib/astropy blocks above, so
  # it is inert for every other repository.
  if [[ "$ORIGIN_URL" =~ github\.com/shapely/shapely(\.git)?$ ]]; then
    log "shapely: installing GEOS from conda-forge into $ENV_NAME"
    micromamba install -y -n "$ENV_NAME" -c conda-forge geos
    micromamba run -n "$ENV_NAME" geos-config --version
  fi
  # -----------------------------------------------------------------

  # --no-build-isolation needs the PEP 517 backend already present in the env.
  # It is not there by default, so every repo that uses hatchling,
  # scikit-build-core, meson-python, poetry-core or flit failed here with
  # "BackendUnavailable: Cannot import 'hatchling.build'". Both attempts below
  # used to differ only in $EXTRAS, so the retry could never fix it.
  ensure_build_backend "$ENV_NAME"

  _NUMPY_BEFORE="$(env_numpy_version "$ENV_NAME")"

  log "Editable install with --no-build-isolation"
  _ISOLATION="none"
  if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"$EXTRAS; then
    warn "Failed with extras, retrying without"
    if ! PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"; then
      # Last resort. An isolated build resolves the backend itself, but it also
      # builds against whatever numpy the backend picks, which is a weaker ABI
      # guarantee than building against the env's own. Record which one we got
      # so whatever gates publishing can see the difference.
      warn "No-isolation install failed twice, falling back to an ISOLATED build"
      _ISOLATION="isolated"
      micromamba run -n "$ENV_NAME" python -m pip install -v -e "$REPO_ROOT"
    fi
  fi
  fc_note "build_isolation=$_ISOLATION"

  # An install that moves numpy under the extension it just compiled produces a
  # container that imports cleanly now and mismeasures later. Name it here.
  _NUMPY_AFTER="$(env_numpy_version "$ENV_NAME")"
  if [ -n "$_NUMPY_BEFORE" ] && [ "$_NUMPY_BEFORE" != "$_NUMPY_AFTER" ]; then
    warn "numpy moved during install: $_NUMPY_BEFORE -> $_NUMPY_AFTER (ABI risk)"
    fc_note "numpy_moved_during_install=$_NUMPY_BEFORE->$_NUMPY_AFTER"
  fi

  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete"
