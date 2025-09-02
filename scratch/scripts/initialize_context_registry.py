from __future__ import annotations

from datasmith.docker.context import ContextRegistry, DockerContext
from datasmith.logging_config import get_logger

logger = get_logger("docker.context_registry")

CONTEXT_REGISTRY = ContextRegistry(default_context=DockerContext())

# CONTEXT_REGISTRY.register(
#     "astropy/astropy:pkg",
#     DockerContext(
#         building_data="""#!/usr/bin/env bash
# # Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
# set -euo pipefail

# ###### SETUP CODE (NOT TO BE MODIFIED) ######
# # Loads micromamba, common helpers, and persisted variables from the env stage.
# source /etc/profile.d/asv_utils.sh || true
# source /etc/profile.d/asv_build_vars.sh || true
# eval "$(micromamba shell hook --shell=bash)"

# ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
# REPO_ROOT="$ROOT_PATH"
# TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
# if [[ -z "${TARGET_VERSIONS}" ]]; then
#   echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
#   exit 1
# fi
# ###### END SETUP CODE ######

# # -----------------------------
# # Agent guidance (read-first)
# # -----------------------------
# # GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
# #       with NO build isolation, then run health checks.
# #
# # Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# # - Add extra conda/pip dependencies needed to build this project.
# # - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# # - Run arbitrary micromamba/pip commands in the target env.
# # - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# # - Change files in the repo if needed (e.g., fix a missing #include).
# # - Anything else needed to get a successful editable install.
# #
# # MUST:
# # - Keep this script idempotent.
# # - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# # - Do not modify the SETUP CODE or helper functions below.
# #
# # DO NOT:
# # - Change env names or Python versions outside MODEL EDIT AREA.
# # - Use build isolation unless absolutely necessary.

# # -----------------------------
# # Helpers (do not modify)
# # -----------------------------
# log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
# warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
# die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# # Conservative default parallelism (override if the repo benefits)
# export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
# export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# # -----------------------------
# # Build & test across envs
# # -----------------------------
# for version in $TARGET_VERSIONS; do
#   ENV_NAME="asv_${version}"
#   log "==> Building in env: $ENV_NAME (python=$version)"

#   if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
#     die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
#   fi

#   # Import name resolution (kept simple for the agent)
#   IMP="${IMPORT_NAME:-}"
#   if [[ -z "$IMP" ]]; then
#     if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
#       die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
#     fi
#   fi
#   log "Using import name: $IMP"

#   # -----------------------------
#   # MODEL EDIT AREA: repo-specific tweaks (optional)
#   # -----------------------------
#   # Examples (uncomment if needed for this repo):
#   #
#   # log "Updating submodules"
#   # git -C "$REPO_ROOT" submodule update --init --recursive
#   #
#   # log "Installing extra system libs via conda-forge"
#   # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
#   #
#   # log "Pre-generating Cython sources"
#   # micromamba run -n "$ENV_NAME" python -m cython --version
#   #
#   # export CFLAGS="${CFLAGS:-}"
#   # export CXXFLAGS="${CXXFLAGS:-}"
#   # export LDFLAGS="${LDFLAGS:-}"
#   # -----------------------------

#   # Install some basic micromamba packages.
#   micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9"

#   export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
#   micromamba run -n "$ENV_NAME" pip install -e . scipy matplotlib

#   # Editable install (no build isolation preferrably). Toolchain lives in the env already.
#   log "Editable install with --no-build-isolation"
#   PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

#   # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
#   log "Running smoke checks"
#   micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

#   # Machine-readable markers (useful in logs)
#   echo "::import_name=${IMP}::env=${ENV_NAME}"
# done

# log "All builds complete ✅"
# """.strip(),
#     ),
# )

CONTEXT_REGISTRY.register(
    "scikit-learn/scikit-learn:pkg",
    DockerContext(
        building_data="""#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Agent guidance (read-first)
# -----------------------------
# GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
#       with NO build isolation, then run health checks.
#
# Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# - Add extra conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# - Run arbitrary micromamba/pip commands in the target env.
# - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# - Change files in the repo if needed (e.g., fix a missing #include).
# - Anything else needed to get a successful editable install.
#
# MUST:
# - Keep this script idempotent.
# - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# - Do not modify the SETUP CODE or helper functions below.
#
# DO NOT:
# - Change env names or Python versions outside MODEL EDIT AREA.
# - Use build isolation unless absolutely necessary.

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

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
  IMP="${IMPORT_NAME:-}"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # -----------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------
  # Examples (uncomment if needed for this repo):
  #
  # log "Updating submodules"
  # git -C "$REPO_ROOT" submodule update --init --recursive
  #
  # log "Installing extra system libs via conda-forge"
  # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
  #
  # log "Pre-generating Cython sources"
  # micromamba run -n "$ENV_NAME" python -m cython --version
  #
  # export CFLAGS="${CFLAGS:-}"
  # export CXXFLAGS="${CXXFLAGS:-}"
  # export LDFLAGS="${LDFLAGS:-}"
  # -----------------------------

  # Install some basic micromamba packages.

  micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
  micromamba run -n "$ENV_NAME" pip install meson-python cython

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  # Machine-readable markers (useful in logs)
  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
""".strip(),
    ),
)

CONTEXT_REGISTRY.register(
    "scikit-learn/scikit-learn/8bc36080d9855d29e1fcbc86da46a9e89e86c046:pkg",
    DockerContext(
        building_data="""#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Agent guidance (read-first)
# -----------------------------
# GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
#       with NO build isolation, then run health checks.
#
# Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# - Add extra conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# - Run arbitrary micromamba/pip commands in the target env.
# - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# - Change files in the repo if needed (e.g., fix a missing #include).
# - Anything else needed to get a successful editable install.
#
# MUST:
# - Keep this script idempotent.
# - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# - Do not modify the SETUP CODE or helper functions below.
#
# DO NOT:
# - Change env names or Python versions outside MODEL EDIT AREA.
# - Use build isolation unless absolutely necessary.

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

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
  IMP="${IMPORT_NAME:-}"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # -----------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------
  # Examples (uncomment if needed for this repo):
  #
  # log "Updating submodules"
  # git -C "$REPO_ROOT" submodule update --init --recursive
  #
  # log "Installing extra system libs via conda-forge"
  # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
  #
  # log "Pre-generating Cython sources"
  # micromamba run -n "$ENV_NAME" python -m cython --version
  #
  # export CFLAGS="${CFLAGS:-}"
  # export CXXFLAGS="${CXXFLAGS:-}"
  # export LDFLAGS="${LDFLAGS:-}"
  # -----------------------------

  # Install some basic micromamba packages.

  micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
  micromamba run -n "$ENV_NAME" pip install -U meson-python "cython<3" "numpy<2" "setuptools==60" "scipy<1.14"
  export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  # Machine-readable markers (useful in logs)
  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
""".strip(),
    ),
)


CONTEXT_REGISTRY.register(
    "nvidia/warp:pkg",
    DockerContext(
        building_data="""#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Agent guidance (read-first)
# -----------------------------
# GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
#       with NO build isolation, then run health checks.
#
# Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# - Add extra conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# - Run arbitrary micromamba/pip commands in the target env.
# - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# - Change files in the repo if needed (e.g., fix a missing #include).
# - Anything else needed to get a successful editable install.
#
# MUST:
# - Keep this script idempotent.
# - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# - Do not modify the SETUP CODE or helper functions below.
#
# DO NOT:
# - Change env names or Python versions outside MODEL EDIT AREA.
# - Use build isolation unless absolutely necessary.

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# only run the below if condition if bvh.cpp is present
grep -q '^#include <climits>' "${ROOT_PATH}/warp/native/bvh.cpp" || \
  sed -i 's|#include <map>|#include <map>\n#include <climits>|' "${ROOT_PATH}/warp/native/bvh.cpp"


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
  IMP="${IMPORT_NAME:-}"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # -----------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------
  # Examples (uncomment if needed for this repo):
  #
  # log "Updating submodules"
  # git -C "$REPO_ROOT" submodule update --init --recursive
  #
  # log "Installing extra system libs via conda-forge"
  # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
  #
  # log "Pre-generating Cython sources"
  # micromamba run -n "$ENV_NAME" python -m cython --version
  #
  # export CFLAGS="${CFLAGS:-}"
  # export CXXFLAGS="${CXXFLAGS:-}"
  # export LDFLAGS="${LDFLAGS:-}"
  # -----------------------------

  # Install some basic micromamba packages.

  micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
  micromamba run -n "$ENV_NAME" pip install meson-python cython
  export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"
  micromamba run -n "$ENV_NAME" python "${ROOT_PATH}/build_lib.py"

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  # Machine-readable markers (useful in logs)
  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
""".strip(),
    ),
)


CONTEXT_REGISTRY.register(
    "python-control/python-control:pkg",
    DockerContext(
        building_data="""
#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true
eval "$(micromamba shell hook --shell=bash)"

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi
###### END SETUP CODE ######

# -----------------------------
# Agent guidance (read-first)
# -----------------------------
# GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
#       with NO build isolation, then run health checks.
#
# Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# - Add extra conda/pip dependencies needed to build this project.
# - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# - Run arbitrary micromamba/pip commands in the target env.
# - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# - Change files in the repo if needed (e.g., fix a missing #include).
# - Anything else needed to get a successful editable install.
#
# MUST:
# - Keep this script idempotent.
# - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# - Do not modify the SETUP CODE or helper functions below.
#
# DO NOT:
# - Change env names or Python versions outside MODEL EDIT AREA.
# - Use build isolation unless absolutely necessary.

# -----------------------------
# Helpers (do not modify)
# -----------------------------
log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# Conservative default parallelism (override if the repo benefits)
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

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
  IMP="${IMPORT_NAME:-}"
  if [[ -z "$IMP" ]]; then
    if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
      die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
    fi
  fi
  log "Using import name: $IMP"

  # -----------------------------
  # MODEL EDIT AREA: repo-specific tweaks (optional)
  # -----------------------------
  # Examples (uncomment if needed for this repo):
  #
  # log "Updating submodules"
  # git -C "$REPO_ROOT" submodule update --init --recursive
  #
  # log "Installing extra system libs via conda-forge"
  # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
  #
  # log "Pre-generating Cython sources"
  # micromamba run -n "$ENV_NAME" python -m cython --version
  #
  # export CFLAGS="${CFLAGS:-}"
  # export CXXFLAGS="${CXXFLAGS:-}"
  # export LDFLAGS="${LDFLAGS:-}"
  # -----------------------------

  # Install some basic micromamba packages.

  micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
  if [[ -f "${ROOT_PATH}/make_version.py" ]]; then
      micromamba run -n "$ENV_NAME" python "${ROOT_PATH}/make_version.py"
  fi

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  log "Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  log "Running smoke checks"
  micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  # Machine-readable markers (useful in logs)
  echo "::import_name=${IMP}::env=${ENV_NAME}"
done

log "All builds complete ✅"
""".strip(),
    ),
)


# CONTEXT_REGISTRY.register(
#     "mdanalysis/mdanalysis:pkg",
#     DockerContext(
#         building_data="""
# #!/usr/bin/env bash
# # Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
# set -euo pipefail

# ###### SETUP CODE (NOT TO BE MODIFIED) ######
# # Loads micromamba, common helpers, and persisted variables from the env stage.
# source /etc/profile.d/asv_utils.sh || true
# source /etc/profile.d/asv_build_vars.sh || true
# eval "$(micromamba shell hook --shell=bash)"

# ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
# REPO_ROOT="$ROOT_PATH"
# TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
# if [[ -z "${TARGET_VERSIONS}" ]]; then
#   echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
#   exit 1
# fi
# ###### END SETUP CODE ######

# # -----------------------------
# # Agent guidance (read-first)
# # -----------------------------
# # GOAL: For each Python version below, install the project in EDITABLE mode into env asv_{version},
# #       with NO build isolation, then run health checks.
# #
# # Below this comment, you should do whatever is necessary to build the project without errors. Including (but not limited to):
# # - Add extra conda/pip dependencies needed to build this project.
# # - Run repo-specific pre-steps (e.g., submodules, generating Cython, env vars).
# # - Run arbitrary micromamba/pip commands in the target env.
# # - Set CFLAGS/CXXFLAGS/LDFLAGS if needed for this repo.
# # - Change files in the repo if needed (e.g., fix a missing #include).
# # - Anything else needed to get a successful editable install.
# #
# # MUST:
# # - Keep this script idempotent.
# # - Use: `pip install --no-build-isolation -v -e .` or `pip install -e .` or equivalent.
# # - Do not modify the SETUP CODE or helper functions below.
# #
# # DO NOT:
# # - Change env names or Python versions outside MODEL EDIT AREA.
# # - Use build isolation unless absolutely necessary.

# # -----------------------------
# # Helpers (do not modify)
# # -----------------------------
# log()  { printf "\033[1;34m[build]\033[0m %s\n" "$*"; }
# warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
# die()  { printf "\033[1;31m[fail]\033[0m %s\n" "$*" >&2; exit 1; }

# # Conservative default parallelism (override if the repo benefits)
# export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
# export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-2}"

# # -----------------------------
# # Build & test across envs
# # -----------------------------
# for version in $TARGET_VERSIONS; do
#   ENV_NAME="asv_${version}"
#   log "==> Building in env: $ENV_NAME (python=$version)"

#   if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
#     die "Env $ENV_NAME not found. Did docker_build_env.sh run?"
#   fi

#   # Import name resolution (kept simple for the agent)
#   IMP="${IMPORT_NAME:-}"
#   if [[ -z "$IMP" ]]; then
#     if ! IMP="$(asv_detect_import_name --repo-root "$REPO_ROOT" 2>/dev/null)"; then
#       die "Could not determine import name. Set IMPORT_NAME in /etc/profile.d/asv_build_vars.sh"
#     fi
#   fi
#   log "Using import name: $IMP"

#   # -----------------------------
#   # MODEL EDIT AREA: repo-specific tweaks (optional)
#   # -----------------------------
#   # Examples (uncomment if needed for this repo):
#   #
#   # log "Updating submodules"
#   # git -C "$REPO_ROOT" submodule update --init --recursive
#   #
#   # log "Installing extra system libs via conda-forge"
#   # micromamba install -y -n "$ENV_NAME" -c conda-forge 'openblas' 'blas=*=openblas' 'libopenmp'
#   #
#   # log "Pre-generating Cython sources"
#   # micromamba run -n "$ENV_NAME" python -m cython --version
#   #
#   # export CFLAGS="${CFLAGS:-}"
#   # export CXXFLAGS="${CXXFLAGS:-}"
#   # export LDFLAGS="${LDFLAGS:-}"
#   # -----------------------------

#   # Install some basic micromamba packages.

#   micromamba install -y -n "$ENV_NAME" -c conda-forge git conda mamba "libmambapy<=1.9.9" numpy scipy "cython<3" joblib threadpoolctl pytest compilers meson-python
#   # if maintainer/install_all.sh exists run it with develop
#   if [[ -f "maintainer/install_all.sh" ]]; then
#     micromamba activate "$ENV_NAME"
#     working_dir=$(pwd)
#     cd "$ROOT_PATH" || exit 1
#     bash maintainer/install_all.sh develop
#     cd "$working_dir" || exit 1
#   else
#     # Editable install (no build isolation preferrably). Toolchain lives in the env already.
#     log "Editable install with --no-build-isolation"
#     PIP_NO_BUILD_ISOLATION=1 micromamba run -n "$ENV_NAME" python -m pip install --no-build-isolation -v -e "$REPO_ROOT"
#   fi


#   # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
#   log "Running smoke checks"
#   micromamba run -n "$ENV_NAME" asv_smokecheck.py --import-name "$IMP" --repo-root "$REPO_ROOT" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

#   # Machine-readable markers (useful in logs)
#   echo "::import_name=${IMP}::env=${ENV_NAME}"
# done

# log "All builds complete ✅"
# """.strip(),
#     ),
# )


# if __name__ == "__main__":
#     from pathlib import Path

#     CONTEXT_REGISTRY.save_to_file(Path("scratch/context_registry_init.json"))
#     # for each context, build an image with the context.
#     import docker

#     from datasmith.docker.context import ContextRegistry, DockerContext, Task

#     client = docker.from_env()

#     import concurrent.futures

#     fails = dict()

#     def build_context(task_context):
#         task, context = task_context
#         if not task.sha:
#             import requests

#             resp = requests.get(f"https://api.github.com/repos/{task.owner}/{task.repo}")
#             resp.raise_for_status()
#             commit_sha = resp.json().get("default_branch", "main")
#             task = Task(owner=task.owner, repo=task.repo, sha=commit_sha, tag=task.tag)
#         print(f"Building image for {task.get_image_name()} at {task.sha}")
#         res = context.build_container_streaming(
#             client=client,
#             image_name=task.get_image_name(),
#             build_args={
#                 "REPO_URL": f"https://www.github.com/{task.owner}/{task.repo}",
#                 "COMMIT_SHA": task.sha,  # pyright: ignore[reportArgumentType]
#             },
#             force=True,
#             timeout_s=1200,
#             pull=True,
#         )
#         return (task, res)

#     with concurrent.futures.ProcessPoolExecutor() as executor:
#         futures = {executor.submit(build_context, item): item[0] for item in CONTEXT_REGISTRY.registry.items()}
#         for future in concurrent.futures.as_completed(futures):
#             task = futures[future]
#             try:
#                 task, res = future.result()
#                 if res.ok:
#                     print(f"Built image {task.get_image_name()} successfully")
#                 else:
#                     print(f"Failed to build image {task.get_image_name()}")
#                     fails[task] = res
#             except Exception as exc:
#                 print(f"Exception building image {task.get_image_name()}: {exc}")
#                 fails[task] = exc

#     if fails:
#         import IPython

#         IPython.embed()
