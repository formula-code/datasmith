#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh 2>/dev/null || true
source /etc/profile.d/asv_build_vars.sh 2>/dev/null || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="${ROOT_PATH}"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"
EXTRAS="${ALL_EXTRAS:+[$ALL_EXTRAS]}"
if [[ -z "${TARGET_VERSIONS}" ]]; then
  echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
  exit 1
fi

# Default IMPORT_NAME to project name from pyproject.toml if not set
IMPORT_NAME="${IMPORT_NAME:-pandas}"
echo "==> Using import name: ${IMPORT_NAME}"
###### END SETUP CODE ######

# -----------------------------
# Build & test across envs
# -----------------------------
for version in ${TARGET_VERSIONS}; do
  ENV_NAME="asv_${version}"
  echo "==> Building in env: ${ENV_NAME} (python=${version})"

  # Install build dependencies including versioneer
  micromamba install -y -n "${ENV_NAME}" -c conda-forge pip git conda mamba "libmambapy<=1.9.9" \
        numpy scipy cython joblib fakeredis threadpoolctl pytest \
        compilers meson-python cmake ninja pkg-config tomli versioneer

  # Generate version info first to avoid circular dependency
  echo "==> Generating version info"
  micromamba run -n "${ENV_NAME}" python generate_version.py --outfile _version_meson.py

  # Editable install (no build isolation preferrably). Toolchain lives in the env already.
  echo "==> Editable install with --no-build-isolation"
  PIP_NO_BUILD_ISOLATION=1 micromamba run -n "${ENV_NAME}" python -m pip install --no-build-isolation -v -e "${REPO_ROOT}${EXTRAS}"

  # Health checks (import + compiled extension probe; optional pytest smoke with RUN_PYTEST_SMOKE=1)
  echo "==> Running smoke checks"
  micromamba run -n "${ENV_NAME}" asv_smokecheck.py --import-name "${IMPORT_NAME}" --repo-root "${REPO_ROOT}" ${RUN_PYTEST_SMOKE:+--pytest-smoke}

  echo "::import_name=${IMPORT_NAME}::env=${ENV_NAME}"
done

echo "==> All builds complete ✅"
