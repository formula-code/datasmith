#!/usr/bin/env bash
# Purpose: Build/install the repo (editable) in one or more ASV micromamba envs, then run health checks.
set -euo pipefail

###### SETUP CODE (NOT TO BE MODIFIED) ######
# Loads micromamba, common helpers, and persisted variables from the env stage.
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

REPO_ROOT=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
TARGET_VERSIONS=


###### CUSTOM BUILD SCRIPT ######
# This section is for the user to modify.

# Change to the repository root directory
cd "${REPO_ROOT}"

# Install the project in editable mode with test and doc dependencies
# Removed --no-deps to allow pip to handle dependency resolution.
pip install -e ".[test,doc]"

# Optional: Add a simple import check for basic sanity (post-install)
# python -c "import pvlib; print(f'Successfully imported pvlib version: {pvlib.__version__}')"