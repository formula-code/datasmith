#!/usr/bin/env bash
set -euo pipefail

# Define logging function
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S')] $*"
}

# Source common helpers if they exist
source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}
REPO_ROOT="$ROOT_PATH"
TARGET_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-3.9}}"

if [[ -z "${TARGET_VERSIONS}" ]]; then
    echo "Error: No PY_VERSION set and ASV_PY_VERSIONS not found." >&2
    exit 1
fi

# Build & test across envs
for version in $TARGET_VERSIONS; do
    ENV_NAME="asv_${version}"
    log "==> Building in env: $ENV_NAME (python=$version)"

    # Create and activate environment
    micromamba create -y -n "$ENV_NAME" -c conda-forge \
        "python=${version}" \
        pip \
        pytest \
        pytest-asyncio \
        pytest-cov \
        hatchling \
        hatch-vcs

    # Install the package with test dependencies
    log "Installing param with test dependencies..."
    micromamba run -n "$ENV_NAME" pip install -e "$REPO_ROOT[tests]"

    # Verify installation
    log "Verifying installation..."
    micromamba run -n "$ENV_NAME" python -c "import param; print(f'param version: {param.__version__}')"

    log "Build successful for Python $version ✅"
done

log "All builds completed successfully"
