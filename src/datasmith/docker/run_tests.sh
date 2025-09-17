#!/usr/bin/env bash
# Run pytest test suite after the pkg stage and archive results.
set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [LOG_PATH] [TEST_SUITE] [ADDITIONAL_PYTEST_ARGS...]

Options:
  LOG_PATH                 Full path for logging/results (required)
  TEST_SUITE               Path/package/expr to pass to pytest (required)
  ADDITIONAL_PYTEST_ARGS   Additional arguments to pass to pytest (optional)
  -h, --help               Show this help message and exit

Description:
  This script runs a project's pytest test suite inside the sandboxed
  environment and bundles the results into a single tar.gz artifact.
  It mirrors the conventions of profile.sh but invokes pytest instead of asv.

  Heuristics:
    - If TEST_SUITE is a directory, the script cd's into that directory and
      invokes 'pytest -rA .' (helps with projects like SciPy that disallow
      running pytest from the repo root).
    - Otherwise, pytest is run from the repo root with TEST_SUITE as given.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  show_help
  exit 0
fi

if [ -z "${1:-}" ] || [ -z "${2:-}" ]; then
  echo "Error: LOG_PATH and TEST_SUITE arguments are required." >&2
  show_help
  exit 1
fi

LOG_DIR=$(realpath "$1")
shift
TEST_SUITE="$1"
shift || true

# Remaining args go straight to pytest
ADDITIONAL_PYTEST_ARGS=("$@")

RESULTS_DIR="${LOG_DIR}/results"
TARBALL="${LOG_DIR}.tar.gz"
BASE_SHA_FILE="${LOG_DIR}/sha.txt"
BRANCH_NAME="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

# Environment hooks (may not exist in all images)
source /etc/profile.d/asv_utils.sh 2>/dev/null || true
source /etc/profile.d/asv_build_vars.sh 2>/dev/null || true

# Select/activate micromamba environment if not already selected
if [ -z "${ENV_NAME:-}" ]; then
  if [ -n "${ASV_PY_VERSIONS:-}" ]; then
    ENV_NAME="asv_$(echo "$ASV_PY_VERSIONS" | awk '{print $1}')"
  else
    ENV_NAME="default"
  fi
  set +u
  micromamba activate "$ENV_NAME" 2>/dev/null || true
  set -u
fi

CURR_DIR="$(pwd)"
cd /workspace/repo

mkdir -p "$LOG_DIR" "$RESULTS_DIR"

COMMIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

# Persist some context
echo "$COMMIT_SHA" > "$BASE_SHA_FILE"
echo "$BRANCH_NAME" > "${LOG_DIR}/branch.txt"
echo "$REPO_ROOT" > "${LOG_DIR}/repo_root.txt"

# Capture environment details
{
  echo "[env] date: $(date -Is)"
  echo "[env] python: $(micromamba run -n "$ENV_NAME" python -V 2>/dev/null || python -V)"
  echo "[env] which python: $(micromamba run -n "$ENV_NAME" which python 2>/dev/null || which python)"
  micromamba run -n "$ENV_NAME" python -m pip freeze 2>/dev/null || true
} > "${LOG_DIR}/environment.txt" || true

# Decide working directory and target based on TEST_SUITE
RUN_DIR="/workspace/repo"
TARGET="$TEST_SUITE"
if [ -d "$TEST_SUITE" ]; then
  # For directories, cd into the directory and run against '.'
  RUN_DIR="$(realpath "$TEST_SUITE")"
  TARGET="."
fi

# Fallback for common package-name invocations like 'scipy'
if [ "$TARGET" != "." ] && [ ! -e "$TARGET" ] && [ -d "/workspace/repo/$TEST_SUITE" ]; then
  RUN_DIR="/workspace/repo/$TEST_SUITE"
  TARGET="."
fi

PYTEST_LOG="${LOG_DIR}/pytest.log"
JUNIT_XML="${LOG_DIR}/junit.xml"
EXIT_CODE_FILE="${LOG_DIR}/exit_code.txt"

echo "[run-tests] Running pytest in $RUN_DIR: target=$TARGET"

OLD_DIR="$(pwd)"
cd "$RUN_DIR"

# Compose pytest arguments
PYTEST_ARGS=(-rA "$TARGET" --junitxml "$JUNIT_XML")
if [ ${#ADDITIONAL_PYTEST_ARGS[@]} -gt 0 ]; then
  # shellcheck disable=SC2206
  PYTEST_ARGS+=("${ADDITIONAL_PYTEST_ARGS[@]}")
fi

# Execute tests; never abort the script on non-zero to preserve artifacts
micromamba run -n "$ENV_NAME" pytest "${PYTEST_ARGS[@]}" | tee "$PYTEST_LOG"
TEST_RC="$?"
echo "$TEST_RC" > "$EXIT_CODE_FILE"

cd "$OLD_DIR"

# Bundle everything
tar -czf "$TARBALL" "$LOG_DIR"

# Optionally clean up the expanded directory, keeping only the tarball
rm -rf "$LOG_DIR"

cd "$CURR_DIR"
echo "[run-tests] Pytest completed with exit code $TEST_RC. Artifact: $TARBALL"
