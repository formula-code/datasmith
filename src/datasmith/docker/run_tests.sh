#!/usr/bin/env bash
# Run pytest test suite after the pkg stage and archive results.
# Key behaviors:
#   • Env auto-select: chooses among asv_3.11 … asv_3.7, honoring repo "requires-python" when available.
#   • SciPy: supports BOTH modern (dev.py) and legacy (runtests.py) flows.
#   • Default to installed-package tests (--pyargs) for bare names (robust for compiled/src layouts).
#   • Special cases: MDAnalysisTests, Pillow/Tests, Apache Beam sdks/python, PyArrow test data, napari headless, h5py MPI.
#   • Always writes junit.xml, logs, exit_code.txt, command.txt, tarball; never drops artifacts on failures.

set -euo pipefail

show_help() {
  cat <<EOF
Usage: $(basename "$0") [LOG_PATH] [TEST_SUITE] [ADDITIONAL_PYTEST_ARGS...]

Arguments:
  LOG_PATH                 Full path for logs/results (required)
  TEST_SUITE               Dir/file/import name/pytest expr (required)
  ADDITIONAL_PYTEST_ARGS   Extra args for pytest (or after '--' for SciPy dev.py)

Env knobs:
  ENV_NAME            Use this micromamba env as-is.
  ASV_PY_VERSIONS     If set, try 'asv_<first-token>'.
  RUN_MPI_TESTS=1     Enable mpirun for h5py (requires MPI)
  MPI_NP              MPI ranks (default 2)
  PYTEST_K            If set, adds: -k "\$PYTEST_K"
  MAXFAIL             If set, adds: --maxfail "\$MAXFAIL"
EOF
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { show_help; exit 0; }
if [ -z "${1:-}" ] || [ -z "${2:-}" ]; then
  echo "Error: LOG_PATH and TEST_SUITE are required." >&2
  show_help; exit 1
fi

LOG_DIR="$(realpath "$1")"; shift
TEST_SUITE="$1"; shift || true
ADDITIONAL_PYTEST_ARGS=("$@")

RESULTS_DIR="${LOG_DIR}/results"
TARBALL="${LOG_DIR}.tar.gz"
BASE_SHA_FILE="${LOG_DIR}/sha.txt"
BRANCH_NAME="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

# Optional profile hooks
source /etc/profile.d/asv_utils.sh 2>/dev/null || true
source /etc/profile.d/asv_build_vars.sh 2>/dev/null || true

# ---------- ENV SELECTION (asv_3.11…asv_3.7; honors repo "requires-python") ----------
_detect_repo_python_spec() {
  python - <<'PY' 2>/dev/null || echo unknown
import re, pathlib
root = pathlib.Path("/workspace/repo")
def read(p):
    try: return p.read_text(encoding="utf-8", errors="ignore")
    except: return ""
spec = None
# pyproject.toml
txt = read(root/"pyproject.toml")
if txt:
    try:
        import tomllib as tomli
    except Exception:
        try:
            import tomli
        except Exception:
            tomli = None
    if tomli:
        try:
            data = tomli.loads(txt)
            spec = (data.get("project",{}) or {}).get("requires-python") \
                   or (data.get("tool",{}).get("poetry",{}).get("dependencies",{}) or {}).get("python")
        except Exception:
            pass
# setup.cfg
if not spec:
    import configparser
    cfg = configparser.ConfigParser()
    s = read(root/"setup.cfg")
    if s:
        try:
            cfg.read_string(s)
            if cfg.has_section("options") and cfg.has_option("options","python_requires"):
                spec = cfg.get("options","python_requires").strip()
        except Exception:
            pass
# setup.py
if not spec:
    sp = read(root/"setup.py")
    if sp:
        m = re.search(r"python_requires\s*=\s*['\"]([^'\"]+)['\"]", sp)
        if m: spec = m.group(1).strip()
print(spec or "unknown")
PY
}

_pick_env_by_spec() {
  local spec="$1"
  # explicit override
  if [ -n "${ENV_NAME:-}" ]; then echo "$ENV_NAME"; return; fi
  # ASV_PY_VERSIONS hint
  if [ -n "${ASV_PY_VERSIONS:-}" ]; then
    local tok; tok="$(echo "$ASV_PY_VERSIONS" | awk '{print $1}')"
    micromamba run -n "asv_${tok}" python -V >/dev/null 2>&1 && { echo "asv_${tok}"; return; }
  fi
  local candidates=("asv_3.11" "asv_3.10" "asv_3.9" "asv_3.8" "asv_3.7")
  if [ "$spec" = "unknown" ]; then
    for e in "${candidates[@]}"; do micromamba run -n "$e" python -V >/dev/null 2>&1 && { echo "$e"; return; }; done
    echo "default"; return
  fi
  for e in "${candidates[@]}"; do
    micromamba run -n "$e" python - <<PY >/dev/null 2>&1 && { echo "$e"; exit 0; }
from packaging.specifiers import SpecifierSet
import sys
spec = SpecifierSet("$spec")
v = f"{sys.version_info[0]}.{sys.version_info[1]}"
raise SystemExit(0 if spec.contains(v, prereleases=True) else 1)
PY
  done
  for e in "${candidates[@]}"; do micromamba run -n "$e" python -V >/dev/null 2>&1 && { echo "$e"; return; }; done
  echo "default"
}

REPO_PY_SPEC="$(_detect_repo_python_spec)"
ENV_NAME="$(_pick_env_by_spec "$REPO_PY_SPEC")"
set +u; micromamba activate "$ENV_NAME" 2>/dev/null || true; set -u
# ---------------------------------------------------------------------------------------

CURR_DIR="$(pwd)"
cd /workspace/repo

mkdir -p "$LOG_DIR" "$RESULTS_DIR"
PYTEST_LOG="${LOG_DIR}/pytest.log"
JUNIT_XML="${LOG_DIR}/junit.xml"
EXIT_CODE_FILE="${LOG_DIR}/exit_code.txt"
CMD_FILE="${LOG_DIR}/command.txt"

COMMIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
echo "$COMMIT_SHA" > "$BASE_SHA_FILE"
echo "$BRANCH_NAME" > "${LOG_DIR}/branch.txt"
echo "$REPO_ROOT" > "${LOG_DIR}/repo_root.txt"

{
  echo "[env] date: $(date -Is)"
  echo "[env] env_name: $ENV_NAME (requires-python: $REPO_PY_SPEC)"
  echo "[env] python: $(micromamba run -n "$ENV_NAME" python -V 2>/dev/null || python -V)"
  echo "[env] which python: $(micromamba run -n "$ENV_NAME" which python 2>/dev/null || which python)"
  micromamba run -n "$ENV_NAME" python -m pip freeze 2>/dev/null || true
} > "${LOG_DIR}/environment.txt" || true

# Determine run directory/target
RUN_DIR="/workspace/repo"
TARGET="$TEST_SUITE"
if [ -d "$TEST_SUITE" ]; then RUN_DIR="$(realpath "$TEST_SUITE")"; TARGET="."; fi

# IMPORTANT: Do NOT auto-map bare names to /workspace/repo/<name>.
# Bare names are treated as import targets to prefer installed-package tests.

# SciPy detection: modern and legacy
IS_SCIPY_NEW=0; IS_SCIPY_OLD=0
if [ -d "/workspace/repo/scipy" ]; then
  [ -f "/workspace/repo/dev.py" ] && IS_SCIPY_NEW=1
  [ -f "/workspace/repo/runtests.py" ] && IS_SCIPY_OLD=1
fi

# Bare package import name?
USE_PYARGS=0
if [[ "$TARGET" != "." && "$TARGET" != */* && ! -e "$TARGET" ]]; then USE_PYARGS=1; fi
# Special-cases (may flip mode/run dir)
if [[ "$TARGET" == "MDAnalysis" || "$TARGET" == "mdanalysis" ]]; then
  echo "[run-tests] MDAnalysis detected; switching to MDAnalysisTests"
  TARGET="MDAnalysisTests"; USE_PYARGS=1
fi
if [[ "$TARGET" == "pillow" || "$TARGET" == "Pillow" ]]; then
  if [ -d "/workspace/repo/Tests" ]; then
    echo "[run-tests] Pillow repo detected; using Tests/"
    RUN_DIR="/workspace/repo/Tests"; TARGET="."; USE_PYARGS=0
  fi
fi
if [[ "$TARGET" == "apache_beam" || "$TARGET" == "beam" ]]; then
  if [ -d "/workspace/repo/sdks/python" ]; then
    echo "[run-tests] Apache Beam repo detected; using sdks/python"
    RUN_DIR="/workspace/repo/sdks/python"; TARGET="."; USE_PYARGS=0
  fi
fi
if [[ "$TARGET" == "pyarrow" || "$TARGET" == "arrow" ]]; then
  [ -d "/workspace/repo/testing/data" ] && export ARROW_TEST_DATA="/workspace/repo/testing/data"
  [ -d "/workspace/repo/cpp/submodules/parquet-testing/data" ] && export PARQUET_TEST_DATA="/workspace/repo/cpp/submodules/parquet-testing/data"
  echo "[run-tests] pyarrow data: ARROW_TEST_DATA=${ARROW_TEST_DATA:-unset} PARQUET_TEST_DATA=${PARQUET_TEST_DATA:-unset}"
fi

# napari headless
USE_XVFB=0
if [[ "$TARGET" == "napari" || "$TARGET" == napari* ]]; then
  export NAPARI_HEADLESS=1
  export QT_API="${QT_API:-pyqt5}"
  if ! env | grep -q '^DISPLAY=' && command -v xvfb-run >/dev/null 2>&1; then USE_XVFB=1; fi
fi

# Base pytest args
PYTEST_ARGS=(-rA --junitxml "$JUNIT_XML")
[ -n "${PYTEST_K:-}" ] && PYTEST_ARGS+=(-k "$PYTEST_K")
[ -n "${MAXFAIL:-}" ] && PYTEST_ARGS+=(--maxfail "$MAXFAIL")
case "$TARGET" in
  pandas) PYTEST_ARGS+=(-n auto) ;;
  sklearn|scikit-learn|sklearn/*) PYTEST_ARGS+=(-Werror::FutureWarning) ;;
esac
# Append user args last (highest precedence)
[ ${#ADDITIONAL_PYTEST_ARGS[@]} -gt 0 ] && PYTEST_ARGS+=("${ADDITIONAL_PYTEST_ARGS[@]}")

echo "[run-tests] repo_root=$REPO_ROOT" | tee -a "$PYTEST_LOG"
echo "[run-tests] log_dir=$LOG_DIR results_dir=$RESULTS_DIR" | tee -a "$PYTEST_LOG"
echo "[run-tests] env=$ENV_NAME detected: IS_SCIPY_NEW=$IS_SCIPY_NEW IS_SCIPY_OLD=$IS_SCIPY_OLD USE_PYARGS=$USE_PYARGS RUN_DIR=$RUN_DIR TARGET=$TARGET" | tee -a "$PYTEST_LOG"

set +e
if [ "$IS_SCIPY_NEW" -eq 1 ]; then
  echo "[run-tests] SciPy (dev.py): python dev.py test -- ${PYTEST_ARGS[*]}" | tee -a "$PYTEST_LOG"
  printf 'python dev.py test -- %q ' "${PYTEST_ARGS[@]}" > "$CMD_FILE"
  micromamba run -n "$ENV_NAME" python dev.py test -- "${PYTEST_ARGS[@]}" | tee -a "$PYTEST_LOG"
  TEST_RC="${PIPESTATUS[0]}"

elif [ "$IS_SCIPY_OLD" -eq 1 ]; then
  echo "[run-tests] SciPy (legacy runtests.py)" | tee -a "$PYTEST_LOG"
  if grep -q "pytest" "/workspace/repo/runtests.py" 2>/dev/null; then
    FULL_CMD=(python runtests.py -- "${PYTEST_ARGS[@]}")
  else
    FULL_CMD=(python runtests.py)
  fi
  echo "[run-tests] Running: ${FULL_CMD[*]}" | tee -a "$PYTEST_LOG"
  printf '%q ' "${FULL_CMD[@]}" > "$CMD_FILE"
  micromamba run -n "$ENV_NAME" "${FULL_CMD[@]}" | tee -a "$PYTEST_LOG"
  TEST_RC="${PIPESTATUS[0]}"

elif [[ "${RUN_MPI_TESTS:-0}" -eq 1 && "$TARGET" == "h5py" ]]; then
  NP="${MPI_NP:-2}"
  FULL_CMD=(mpirun -n "$NP" python -m pytest --with-mpi "${PYTEST_ARGS[@]}" --pyargs h5py)
  echo "[run-tests] Running (MPI): ${FULL_CMD[*]}" | tee -a "$PYTEST_LOG"
  printf '%q ' "${FULL_CMD[@]}" > "$CMD_FILE"
  micromamba run -n "$ENV_NAME" "${FULL_CMD[@]}" | tee -a "$PYTEST_LOG"
  TEST_RC="${PIPESTATUS[0]}"

else
  if [ "$USE_PYARGS" -eq 1 ]; then
    FULL_CMD=(pytest "${PYTEST_ARGS[@]}" --pyargs "$TARGET")
    [ "$USE_XVFB" -eq 1 ] && FULL_CMD=(xvfb-run -a "${FULL_CMD[@]}")
    echo "[run-tests] Running (installed): ${FULL_CMD[*]}" | tee -a "$PYTEST_LOG"
    printf '%q ' "${FULL_CMD[@]}" > "$CMD_FILE"
    micromamba run -n "$ENV_NAME" "${FULL_CMD[@]}" | tee -a "$PYTEST_LOG"
    TEST_RC="${PIPESTATUS[0]}"
  else
    OLD_DIR="$(pwd)"; cd "$RUN_DIR"
    FULL_CMD=(pytest "${PYTEST_ARGS[@]}" "$TARGET")
    [ "$USE_XVFB" -eq 1 ] && FULL_CMD=(xvfb-run -a "${FULL_CMD[@]}")
    echo "[run-tests] Running (in-tree): (cd $RUN_DIR && ${FULL_CMD[*]})" | tee -a "$PYTEST_LOG"
    printf '(cd %q && ' "$RUN_DIR" > "$CMD_FILE"; printf '%q ' "${FULL_CMD[@]}" >> "$CMD_FILE"; printf ')' >> "$CMD_FILE"
    micromamba run -n "$ENV_NAME" "${FULL_CMD[@]}" | tee -a "$PYTEST_LOG"
    TEST_RC="${PIPESTATUS[0]}"
    cd "$OLD_DIR"
  fi
fi
set -e

echo "$TEST_RC" > "$EXIT_CODE_FILE"
tar -czf "$TARBALL" "$LOG_DIR"
rm -rf "$LOG_DIR"

cd "$CURR_DIR"
echo "[run-tests] Pytest completed with exit code $TEST_RC. Artifact: $TARBALL"
exit "$TEST_RC"
