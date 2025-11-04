#!/usr/bin/env bash
# docker_build_env.sh (drop-in replacement)
set -euo pipefail

# shellcheck disable=SC1091
source /etc/profile.d/asv_utils.sh || true
micromamba activate base

# -------------------------- ARG PARSING --------------------------
ENV_PAYLOAD=""

usage() {
  cat >&2 <<'USAGE'
Usage: $(basename "$0") [--env-payload PATH|-e PATH]

Options:
  -e, --env-payload  Path to a json file (or "-" for stdin) containing the
                     environment payload: either "" or a JSON object with two
                     lists of strings, "constraints" and "to_install".
                     Example:
                       ""
                     or
                       {
                       "dependencies": ["pkgA==1.2.3","pkgB==4.5.6"],
                        }

This script expects an env payload because dependency discovery and
time-capped pinning are handled upstream.
USAGE
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -e|--env-payload)
      [[ $# -ge 2 ]] || usage
      ENV_PAYLOAD="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "Unknown argument: $1" >&2; usage;;
  esac
done

[[ -n "${ENV_PAYLOAD}" ]] || { echo "Error: --env-payload is required." >&2; usage; }
[[ "${ENV_PAYLOAD}" == "-" || -f "${ENV_PAYLOAD}" ]] || { echo "Error: env payload path not found: ${ENV_PAYLOAD}" >&2; exit 2; }

parse_env_payload() {
  local path="$1"
  python - "$path" <<'PY'
import json
import os
import sys
import tempfile

path = sys.argv[1]
if path == "-":
    raw = sys.stdin.read()
else:
    with open(path, encoding="utf-8") as f:
        raw = f.read()

raw_stripped = raw.strip()

dependencies = []

# Accept either empty string ("" or blank) or a JSON object
if raw_stripped == "" or raw_stripped == '""':
    pass
else:
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"ERROR: invalid JSON in {path}: {e}", file=sys.stderr)
        sys.exit(3)
    if not isinstance(data, dict):
        print("ERROR: env payload must be a JSON object or an empty string", file=sys.stderr)
        sys.exit(3)

    def take_list(key):
        v = data.get(key, [])
        if v is None:
            v = []
        if not isinstance(v, list):
            print(f"ERROR: '{key}' must be a list of strings", file=sys.stderr)
            sys.exit(3)
        out = []
        for i, item in enumerate(v):
            if not isinstance(item, str):
                print(f"ERROR: '{key}[{i}]' is not a string", file=sys.stderr)
                sys.exit(3)
            s = item.strip()
            if s:
                out.append(s)
        return out

    dependencies = take_list("dependencies")

# Write temp files
d_fd, d_path = tempfile.mkstemp(prefix="dependencies-", suffix=".txt")
with os.fdopen(d_fd, "w", encoding="utf-8") as df:
    for line in dependencies:
        df.write(line.rstrip() + "\n")
print(d_path)
PY
}

# Capture the two generated paths safely
mapfile -t __env_paths < <(parse_env_payload "${ENV_PAYLOAD}")
if [[ "${#__env_paths[@]}" -ne 1 ]]; then
  echo "Error: failed to parse env payload into two paths." >&2
  exit 3
fi

DEPENDENCIES_PATH="${__env_paths[0]}"

# CONSTRAINTS_PATH="${__env_paths[0]}"
# TO_INSTALL_PATH="${__env_paths[1]}"
# BANNED_PATH="${__env_paths[2]}"

# [[ -f "${CONSTRAINTS_PATH}" ]] || { echo "Error: constraints file missing: ${CONSTRAINTS_PATH}" >&2; exit 3; }
# [[ -f "${TO_INSTALL_PATH}"  ]] || { echo "Error: to-install file missing: ${TO_INSTALL_PATH}"  >&2; exit 3; }
# [[ -f "${BANNED_PATH}"     ]] || { echo "Error: banned file missing: ${BANNED_PATH}"     >&2; exit 3; }

# -------------------------- HELPERS --------------------------
write_build_vars() {
  local py_versions="$1"
  local import_name="$2"

  mkdir -p /etc/asv_env
  echo "$py_versions" > /etc/asv_env/py_versions
  echo "$import_name" > /etc/asv_env/import_name

  cat >/etc/profile.d/asv_build_vars.sh <<EOF
# Auto-generated during docker_build_env.sh
export ASV_PY_VERSIONS="${py_versions}"
export IMPORT_NAME="${import_name}"
EOF
}

append_install_vars() {
  local extras_all="$1"
  local setuppy_cmd="$2"

  mkdir -p /etc/asv_env
  printf "%s\n" "$extras_all" > /etc/asv_env/extras_all
  printf "%s\n" "$setuppy_cmd" > /etc/asv_env/setuppy_cmd

  cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export ALL_EXTRAS="${extras_all}"
export SAVED_SETUPPY_CMD="${setuppy_cmd}"
EOF
}

# -------------------------- ENV DISCOVERY --------------------------

URL=$(git remote -v | grep "(fetch)" | awk '{print $2}')

# Optional benchmark grafting left intact
if [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/dask(\.git)?$ ]]; then
    git clone https://github.com/dask/dask-benchmarks.git /tmp/repo
    cp -r /tmp/repo/dask/* /workspace/repo/
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/distributed(\.git)?$ ]]; then
    git clone https://github.com/dask/dask-benchmarks.git /tmp/repo
    cp -r /tmp/repo/distributed/* /workspace/repo/
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/joblib/joblib(\.git)?$ ]]; then
    git clone https://github.com/pierreglaser/joblib_benchmarks.git /tmp/repo
    cp -r /tmp/repo/* /workspace/repo/
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/astropy/astropy(\.git)?$ ]]; then
    git clone -b main https://github.com/astropy/astropy-benchmarks.git --single-branch
fi

IMPORT_NAME="$(detect_import_name || true)"
if [[ -z "$IMPORT_NAME" ]]; then
    echo "WARN: Could not determine import name; the pkg stage will fall back to local detection."
fi

cd_asv_json_dir || { echo "No 'asv.*.json' file found." >&2; exit 1; }

CONF_NAME="$(asv_conf_name || true)"
if [[ -z "${CONF_NAME:-}" ]]; then
    echo "No 'asv.*.json' file found." >&2
    exit 1
fi

# PY_VERSIONS=$(python - <<PY
# import asv
# cfg = asv.config.Config.load("$CONF_NAME")
# cfg.pythons = [v for v in cfg.pythons if tuple(map(int, v.split('.'))) >= (3,7)]
# print(" ".join(cfg.pythons))
# PY
# )
source /etc/profile.d/asv_build_vars.sh || true
PY_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"

if [[ -z "$PY_VERSIONS" ]]; then
    echo "No Satisfying PY_VERSIONS found in $CONF_NAME" >&2
    cat "$CONF_NAME" >&2
    exit 1
fi

for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
        echo "Error: Expected micromamba environment '$ENV_NAME' not found."
        exit 1
    fi
done

cd /workspace/repo

# Extract commit date and set UV_EXCLUDE_NEWER for time-consistent package resolution
# COMMIT_DATE=$(git log -1 --format=%cI 2>/dev/null || echo "")
# if [[ -n "$COMMIT_DATE" ]]; then
#     COMMIT_DATE_BUFFER=$(date -d "$COMMIT_DATE +1 year" -Iseconds)
#     export UV_EXCLUDE_NEWER="$COMMIT_DATE_BUFFER"
#     echo "Using UV_EXCLUDE_NEWER=$UV_EXCLUDE_NEWER for time-capped resolution"
# fi

# write_vars "UV_EXCLUDE_NEWER" "$UV_EXCLUDE_NEWER"

# Derive package name (for uninstall sweep)
if [ -f pyproject.toml ]; then
    PKG_NAME=$(python -c "import tomli; d=tomli.load(open('pyproject.toml','rb')); print(d.get('project',{}).get('name',''))")
elif [ -f setup.cfg ]; then
    PKG_NAME=$(python -c "from setuptools.config import read_configuration as r;print(r('setup.cfg')['metadata'].get('name',''))")
elif [ -f setup.py ]; then
    PKG_NAME=$(python setup.py --name 2>/dev/null || true)
else
    PKG_NAME=""
fi

for v in $PY_VERSIONS; do
    e="asv_$v"
    micromamba env list | awk '{print $1}' | grep -qx "$e" || { echo "missing $e"; exit 1; }
    PYTHON_BIN="/opt/conda/envs/$e/bin/python"
    # Uninstall PUT by uv pip and micromamba to ensure clean re-install
    [ -n "$PKG_NAME" ]    && uv pip uninstall --python "$PYTHON_BIN" "$PKG_NAME"    || true
    [ -n "$IMPORT_NAME" ] && uv pip uninstall --python "$PYTHON_BIN" "$IMPORT_NAME" || true
    [ -n "$PKG_NAME" ]    && micromamba remove -n "$e" -y "$PKG_NAME"    || true
    [ -n "$IMPORT_NAME" ] && micromamba remove -n "$e" -y "$IMPORT_NAME" || true
done


export UV_NO_PROGRESS=1

uv_install() {
  local python_bin=$1; shift
  # Try with build isolation first; on failure, retry without it.
  uv pip install --python "$python_bin" "$@" || \
  uv pip install --python "$python_bin" --no-build-isolation "$@"
}

UV_OPTS=(--upgrade)
for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    PYTHON_BIN="/opt/conda/envs/$ENV_NAME/bin/python"
    echo "Installing dependencies in $ENV_NAME from $DEPENDENCIES_PATH"
    if [ -s "$DEPENDENCIES_PATH" ]; then
        # Ensure build tooling is present inside target env
        uv_install "$PYTHON_BIN" --upgrade pip setuptools wheel >/dev/null 2>&1 || true

        # If a numpy pin exists, install it first to satisfy legacy build chains
        if grep -Eqi '^\s*numpy(==|>=|<=|~=|>|<|\s|$)' "$DEPENDENCIES_PATH"; then
            # Collect numpy spec lines and install them up-front
            mapfile -t __npys < <(awk 'BEGIN{IGNORECASE=1} $0 ~ /^[[:space:]]*numpy(==|>=|<=|~=|>|<|[[:space:]]|$)/{gsub(/^[[:space:]]+|[[:space:]]+$/,"",$0); print $0}' "$DEPENDENCIES_PATH")
            if (( ${#__npys[@]} > 0 )); then
                uv_install "$PYTHON_BIN" "${__npys[@]}" || true
            fi
        fi

        # Install the full resolved set (build isolation preferred via uv_install)
        uv_install "$PYTHON_BIN" "${UV_OPTS[@]}" -r "$DEPENDENCIES_PATH"
    fi
done


# -------------------------- PERSIST METADATA --------------------------
# Persist base variables for downstream stages
write_build_vars "$PY_VERSIONS" "${IMPORT_NAME:-}"

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
# Extras detection retained (metadata only)
ALL_EXTRAS="$(detect_extras --repo-root "$REPO_ROOT" 2>/dev/null | tr -s ' ' | tr ' ' ',')"
echo $ALL_EXTRAS

SETUPPY_CMD="develop"
append_install_vars "${ALL_EXTRAS}" "${SETUPPY_CMD}"

# Snapshot each env's from-history packages
for version in $PY_VERSIONS; do
  ENV_NAME="asv_${version}"
  micromamba -n "$ENV_NAME" env export --from-history > "/etc/asv_env/installed_packages_${version}"
done

echo "Environment setup complete."
