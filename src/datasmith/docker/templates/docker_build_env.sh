#!/usr/bin/env bash
# docker_build_env.sh (drop-in replacement)
set -euo pipefail

# shellcheck disable=SC1091
source /etc/profile.d/asv_utils.sh || true
# Defense in depth: if the profile.d helper was somehow never installed,
# fall back to a no-op so a bare `fc_note ...` call can never abort this
# script under `set -euo pipefail` (command-not-found = exit 127).
command -v fc_note >/dev/null 2>&1 || fc_note() { :; }
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

# Accept: empty string, JSON object {"dependencies": [...]}, or JSON array [...]
if raw_stripped == "" or raw_stripped == '""':
    pass
else:
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"ERROR: invalid JSON in {path}: {e}", file=sys.stderr)
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

    if isinstance(data, list):
        # Flat list of dependency strings: ["pkg==1.0", "pkg2==2.0"]
        for i, item in enumerate(data):
            if not isinstance(item, str):
                print(f"ERROR: array element [{i}] is not a string", file=sys.stderr)
                sys.exit(3)
            s = item.strip()
            if s:
                dependencies.append(s)
    elif isinstance(data, dict):
        dependencies = take_list("dependencies")
    else:
        print("ERROR: env payload must be a JSON object, array, or empty string", file=sys.stderr)
        sys.exit(3)

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

# Clone external benchmarks so the ASV config file (asv.*.json) is discoverable
# below.  The pkg stage also clones these in an agent-editable section so the
# synthesizer can adjust URLs/branches if needed.
if [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/dask(\.git)?$ ]]; then
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/repo
    cp -r /tmp/repo/dask/* /workspace/repo/
    rm -rf /tmp/repo
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/distributed(\.git)?$ ]]; then
    git clone --depth 1 https://github.com/dask/dask-benchmarks.git /tmp/repo
    cp -r /tmp/repo/distributed/* /workspace/repo/
    rm -rf /tmp/repo
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/joblib/joblib(\.git)?$ ]]; then
    git clone --depth 1 https://github.com/pierreglaser/joblib_benchmarks.git /tmp/repo
    cp -r /tmp/repo/* /workspace/repo/
    rm -rf /tmp/repo
elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/astropy/astropy(\.git)?$ ]]; then
    git clone -b main --depth 1 --single-branch https://github.com/astropy/astropy-benchmarks.git
fi

IMPORT_NAME="$(detect_import_name || true)"
if [[ -z "$IMPORT_NAME" ]]; then
    echo "WARN: Could not determine import name; the pkg stage will fall back to local detection."
fi

# NOTE: asv.*.json discovery is deliberately NOT done here.  Some repos
# (Qiskit, Dask, Astropy, …) keep their ASV config in a separate benchmark
# repo that is cloned by docker_build_pkg.sh, which runs after this stage.
# Gating the env stage on asv.*.json would make those repos unreachable.
source /etc/profile.d/asv_build_vars.sh || true
PY_VERSIONS="${PY_VERSION:-${ASV_PY_VERSIONS:-}}"

if [[ -z "$PY_VERSIONS" ]]; then
    echo "No PY_VERSIONS configured (set PY_VERSION or ASV_PY_VERSIONS)." >&2
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
    # Uninstall PUT by uv pip and micromamba to ensure clean re-install.
    #
    # --no-prune-deps is load-bearing. micromamba prunes dependencies by
    # default, so removing the package under test also removes everything that
    # was pulled in only for it. That cascade can take the environment's own
    # interpreter with it: apache/arrow#1646 unlinked libarchive here and then
    # died several steps later on "No virtual environment or system Python
    # installation found for path /opt/conda/envs/asv_3.8/bin/python". We want
    # exactly one package gone, never its dependency closure.
    [ -n "$PKG_NAME" ]    && uv pip uninstall --python "$PYTHON_BIN" "$PKG_NAME"    || true
    [ -n "$IMPORT_NAME" ] && uv pip uninstall --python "$PYTHON_BIN" "$IMPORT_NAME" || true
    [ -n "$PKG_NAME" ]    && micromamba remove -n "$e" -y --no-prune-deps "$PKG_NAME"    || true
    [ -n "$IMPORT_NAME" ] && micromamba remove -n "$e" -y --no-prune-deps "$IMPORT_NAME" || true

    # Every command above ends in `|| true`, so a removal that damages the
    # environment is silent here and surfaces much later as an unrelated uv
    # error. Check the interpreter still runs, and name the cause if it does not.
    if ! "$PYTHON_BIN" -c "import sys" >/dev/null 2>&1; then
        echo "Error: removing '$PKG_NAME'/'$IMPORT_NAME' broke the interpreter in '$e'." >&2
        echo "       $PYTHON_BIN no longer runs. This is a package-removal cascade," >&2
        echo "       not a dependency-resolution failure." >&2
        exit 1
    fi
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

# ── Manifest breadcrumbs: requested vs. resolved pins ─────────────────
# NOTE: $ENV_PAYLOAD is deliberately NOT used here. By this point in the
# script it holds the *path* docker_build_env.sh was invoked with
# (`--env-payload /tmp/env_payload.json`, see the arg-parsing block above,
# which reassigns the exported ENV_PAYLOAD build ARG to "$2"), not the raw
# JSON payload text. $DEPENDENCIES_PATH is the flattened, one-spec-per-line
# file that was actually fed to `uv pip install -r`, which is a more direct
# and reliable source for "what was requested" than re-parsing JSON out of
# an env var that no longer holds it.
fc_note pins_requested="$(tr '\n' ' ' < "$DEPENDENCIES_PATH" 2>/dev/null || true)"
fc_note pins_resolved="$(micromamba run -n "$ENV_NAME" python -m pip freeze 2>/dev/null | tr '\n' ' ' || true)"


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
