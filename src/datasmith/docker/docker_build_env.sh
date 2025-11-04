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
# CONSTRAINTS_FILE="${CONSTRAINTS_PATH}"
# PKG_LIST_FILE="${TO_INSTALL_PATH}"

# echo "Using constraints file: ${CONSTRAINTS_FILE}"
# echo "Using to-install list: ${PKG_LIST_FILE}"

# # Build merged PKG_LIST_FILE by appending only NEW matrix items (by normalized name).
# PKG_LIST_FILE="$(mktemp)"

# python - "$CONF_NAME" "$CONSTRAINTS_PATH" "$TO_INSTALL_PATH" "$IMPORT_NAME" "$PKG_LIST_FILE" "$BANNED_PATH" <<'PY'
# import os, sys, re, json, shutil, io
# from typing import Iterable

# conf_path, constraints_path, to_install_path, import_name, out_pkg_list, banned_path = sys.argv[1:7]

# # --- helpers ---
# try:
#     from packaging.requirements import Requirement
#     from packaging.utils import canonicalize_name
# except Exception:
#     Requirement = None
#     def canonicalize_name(n: str) -> str:
#         return re.sub(r"[-_.]+", "-", n.strip().lower())

# def req_name(line: str) -> str | None:
#     s = line.strip()
#     if not s or "://" in s or s.startswith(("-r ", "--", "-e ")):
#         return None
#     s = s.split("#", 1)[0].split(";", 1)[0].strip()
#     if not s:
#         return None
#     # naive fast path
#     if Requirement:
#         try:
#             return canonicalize_name(Requirement(s).name)
#         except Exception:
#             pass
#     m = re.match(r"^\s*([A-Za-z0-9_.\-]+)", s)
#     return canonicalize_name(m.group(1)) if m else None

# def read_lines(path: str) -> list[str]:
#     if not path or not os.path.isfile(path):
#         return []
#     with open(path, "r", encoding="utf-8") as f:
#         return [ln.rstrip("\n") for ln in f]

# # --- load upstream payload ---
# up_constraints = read_lines(constraints_path)
# up_to_install  = read_lines(to_install_path)
# up_banned = read_lines(banned_path)

# have_names = set()
# for ln in up_constraints:
#     n = req_name(ln)
#     if n: have_names.add(n)
# for ln in up_to_install:
#     n = req_name(ln)
#     if n: have_names.add(n)
# # Ban risky/base tooling and the project-under-test itself
# banned = {
#     "pip","setuptools","wheel","virtualenv","micromamba","conda","mamba","python",
#     "build","python-build","meson","ninja","cython","git","scipy", "pytest-qt"
# }
# for ln in up_banned:
#     n = req_name(ln)
#     if n: banned.add(n)

# if import_name:
#     banned.add(canonicalize_name(import_name))

# # --- read ASV matrix and compute additions ---
# try:
#     import asv
#     conf = asv.config.Config.load(conf_path)
#     matrix = conf.matrix or {}
# except Exception as e:
#     matrix = {}

# extra_constraints: list[str] = []
# extra_installs:   list[str] = []

# def want(name: str) -> bool:
#     n = canonicalize_name(name)
#     return n not in banned and n not in have_names

# for pkg, versions in matrix.items():
#     if not isinstance(pkg, str) or not pkg.strip():
#         continue
#     if not want(pkg):
#         continue
#     pin = None
#     if isinstance(versions, (list, tuple)) and versions:
#         v0 = str(versions[0]).strip()
#         if v0:  # treat first entry as the desired pin
#             pin = f"{pkg}=={v0}"
#     # If pinned and not already pinned upstream, append to constraints
#     if pin:
#         # do not add a duplicate pin for the same normalized name
#         n_pin = req_name(pin)
#         if n_pin and n_pin not in {req_name(x) for x in up_constraints}:
#             extra_constraints.append(pin)
#     # For installs: only add if name isn't already in payload (have_names)
#     # Prefer pinned spec if available, else plain name
#     extra_installs.append(pin if pin else pkg)

# # Deduplicate extra_installs by normalized name and exclude anything already present
# seen_local: set[str] = set()
# dedup_extra_installs: list[str] = []
# for spec in extra_installs:
#     n = req_name(spec)
#     if not n or n in have_names or n in seen_local:
#         continue
#     dedup_extra_installs.append(spec)
#     seen_local.add(n)

# # --- write merged outputs ---
# # 1) append constraints in place (no-op if none)
# if extra_constraints:
#     with open(constraints_path, "a", encoding="utf-8") as f:
#         for ln in extra_constraints:
#             f.write(ln + "\n")

# # 2) write new PKG_LIST_FILE: upstream to_install first, then matrix additions
# with open(out_pkg_list, "w", encoding="utf-8") as f:
#     for ln in up_to_install:
#         if ln.strip():
#             f.write(ln.strip() + "\n")
#     for ln in dedup_extra_installs:
#         f.write(ln + "\n")

# print(out_pkg_list)
# PY

# # Sanity: empty file is fine (means "nothing new to install" beyond upstream)
# [[ -f "$PKG_LIST_FILE" ]] || { echo "ERROR: failed to build PKG_LIST_FILE" >&2; exit 3; }

# # echo "Installing packages in the following environments:" > /etc/asv_env/install_summary.txt
# # make  /etc/asv_env/install_summary.txt
# mkdir -p /etc/asv_env
# touch /etc/asv_env/install_summary.txt
# # Tunables
# CHUNK_SIZE=20          # start chunk size
# UV_OPTS=(--upgrade-strategy only-if-needed --prefer-binary)
# DRY_RUN_OPTS=(--dry-run -q)  # quiet dry-run to reduce IO
# for version in $PY_VERSIONS; do
#   ENV_NAME="asv_${version}"
#   PYTHON_BIN="/opt/conda/envs/$ENV_NAME/bin/python"
#   OK=()
#   BAD=()

#   # First pass: quick screen with a uv pip dry-run (fast, cached)
#   while IFS= read -r pkg || [[ -n "$pkg" ]]; do
#     [[ -z "$pkg" ]] && continue
#     echo ">> [$ENV_NAME] checking $pkg"
#     if timeout 20 uv pip install --python "$PYTHON_BIN" \
#          "${UV_OPTS[@]}" -c "$CONSTRAINTS_FILE" "${DRY_RUN_OPTS[@]}" "$pkg" >/dev/null 2>&1; then
#       OK+=("$pkg")
#     else
#       echo "!! timeout/violation on $pkg — skipping."
#       BAD+=("$pkg")
#     fi
#   done < "$PKG_LIST_FILE"

#   # Try to install all OK in chunks; on conflict, bisect chunk
#   install_chunk() {
#     local -a chunk=("$@")
#     if ( uv pip install --python "$PYTHON_BIN" \
#            "${UV_OPTS[@]}" -c "$CONSTRAINTS_FILE" "${chunk[@]}" ); then
#       return 0
#     fi
#     # Conflict: split if more than one item; otherwise mark skipped
#     if (( ${#chunk[@]} > 1 )); then
#       local mid=$(( ${#chunk[@]} / 2 ))
#       install_chunk "${chunk[@]:0:$mid}" || true
#       install_chunk "${chunk[@]:$mid}" || true
#     else
#       echo "!! install failed for ${chunk[0]} — skipping."
#       BAD+=("${chunk[0]}")
#     fi
#   }

#   # Greedy install with backoff
#   idx=0
#   while (( idx < ${#OK[@]} )); do
#     chunk=( "${OK[@]:$idx:$CHUNK_SIZE}" )
#     (( ${#chunk[@]} == 0 )) && break
#     echo ">> [$ENV_NAME] installing ${#chunk[@]} packages..."
#     install_chunk "${chunk[@]}"
#     idx=$(( idx + CHUNK_SIZE ))
#   done

#   # Summary
#   {
#     echo "Environment: $ENV_NAME"
#     echo -n "Installs: "
#     uv pip list --python "$PYTHON_BIN" --format=freeze | cut -d= -f1 | tr '\n' ' '
#     echo
#     echo "Skipped: ${BAD[*]}"
#   } >> /etc/asv_env/install_summary.txt
# done


# Install any ASV-defined install_command (unchanged)
# CONF_NAME=$(find . -type f -name "asv.*.json" | head -n 1)
# install_command=$(python - <<PY
# import asv
# cfg = asv.config.Config.load("$CONF_NAME")
# install_command = cfg.install_command
# if install_command is None:
#     install_command = []
# elif isinstance(install_command, str):
#     install_command = [install_command]
# print(" ".join(install_command))
# PY
# )
UV_OPTS=(--upgrade)
for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    PYTHON_BIN="/opt/conda/envs/$ENV_NAME/bin/python"
    echo "Installing dependencies in $ENV_NAME from $DEPENDENCIES_PATH"
    if [ -s "$DEPENDENCIES_PATH" ]; then
        uv pip install --python "$PYTHON_BIN" wheel &&\
         uv pip install --python "$PYTHON_BIN" "${UV_OPTS[@]}" -r "$DEPENDENCIES_PATH"
#     if [ -n "$install_command" ]; then
#       echo "Running install commands from $CONF_NAME in $ENV_NAME:"
#       echo "$install_command"
#       while IFS= read -r cmd; do
#         # strip " ." to avoid reinstalling local project here
#         cmd=$(echo "$cmd" | sed 's/ \.//g')
#         echo "Executing: $cmd"
#         micromamba run -n "$ENV_NAME" $cmd || true
#       done <<< "$install_command"
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
