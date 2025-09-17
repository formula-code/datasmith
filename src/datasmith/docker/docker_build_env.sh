#!/usr/bin/env bash
set -euo pipefail

# -------- Persisted build variables --------
write_build_vars() {
    local py_versions="$1"
    local import_name="$2"

    mkdir -p /etc/asv_env
    echo "$py_versions" > /etc/asv_env/py_versions
    echo "$import_name" > /etc/asv_env/import_name

    # Exported for every future shell (pkg script, interactive, etc.)
    cat >/etc/profile.d/asv_build_vars.sh <<EOF
# Auto-generated during docker_build_env.sh
export ASV_PY_VERSIONS="${py_versions}"
export IMPORT_NAME="${import_name}"
EOF
}

# Append install-related variables (extras/specs) so the follow-up script can use them.
append_install_vars() {
    local extras_all="$1"
    local setuppy_cmd="$2"

    mkdir -p /etc/asv_env
    printf "%s\n" "$extras_all" > /etc/asv_env/extras_all
    printf "%s\n" "$setuppy_cmd" > /etc/asv_env/setuppy_cmd

    # Export for future shells
    cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export ALL_EXTRAS="${extras_all}"
export SAVED_SETUPPY_CMD="${setuppy_cmd}"
EOF
}

# shellcheck disable=SC1091
source /etc/profile.d/asv_utils.sh || true

lock_repo_to_current_commit() {
  set -euo pipefail

  # 0) Sanity checks
  git rev-parse --git-dir >/dev/null 2>&1 || { echo "Not a git repo"; return 1; }
  if git worktree list 2>/dev/null | awk 'NR>1{exit 1}'; then :; else
    echo "Multiple worktrees detected. Aborting for strictness."; return 1
  fi

  # 1) Decide the branch name (same logic as your snippet)
  BR=$(
    git symbolic-ref --short HEAD 2>/dev/null \
    || git rev-parse --abbrev-ref origin/HEAD 2>/dev/null | cut -d/ -f2- \
    || git remote show origin 2>/dev/null | awk '/HEAD branch/ {print $NF}' \
    || echo main
  )

  # 2) Pin $BR to the current commit and detach from upstream
  HEAD_SHA="$(git rev-parse --verify HEAD)"
  git checkout -B "$BR" "$HEAD_SHA"
  git branch --unset-upstream 2>/dev/null || true

  # 3) Remove ALL remotes so nothing reappears on fetch
  for r in $(git remote); do git remote remove "$r"; done

  # 4) Delete ALL refs except the current branch tip
  #    (branches, tags, remotes, stash, notes, everything)
  while IFS= read -r ref; do
    [[ "$ref" == "refs/heads/$BR" ]] && continue
    git update-ref -d "$ref" || true
  done < <(git for-each-ref --format='%(refname)')

  # Explicitly nuke stash and notes namespaces (in case they were packed)
  git update-ref -d refs/stash 2>/dev/null || true
  while IFS= read -r nref; do git update-ref -d "$nref" || true; done \
    < <(git for-each-ref --format='%(refname)' refs/notes)

  # 5) Remove alternates (could reintroduce hidden objects)
  if [[ -f .git/objects/info/alternates ]]; then
    rm -f .git/objects/info/alternates
  fi

  # 6) Expire ALL reflogs and delete the logs directory for good measure
  git reflog expire --expire=now --expire-unreachable=now --all || true
  rm -rf .git/logs

  # 7) Aggressively prune unreachable objects (including “future” commits)
  git gc --prune=now --aggressive
  git repack -Ad
  git prune --expire=now 2>/dev/null || true

  # 8) Verification: ensure no ref points to a descendant of HEAD
  if while IFS= read -r ref; do
       [[ "$ref" == "refs/heads/$BR" ]] && continue
       # If HEAD is an ancestor of ref, then ref is "ahead" -> fail
       if git merge-base --is-ancestor "$HEAD_SHA" "$(git rev-parse "$ref")"; then
         echo "$ref"
         exit 0
       fi
     done < <(git for-each-ref --format='%(refname)') ; then
     : # no output means OK
   else
     echo "Error: some refs still point ahead of HEAD. Aborting."
     return 1
  fi

  # 9) Success message
  echo "Locked to $HEAD_SHA on branch '$BR'. Older commits remain; newer commits are gone."
}

# Ensure base micromamba is active for introspecting ASV config
micromamba activate base

URL=$(git remote -v | grep "(fetch)" | awk '{print $2}')

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

# Move into the directory that contains asv.*.json
cd_asv_json_dir || { echo "No 'asv.*.json' file found." >&2; exit 1; }

CONF_NAME="$(asv_conf_name || true)"
if [[ -z "${CONF_NAME:-}" ]]; then
    echo "No 'asv.*.json' file found." >&2
    exit 1
fi

# Read python versions from the ASV config
PY_VERSIONS=$(python - <<PY
import asv
cfg = asv.config.Config.load("$CONF_NAME")
# remove any python less than 3.7 (ASV dropped support for 3.6+ in v0.5)
cfg.pythons = [v for v in cfg.pythons if tuple(map(int, v.split('.'))) >= (3,7)]
print(" ".join(cfg.pythons))
PY
)

# If none found, throw a noticeable error and exit
if [[ -z "$PY_VERSIONS" ]]; then
    echo "No Satisfying PY_VERSIONS found in $CONF_NAME" >&2
    # echo asv config for debugging
    cat "$CONF_NAME" >&2
    exit 1
fi

# for each python version, ensure asv_{version} env exists. Otherwise error out.
for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"

    if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
        echo "Error: Expected micromamba environment '$ENV_NAME' not found."
        exit 1
    fi
done

cd /workspace/repo
lock_repo_to_current_commit


CONF_NAME=$(find . -type f -name "asv.*.json" | head -n 1)

install_command=$(python - <<PY
import asv
cfg = asv.config.Config.load("$CONF_NAME")
install_command = cfg.install_command
if install_command is None:
    install_command = []
elif isinstance(install_command, str):
    install_command = [install_command]

print(" ".join(install_command))
PY
)

for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    if [ -n "$install_command" ]; then
    echo "Running install commands from $CONF_NAME:"
    echo "$install_command"
    while IFS= read -r cmd; do
    # if the install command contains "." (i.e. pip install .), remove the dot.
        cmd=$(echo "$cmd" | sed 's/ \.//g')
        echo "Executing: $cmd"
        micromamba run -n "$ENV_NAME" $cmd || true
    done <<< "$install_command"
    fi
done


set -u
export PIP_DISABLE_PIP_VERSION_CHECK=1
# 1) Build a clean constraints file (name==version only)
CONSTRAINTS_FILE="$(mktemp)"
python - <<'PY' > "$CONSTRAINTS_FILE"
import json, subprocess, sys
pkgs = json.loads(subprocess.check_output([sys.executable, "-m", "pip", "list", "--format=json"]))
for d in pkgs:
    print(f"{d['name']}=={d['version']}")
PY
export PIP_CONSTRAINT="$CONSTRAINTS_FILE"
export CONF_NAME="$CONF_NAME"
# 2) Collect matrix packages and append explicit pins
PKG_LIST_FILE="$(mktemp)"
python - <<'PY' | tee "$PKG_LIST_FILE" >/dev/null
import os, asv
conf = asv.config.Config.load(os.environ.get("CONF_NAME","asv.conf.json"))
banned = {"cython","python-build","meson","ninja","setuptools","wheel","pip",
          "virtualenv","micromamba","conda","mamba","git","scipy"}
pkgs, pins = [], []
if conf.matrix:
    for pkg, versions in conf.matrix.items():
        if any(b in pkg.lower() for b in banned): continue
        pinned = (str(versions[0]).strip() if versions and versions[0] else None)
        if pinned:
            pins.append(f"{pkg}=={pinned}")
            pkgs.append(f"{pkg}=={pinned}")
        else:
            pkgs.append(pkg)

with open(os.environ["PIP_CONSTRAINT"], "a", encoding="utf-8") as cf:
    seen=set()
    for p in pins:
        if p not in seen:
            cf.write(p + "\n"); seen.add(p)

print("\n".join(pkgs))
PY


for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    SKIPPED=()
    INSTALLED=()

    while IFS= read -r pkg; do
    [ -z "$pkg" ] && continue
    echo ">> checking $pkg"
    # Use GNU timeout (600s) around the dry-run check
    if timeout 600 micromamba run -n "$ENV_NAME"  python -m pip install --no-cache-dir \
            --upgrade-strategy only-if-needed -c "$CONSTRAINTS_FILE" \
            --dry-run "$pkg" >/dev/null 2>&1; then
        # If the dry run succeeds, do the real install (no timeout here, or add another timeout if desired)
        if micromamba run -n "$ENV_NAME"  python -m pip install --no-cache-dir \
            --upgrade-strategy only-if-needed -c "$CONSTRAINTS_FILE" "$pkg"; then
        INSTALLED+=("$pkg")
        else
        echo "!! install failed for $pkg — skipping."
        SKIPPED+=("$pkg")
        fi
    else
        echo "!! timeout/violation on $pkg — skipping."
        SKIPPED+=("$pkg")
    fi
    done < "$PKG_LIST_FILE"
done

# Persist base variables
write_build_vars "$PY_VERSIONS" "${IMPORT_NAME:-}"
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
ALL_EXTRAS="$(detect_extras --repo-root "$REPO_ROOT" 2>/dev/null | tr -s ' ' | tr ' ' ',')"


SETUPPY_CMD="develop"
append_install_vars "${ALL_EXTRAS}" "${SETUPPY_CMD}"

for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"
    micromamba -n "$ENV_NAME" env export --from-history > /etc/asv_env/installed_packages_$version
done

echo "Environment setup complete."
