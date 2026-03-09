#!/usr/bin/env bash
set -euo pipefail

cd /workspace/repo || exit 1

source /etc/profile.d/asv_utils.sh || true
source /etc/profile.d/asv_build_vars.sh || true

ROOT_PATH=${ROOT_PATH:-$PWD}         # Usually /workspace/repo
REPO_ROOT="$ROOT_PATH"
version=$(echo $ASV_PY_VERSIONS | awk '{print $1}')
ENV_NAME="asv_${version}"
CONF_NAME=$(find . -type f -name "asv.*.json" | head -n 1)


[ -f docker_build_pkg.sh ] && rm docker_build_pkg.sh
[ -f docker_build_env.sh ] && rm docker_build_env.sh
[ -f /workspace/docker_build_base.sh ] && rm /workspace/docker_build_base.sh


# cd to dirname $CONF_NAME
CWD=$(pwd)
cd "$(dirname "$CONF_NAME")"
asv machine --yes --config "$(basename "$CONF_NAME")" \
  --machine "docker" \
  --num_cpu "1" \
  --ram "4GB"
cd "$CWD"


lock_repo_to_current_commit() {
  set -euo pipefail

  # 0) Sanity checks
  git rev-parse --git-dir >/dev/null 2>&1 || { echo "Not a git repo"; return 1; }
  if git worktree list 2>/dev/null | awk 'NR>1{exit 1}'; then :; else
    echo "Multiple worktrees detected. Aborting for strictness."; return 1
  fi

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

  # 3) (Optional) Remove ALL remotes — leave commented so ASV can fetch if needed
  # for r in $(git remote); do git remote remove "$r"; done

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
  rm -rf .git/logs || true

  # 7) ***Do NOT prune unreachable objects***
  #    Keep objects so raw SHAs remain resolvable for ASV.
  #    Also disable future auto-pruning.
  git config gc.auto 0
  git config gc.pruneExpire never
  git config gc.worktreePruneExpire never
  # You may still repack reachable objects without pruning unreachable ones:
  git repack -Ad --write-bitmap-index || true
  # DO NOT run: git gc --prune=now --aggressive
  # DO NOT run: git prune --expire=now

  # 8) Verification: ensure no ref points to a descendant of HEAD
  if while IFS= read -r ref; do
       [[ "$ref" == "refs/heads/$BR" ]] && continue
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

}

HEAD_SHA="$(git rev-parse --verify HEAD)"

lock_repo_to_current_commit

if [[ ! -f "$REPO_ROOT/tox.ini" ]]; then
  cat > "$REPO_ROOT/tox.ini" <<'EOF'
[pytest]
EOF
fi

# install some band-aid packages
micromamba run -n "$ENV_NAME" uv pip install -q --upgrade jinja2 || true
micromamba run -n "$ENV_NAME" uv pip install -q --upgrade pytest || true


# Persist env variables for future shells and auto-activate
cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export ENV_NAME=$ENV_NAME
export CONF_NAME=$CONF_NAME
export ROOT_PATH=$ROOT_PATH
export REPO_ROOT=$REPO_ROOT
export ASV_PY_VERSIONS="$ASV_PY_VERSIONS"
export HEAD_SHA=$HEAD_SHA
EOF


# Remove the setup script so the agent doesn't see it
rm -- "$0"
