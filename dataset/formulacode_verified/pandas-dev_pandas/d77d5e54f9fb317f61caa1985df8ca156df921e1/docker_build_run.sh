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

# Ensure conda-provided libstdc++ is preferred to avoid ABI mismatches at runtime.
ENV_PREFIX="/opt/conda/envs/${ENV_NAME}"
if [[ -d "${ENV_PREFIX}/lib" ]]; then
  cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export LD_LIBRARY_PATH="${ENV_PREFIX}/lib:${LD_LIBRARY_PATH:-}"
EOF

  # Ensure conda backup vars exist during deactivate when tests run with set -u.
  mkdir -p "${ENV_PREFIX}/etc/conda/activate.d"
  cat >"${ENV_PREFIX}/etc/conda/activate.d/00-toolchain-defaults.sh" <<'EOF'
export HOST="${HOST-}"
export BUILD="${BUILD-}"
export CONDA_TOOLCHAIN_HOST="${CONDA_TOOLCHAIN_HOST-}"
export CONDA_TOOLCHAIN_BUILD="${CONDA_TOOLCHAIN_BUILD-}"
export CC="${CC-}"
export CPP="${CPP-}"
export CC_FOR_BUILD="${CC_FOR_BUILD-}"
export CPP_FOR_BUILD="${CPP_FOR_BUILD-}"
export GCC="${GCC-}"
export GCC_AR="${GCC_AR-}"
export GCC_NM="${GCC_NM-}"
export GCC_RANLIB="${GCC_RANLIB-}"
export CPPFLAGS="${CPPFLAGS-}"
export CFLAGS="${CFLAGS-}"
export LDFLAGS="${LDFLAGS-}"
export LDFLAGS_LD="${LDFLAGS_LD-}"
export DEBUG_CPPFLAGS="${DEBUG_CPPFLAGS-}"
export DEBUG_CFLAGS="${DEBUG_CFLAGS-}"
export CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH-}"
export CONDA_BUILD_CROSS_COMPILATION="${CONDA_BUILD_CROSS_COMPILATION-}"
export CONDA_BUILD_SYSROOT="${CONDA_BUILD_SYSROOT-}"
export build_alias="${build_alias-}"
export host_alias="${host_alias-}"
export MESON_ARGS="${MESON_ARGS-}"
export CMAKE_ARGS="${CMAKE_ARGS-}"
export GFORTRAN="${GFORTRAN-}"
export F95="${F95-}"
export FC="${FC-}"
export F77="${F77-}"
export F90="${F90-}"
export FC_FOR_BUILD="${FC_FOR_BUILD-}"
export FFLAGS="${FFLAGS-}"
export FORTRANFLAGS="${FORTRANFLAGS-}"
export DEBUG_FFLAGS="${DEBUG_FFLAGS-}"
export DEBUG_FORTRANFLAGS="${DEBUG_FORTRANFLAGS-}"
export CXX="${CXX-}"
export CXX_FOR_BUILD="${CXX_FOR_BUILD-}"
export GXX="${GXX-}"
export CXXFLAGS="${CXXFLAGS-}"
export DEBUG_CXXFLAGS="${DEBUG_CXXFLAGS-}"
export ADDR2LINE="${ADDR2LINE-}"
export AR="${AR-}"
export CXXFILT="${CXXFILT-}"
export ELFEDIT="${ELFEDIT-}"
export NM="${NM-}"
export OBJCOPY="${OBJCOPY-}"
export OBJDUMP="${OBJDUMP-}"
export RANLIB="${RANLIB-}"
export READELF="${READELF-}"
export SIZE="${SIZE-}"
export STRINGS="${STRINGS-}"
export STRIP="${STRIP-}"
export AS="${AS-}"
export GPROF="${GPROF-}"
export LD="${LD-}"
EOF
  cat >"${ENV_PREFIX}/etc/conda/activate.d/conda-backup-defaults.sh" <<'EOF'
export CONDA_BACKUP_CXX="${CONDA_BACKUP_CXX-}"
export CONDA_BACKUP_CXX_FOR_BUILD="${CONDA_BACKUP_CXX_FOR_BUILD-}"
export CONDA_BACKUP_GXX="${CONDA_BACKUP_GXX-}"
export CONDA_BACKUP_CXXFLAGS="${CONDA_BACKUP_CXXFLAGS-}"
export CONDA_BACKUP_DEBUG_CXXFLAGS="${CONDA_BACKUP_DEBUG_CXXFLAGS-}"
export CONDA_BACKUP_GFORTRAN="${CONDA_BACKUP_GFORTRAN-}"
export CONDA_BACKUP_F95="${CONDA_BACKUP_F95-}"
export CONDA_BACKUP_FC="${CONDA_BACKUP_FC-}"
export CONDA_BACKUP_F77="${CONDA_BACKUP_F77-}"
export CONDA_BACKUP_F90="${CONDA_BACKUP_F90-}"
export CONDA_BACKUP_FC_FOR_BUILD="${CONDA_BACKUP_FC_FOR_BUILD-}"
export CONDA_BACKUP_FFLAGS="${CONDA_BACKUP_FFLAGS-}"
export CONDA_BACKUP_FORTRANFLAGS="${CONDA_BACKUP_FORTRANFLAGS-}"
export CONDA_BACKUP_DEBUG_FFLAGS="${CONDA_BACKUP_DEBUG_FFLAGS-}"
export CONDA_BACKUP_DEBUG_FORTRANFLAGS="${CONDA_BACKUP_DEBUG_FORTRANFLAGS-}"
export CONDA_BACKUP_CC="${CONDA_BACKUP_CC-}"
export CONDA_BACKUP_CFLAGS="${CONDA_BACKUP_CFLAGS-}"
export CONDA_BACKUP_DEBUG_CFLAGS="${CONDA_BACKUP_DEBUG_CFLAGS-}"
export CONDA_BACKUP_CPPFLAGS="${CONDA_BACKUP_CPPFLAGS-}"
export CONDA_BACKUP_LD="${CONDA_BACKUP_LD-}"
export CONDA_BACKUP_LDFLAGS="${CONDA_BACKUP_LDFLAGS-}"
export CONDA_BACKUP_LDFLAGS_LD="${CONDA_BACKUP_LDFLAGS_LD-}"
export CONDA_BACKUP_DEBUG_CPPFLAGS="${CONDA_BACKUP_DEBUG_CPPFLAGS-}"
export CONDA_BACKUP_CMAKE_PREFIX_PATH="${CONDA_BACKUP_CMAKE_PREFIX_PATH-}"
export CONDA_BACKUP_CONDA_BUILD_CROSS_COMPILATION="${CONDA_BACKUP_CONDA_BUILD_CROSS_COMPILATION-}"
export CONDA_BACKUP_BUILD_ALIAS="${CONDA_BACKUP_BUILD_ALIAS-}"
export CONDA_BACKUP_HOST_ALIAS="${CONDA_BACKUP_HOST_ALIAS-}"
export CONDA_BACKUP_build_alias="${CONDA_BACKUP_build_alias-}"
export CONDA_BACKUP_host_alias="${CONDA_BACKUP_host_alias-}"
export CONDA_BACKUP_MESON_ARGS="${CONDA_BACKUP_MESON_ARGS-}"
export CONDA_BACKUP_CMAKE_ARGS="${CONDA_BACKUP_CMAKE_ARGS-}"
export CONDA_BACKUP_HOST="${CONDA_BACKUP_HOST-}"
export CONDA_BACKUP_BUILD="${CONDA_BACKUP_BUILD-}"
export CONDA_BACKUP_CONDA_TOOLCHAIN_HOST="${CONDA_BACKUP_CONDA_TOOLCHAIN_HOST-}"
export CONDA_BACKUP_CONDA_TOOLCHAIN_BUILD="${CONDA_BACKUP_CONDA_TOOLCHAIN_BUILD-}"
export CONDA_BACKUP_CPP="${CONDA_BACKUP_CPP-}"
export CONDA_BACKUP_CC_FOR_BUILD="${CONDA_BACKUP_CC_FOR_BUILD-}"
export CONDA_BACKUP_CPP_FOR_BUILD="${CONDA_BACKUP_CPP_FOR_BUILD-}"
export CONDA_BACKUP_GCC="${CONDA_BACKUP_GCC-}"
export CONDA_BACKUP_GCC_AR="${CONDA_BACKUP_GCC_AR-}"
export CONDA_BACKUP_GCC_NM="${CONDA_BACKUP_GCC_NM-}"
export CONDA_BACKUP_GCC_RANLIB="${CONDA_BACKUP_GCC_RANLIB-}"
export CONDA_BACKUP_AR="${CONDA_BACKUP_AR-}"
export CONDA_BACKUP_AS="${CONDA_BACKUP_AS-}"
export CONDA_BACKUP_NM="${CONDA_BACKUP_NM-}"
export CONDA_BACKUP_OBJCOPY="${CONDA_BACKUP_OBJCOPY-}"
export CONDA_BACKUP_OBJDUMP="${CONDA_BACKUP_OBJDUMP-}"
export CONDA_BACKUP_RANLIB="${CONDA_BACKUP_RANLIB-}"
export CONDA_BACKUP_READELF="${CONDA_BACKUP_READELF-}"
export CONDA_BACKUP_STRIP="${CONDA_BACKUP_STRIP-}"
export CONDA_BACKUP_ADDR2LINE="${CONDA_BACKUP_ADDR2LINE-}"
export CONDA_BACKUP_CXXFILT="${CONDA_BACKUP_CXXFILT-}"
export CONDA_BACKUP_ELFEDIT="${CONDA_BACKUP_ELFEDIT-}"
export CONDA_BACKUP_SIZE="${CONDA_BACKUP_SIZE-}"
export CONDA_BACKUP_STRINGS="${CONDA_BACKUP_STRINGS-}"
export CONDA_BACKUP_GPROF="${CONDA_BACKUP_GPROF-}"
export CONDA_BACKUP_CONDA_BUILD_SYSROOT="${CONDA_BACKUP_CONDA_BUILD_SYSROOT-}"
EOF
fi



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
