# Sourced by setup.sh and test.sh: sha256 over every file and link of the paired base copy, without .git.
fc_base_digest() {
  (cd "${FC_BASE_COPY:-/workspace/.fc_base}" && {
    find . -path ./.git -prune -o \( -type f -o -type l \) -print0 | LC_ALL=C sort -z | xargs -0r sha256sum -- 2>/dev/null
    find . -path ./.git -prune -o -type l -printf '%p %l\n' | LC_ALL=C sort
  }) | sha256sum | cut -d' ' -f1
}
