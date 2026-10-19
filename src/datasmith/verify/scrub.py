"""Remove git history after HEAD from a task image, and check an image for history that exposes the fix.

`scrub` writes a new tag `<image>-scrub`; the source image is not changed. It commits only when the container's
changes are under .git dirs and HEAD, tree, `git describe` and status are unchanged. The origin URL stays because
lsv_init.py and the test scripts read it. Nested .git dirs (benchmark repos cloned at their latest commit) are
removed when their files hash the same afterwards.
"""

from __future__ import annotations

import json
import re
import uuid

from datasmith.verify.images import sh

SCRUB = r"""
set -eu
cd "$(git rev-parse --show-toplevel)"
state() { printf '%s %s %s %s' "$(git rev-parse HEAD)" "$(git rev-parse 'HEAD^{tree}')" \
  "$(git describe --tags --always 2>/dev/null || true)" "$(git status --porcelain --untracked-files=no | sha256sum | cut -c1-16)"; }
before=$(state); head=$(git rev-parse HEAD); abbrev=$(git rev-parse --short HEAD | wc -c)
git for-each-ref --format='%(refname) %(objectname)' | while read -r ref obj; do
  c=$(git rev-parse -q --verify "$obj^{commit}" 2>/dev/null) && git merge-base --is-ancestor "$c" "$head" || git update-ref -d "$ref"
done
git stash clear 2>/dev/null || true
rm -rf .git/logs .git/refs/original .git/FETCH_HEAD .git/ORIG_HEAD .git/objects/info/alternates
git config --unset-all gc.pruneExpire 2>/dev/null || true
git config --unset-all gc.worktreePruneExpire 2>/dev/null || true
git config gc.auto 0
git reflog expire --expire=now --expire-unreachable=now --all
git -c gc.pruneExpire=now gc --prune=now --quiet
git config core.abbrev $((abbrev-1))
after=$(state)
left=$(git fsck --unreachable --no-reflogs --no-progress 2>/dev/null | grep -c '^unreachable commit' || true)
treehash() { (cd "$1" && find . -path ./.git -prune -o -type f -print0 | sort -z | xargs -0 -r sha256sum | sha256sum | cut -c1-16); }
nested=""; same=true
for g in $(find /workspace /tmp /opt -maxdepth 4 -name .git -type d -not -path "$(pwd)/.git" 2>/dev/null); do
  d=$(dirname "$g"); h=$(treehash "$d"); rm -rf "$g"; [ "$h" = "$(treehash "$d")" ] || same=false; nested="$nested$g "
done
printf '{"repo":"%s","before":"%s","after":"%s","unreachable_left":%s,"nested_git_removed":"%s","nested_tree_same":%s}\n' \
  "$(pwd)" "$before" "$after" "$left" "${nested% }" "$same"
"""

PROBE = r"""
cd "$(git rev-parse --show-toplevel 2>/dev/null)" || { echo '{"error":"no git repo"}'; exit 0; }
head=$(git rev-parse HEAD); head_ct=$(git log -1 --format=%ct HEAD)
remotes=$(git remote | tr '\n' ' ')
refs=0; ahead=0
for r in $(git for-each-ref --format='%(objectname)'); do
  refs=$((refs+1))
  c=$(git rev-parse -q --verify "$r^{commit}") || continue
  [ "$c" != "$head" ] && git merge-base --is-ancestor "$head" "$c" && ahead=$((ahead+1))
done
unreach=$(git fsck --unreachable --no-reflogs --no-progress 2>/dev/null | awk '/^unreachable commit/{print $3}')
n_unreach=$(printf '%s\n' "$unreach" | grep -c . || true)
n_newer=$(printf '%s\n' "$unreach" | grep . | git log --no-walk --stdin --format=%ct 2>/dev/null | awk -v h="$head_ct" '$1>h' | wc -l)
gt=missing; [ -n "$GT" ] && git cat-file -e "$GT^{commit}" 2>/dev/null && gt=present
pr_commits=0; [ -n "$PR" ] && pr_commits=$({ printf '%s\n' "$unreach"; git rev-list --all --not HEAD; } | grep . | sort -u | git log --no-walk --stdin --format=%s 2>/dev/null | grep -cE "#$PR([^0-9]|$)" || true)
extra=$(find /workspace /tmp /opt -maxdepth 4 -name .git -not -path "$(pwd)/.git" 2>/dev/null | while read -r g; do
  printf '%s@%s;' "$g" "$(git --git-dir="$g" log -1 --format=%cI 2>/dev/null)"; done)
printf '{"head":"%s","head_ct":%s,"remotes":"%s","refs":%s,"refs_ahead":%s,"unreachable_commits":%s,"unreachable_newer":%s,"gt_commit":"%s","pr_commits":%s,"other_git_dirs":"%s"}\n' \
  "$head" "$head_ct" "$remotes" "$refs" "$ahead" "$n_unreach" "$n_newer" "$gt" "$pr_commits" "$extra"
"""


def pr_number(task_id: str, image: str = "") -> str:
    """The PR number from a task id, else from an fc-task or formulacode image name."""
    m = (
        re.search(r"__(\d+)$", task_id)
        or re.search(r"__(\d+):", image)
        or re.search(r"^formulacode/[^:]+:(\d+)$", image)
    )
    return m[1] if m else ""


def probe(task_id: str, image: str, gt_hash: str, timeout: int = 900) -> dict:
    """status `clean` or `leak` (unreachable commits newer than HEAD, refs ahead of HEAD, the fix commit, PR commits)."""
    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "-e",
        f"GT={gt_hash}",
        "-e",
        f"PR={pr_number(task_id)}",
        "--entrypoint",
        "bash",
        image,
        "-c",
        PROBE,
    ]
    try:
        res = json.loads(sh(cmd, timeout=timeout).stdout.strip().splitlines()[-1])
    except Exception as e:
        return {"status": "error", "error": repr(e)[:200]}
    if "error" in res:
        return {"status": "error", **res}
    leaks = [
        k
        for k, bad in [
            ("unreachable_newer", res["unreachable_newer"] > 0),
            ("refs_ahead", res["refs_ahead"] > 0),
            ("gt_commit", res["gt_commit"] == "present"),
            ("pr_commit", res["pr_commits"] > 0),
        ]
        if bad
    ]
    return {"status": "leak" if leaks else "clean", **res, "leaks": " ".join(leaks)}


def scrub(image: str) -> tuple[str | None, dict]:
    """(new tag or None, scrub report)."""
    name = f"fc-scrub-{uuid.uuid4().hex[:10]}"
    cfg = json.loads(sh(["docker", "image", "inspect", image]).stdout)[0]["Config"]
    try:
        p = sh(
            ["docker", "run", "--name", name, "--network", "none", "--entrypoint", "bash", image, "-c", SCRUB],
            timeout=3600,
        )
        if p.returncode:
            return None, {"error": f"scrub failed: {p.stderr[-400:]}"}
        res = json.loads(p.stdout.strip().splitlines()[-1])
        if res["before"] != res["after"] or res["unreachable_left"] or not res["nested_tree_same"]:
            return None, {"error": "state changed or unreachable commits left", **res}
        git_dir = res["repo"].rstrip("/") + "/.git"
        changed = [line.split(" ", 1)[1] for line in sh(["docker", "diff", name]).stdout.splitlines()]
        removed = [git_dir, *res["nested_git_removed"].split()]
        roots = {"/workspace", "/workspace/repo", "/tmp", "/opt", res["repo"]}  # noqa: S108
        parents = roots | {g.rsplit("/", 1)[0] for g in removed}
        stray = [c for c in changed if c not in parents and not any(c == g or c.startswith(g + "/") for g in removed)]
        if stray:
            return None, {"error": f"changes outside .git: {stray[:5]}", **res}
        changes = [
            f"ENTRYPOINT {json.dumps(cfg.get('Entrypoint') or [])}",
            f"CMD {json.dumps(cfg.get('Cmd') or [])}",
            f"LABEL fc.scrubbed_from={image}",
        ]
        new = f"{image}-scrub"
        c = sh(["docker", "commit", *[x for ch in changes for x in ("--change", ch)], name, new])
        if c.returncode:
            return None, {"error": f"commit failed: {c.stderr[-300:]}", **res}
        return new, res
    finally:
        sh(["docker", "rm", "-f", name])
