"""Runs inside a task image (Python 3.8 or newer): apply the oracle patch, run LSV's selection on the image's deps DB, print one JSON line.

by_deps = benchmarks selected through a dependency on a file the patch changes (always_affected ones excluded).
patch_failed = files where `patch` rejected a hunk (the oracle is then not the PR's change).
edited_before = tracked files the image already has edited, before the patch (some base images ship part of the fix).
edited_overlap = those files whose edited lines touch an oracle hunk (its changed or context lines).
"""
import json
import os
import re
import subprocess

DB = "/opt/lsv/cache/results/.lightspeed_deps.db"
REPO = "/workspace/repo"
HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? ")


def old_ranges(diff):
    """{file: [(first, last)]} old-side line ranges of each hunk; a pure insertion covers the line it follows."""
    out, cur = {}, None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            cur = line[6:] if line.startswith("+++ b/") else None
            if cur is not None:
                out.setdefault(cur, [])
            continue
        m = HUNK.match(line)
        if m and cur is not None:
            start, n = int(m.group(1)), int(m.group(2) or 1)
            out[cur].append((start, start + max(n, 1) - 1) if n else (start, start + 1))
    return out


def overlapping(image_diff, patch_text):
    """Files whose image edits touch a hunk of the oracle patch."""
    edits, patch = old_ranges(image_diff), old_ranges(patch_text)
    return sorted(f for f, rs in edits.items() if any(a <= d and c <= b for a, b in rs for c, d in patch.get(f, [])))


def main():
    from asv.contrib.lightspeed.deps_db import LightspeedDB
    from asv.contrib.lightspeed.fingerprint import changed_files_with_fingerprints

    edited_before = subprocess.check_output(["git", "diff", "--name-only", "HEAD"], cwd=REPO, text=True).split()
    image_diff = subprocess.check_output(["git", "diff", "-U0", "HEAD"], cwd=REPO, text=True, errors="replace")
    with open("/tmp/solve.sh", errors="replace") as f:
        edited_overlap = overlapping(image_diff, f.read())
    # Trials diff against a commit of the starting tree (setup.sh), so the image's own edits are not changed files.
    subprocess.run("git add -A && git -c user.name=fc -c user.email=fc@local commit -q --no-verify --allow-empty -m fc-baseline",
                   shell=True, cwd=REPO, check=True, capture_output=True)
    subprocess.run(["bash", "/tmp/solve.sh"], cwd=REPO, capture_output=True)
    rejected = [p[:-4] for p in subprocess.check_output(["git", "ls-files", "--others", "--", "*.rej"], cwd=REPO, text=True).split()]
    changed = [os.path.join(REPO, p) for p in subprocess.check_output(["git", "diff", "--name-only"], cwd=REPO, text=True).split()]
    db = LightspeedDB(DB)
    always = {r[0] for r in db._con.execute("select benchmark_id from benchmark_meta where always_affected = 1")}
    total = db._con.execute("select count(*) from (select benchmark_id from benchmark_dep union select benchmark_id from benchmark_meta)").fetchone()[0]
    affected = {str(b) for b in db.get_affected_benchmark_ids(changed_files_with_fingerprints(changed, db.get_stored_fshas()))}
    print(json.dumps({"changed_files": len(changed), "benchmarks": total, "always_affected": len(always),
                      "selected": len(affected), "by_deps": len(affected - always), "patch_failed": rejected,
                      "edited_before": edited_before, "edited_overlap": edited_overlap}))


if __name__ == "__main__":
    main()
