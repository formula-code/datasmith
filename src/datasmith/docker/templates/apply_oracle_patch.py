#!/usr/bin/env python3
"""Filter and apply the oracle patch inside the task container.

Runs INSIDE the task image, from measure.sh.  STDLIB ONLY.

Sections of the diff that touch the ASV benchmark directory or an
``asv.*.json`` config are dropped BEFORE applying, mirroring
run-tests.sh::reset_repo_state, which restores exactly those paths to the
base commit at trial time.  Two reasons:

  1. Fidelity — measuring against PR-modified benchmarks would not match
     what stage 7 measures.
  2. Gate correctness — a benchmark INTRODUCED by the PR has no baseline,
     so "finite timing at both commits" is impossible for it and the fatal
     asv_exec_failed gate would reject a legitimate task.

Filtering the diff text (rather than reverting afterwards) is required:
``git checkout`` cannot remove files the patch CREATED, and ``git clean``
would delete the injected benchmark file that
benchmark_dest_present_post_clean exists to protect.

``applied`` is derived from ``git diff --name-only`` — never from patch(1)'s
exit code, which is 0 in some partial-application cases.

Always exits 0.  Whether the patch applied is a fact for the invariants to
judge, not a reason to abort the measure step.

Precheck fields (same as datasmith.verify.in_image.coverage_check):
by_deps = benchmarks LSV selects through a dependency on a file the patch changes (always_affected ones excluded).
edited_before = tracked files the image already has edited, before the patch.
edited_overlap = those files whose edited lines touch an oracle hunk (its changed or context lines).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess  # noqa: S404
import sys
import tempfile

_HEADER = "diff --git "


def _paths_in_header(line: str) -> list[str]:
    """Extract the a/ and b/ paths from a ``diff --git`` header line."""
    rest = line[len(_HEADER) :].strip()
    out = []
    for tok in rest.split():
        for prefix in ("a/", "b/"):
            if tok.startswith(prefix):
                out.append(tok[len(prefix) :])
                break
        else:
            out.append(tok)
    return out


def _is_excluded(path: str, excluded_prefixes: list[str]) -> bool:
    norm = path.lstrip("./")
    base = os.path.basename(norm)
    if base.startswith("asv.") and base.endswith(".json"):
        return True
    for prefix in excluded_prefixes:
        if not prefix:
            continue
        prefix = prefix.rstrip("/") + "/"
        if norm.startswith(prefix):
            return True
    return False


def filter_patch(text: str, excluded_prefixes: list[str]) -> tuple[str, int]:
    """Drop whole file sections whose path is excluded.

    Returns (filtered_text, number_of_sections_dropped).  Content before the
    first ``diff --git`` header is preserved (commit message preamble).
    """
    lines = text.splitlines(keepends=True)
    kept: list[str] = []
    dropped = 0
    skipping = False
    for line in lines:
        if line.startswith(_HEADER):
            paths = _paths_in_header(line)
            skipping = any(_is_excluded(p, excluded_prefixes) for p in paths)
            if skipping:
                dropped += 1
                continue
        if not skipping:
            kept.append(line)
    return "".join(kept), dropped


def _git(repo: str, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603
        ["git", "-C", repo, *args],  # noqa: S607
        capture_output=True,
        text=True,
        errors="replace",
        env=env,
    )


# unidiff is not available in the image (Python 3.8, no network), so hunk headers are parsed here.
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


def deps_counts(db_path: str, changed: list[str]) -> tuple[int, int]:
    """(by_deps, always_affected) from the deps DB lsv_init wrote; ``changed`` are absolute paths."""
    from asv.contrib.lightspeed.deps_db import LightspeedDB
    from asv.contrib.lightspeed.fingerprint import changed_files_with_fingerprints

    db = LightspeedDB(db_path)
    # shortcut: LightspeedDB has no public query for always_affected, so this reads db._con; upgrade when LSV adds one.
    always = {r[0] for r in db._con.execute("select benchmark_id from benchmark_meta where always_affected = 1")}
    affected = {str(b) for b in db.get_affected_benchmark_ids(changed_files_with_fingerprints(changed, db.get_stored_fshas()))}
    return len(affected - always), len(always)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Filter and apply the oracle patch")
    ap.add_argument("--patch", required=True)
    ap.add_argument("--repo", default="/workspace/repo")
    ap.add_argument("--benchmark-dir", default="")
    ap.add_argument("--base-sha", default="")
    ap.add_argument("--out", default="/opt/formulacode/patch_info.json")
    ap.add_argument("--deps-db", default="")
    args = ap.parse_args(argv)

    info = {"present": False, "applied": False, "files_changed": 0, "paths_excluded": 0}
    info.update(dict.fromkeys(("by_deps", "always_affected", "edited_before", "edited_overlap")))

    try:
        with open(args.patch, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError:
        text = ""

    if text.strip():
        info["present"] = True

        excluded: list[str] = []
        if args.benchmark_dir:
            try:
                rel = os.path.relpath(args.benchmark_dir, args.repo)
                if not rel.startswith(".."):
                    excluded.append(rel)
            except ValueError:
                pass

        filtered, dropped = filter_patch(text, excluded)
        info["paths_excluded"] = dropped

        info["edited_before"] = _git(args.repo, "diff", "--name-only", "HEAD").stdout.split()
        info["edited_overlap"] = overlapping(_git(args.repo, "diff", "-U0", "HEAD").stdout, text)
        # A copy of the starting tree in a separate index, so the files the patch changes exclude the image's own edits.
        start_dir = tempfile.TemporaryDirectory()
        start_env = dict(os.environ, GIT_INDEX_FILE=os.path.join(start_dir.name, "index"))
        _git(args.repo, "read-tree", "HEAD", env=start_env)
        _git(args.repo, "add", "-A", env=start_env)

        if filtered.strip():
            fd, tmp = tempfile.mkstemp(suffix=".patch")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(filtered)
                res = subprocess.run(  # noqa: S603
                    ["patch", "--batch", "--force", "--fuzz=5", "-p1", "-i", tmp],  # noqa: S607
                    cwd=args.repo,
                    capture_output=True,
                    text=True,
                )
                print(f"[apply_oracle_patch] patch rc={res.returncode}")
                if res.stdout:
                    print(res.stdout[-4000:])
                if res.stderr:
                    print(res.stderr[-4000:], file=sys.stderr)
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

        patch_changed = [os.path.join(args.repo, p) for p in _git(args.repo, "diff", "--name-only", env=start_env).stdout.split()]
        start_dir.cleanup()
        if args.deps_db and os.path.exists(args.deps_db):
            try:
                info["by_deps"], info["always_affected"] = deps_counts(args.deps_db, patch_changed)
            except Exception as exc:  # noqa: BLE001 — a missing LSV leaves the fields None
                print(f"[apply_oracle_patch] deps count failed: {exc}", file=sys.stderr)

        if args.base_sha:
            diff = _git(args.repo, "diff", "--name-only", args.base_sha)
            if diff.returncode == 0:
                names = [ln for ln in diff.stdout.splitlines() if ln.strip()]
                info["files_changed"] = len(names)
        info["applied"] = info["files_changed"] > 0

    try:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(info, fh, indent=2, sort_keys=True)
    except OSError as exc:
        print(f"[apply_oracle_patch] failed to write {args.out}: {exc}", file=sys.stderr)

    print(f"[apply_oracle_patch] {json.dumps(info, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
