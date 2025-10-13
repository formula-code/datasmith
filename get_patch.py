import tempfile
import os
from pathlib import Path

# Requires GitPython: pip install GitPython
import git

REPO_URL = "https://github.com/astropy/astropy.git"
COMMIT = "74ff5d39fd3b42e50ba3ec6ae155a719b5c0b4d0"
OUTPUT_PATH = "/testbed/solution_patch.diff"  # change if you want

heredoc_tag = "__SOLUTION__"

with tempfile.TemporaryDirectory() as tmpdir:
    # Clone
    repo = git.Repo.clone_from(REPO_URL, tmpdir)

    # Resolve commit and its parent
    commit = repo.commit(COMMIT)
    if not commit.parents:
        raise RuntimeError("Selected commit has no parents (root commit).")
    parent_sha = commit.parents[0].hexsha

    # Generate unified diff exactly like `git diff <parent> <commit>`
    # This produces headers like: "diff --git a/... b/..."
    diff_text = repo.git.diff(parent_sha, COMMIT)

    # Build the here-doc script
    script = (
        f"cat > {OUTPUT_PATH} << '{heredoc_tag}'\n"
        f"{diff_text}\n"
        f"{heredoc_tag}\n"
    )

    # Print to stdout so you can copy-paste or redirect to a .sh file
    print(script)
