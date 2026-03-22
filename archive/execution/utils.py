import re
from pathlib import Path
from typing import Any

from git import Commit, Repo

from datasmith.core.git import (
    clone_repo as core_clone_repo,
)
from datasmith.core.git import (
    find_file_in_tree as core_find_file_in_tree,
)
from datasmith.core.git import (
    get_change_summary as core_get_change_summary,
)
from datasmith.core.git import (
    get_commit_info as core_get_commit_info,
)
from datasmith.core.git import (
    get_commit_info_offline as core_get_commit_info_offline,
)
from datasmith.core.text_utils import (
    any_match as _any_match,
)
from datasmith.core.text_utils import (
    compile_patterns as _compile_patterns,
)
from datasmith.core.text_utils import (
    get_grep_params as _get_grep_params,
)
from datasmith.core.text_utils import (
    neg_matches as _neg_matches,
)
from datasmith.core.text_utils import (
    parse_flag_string as _parse_flag_string,
)
from datasmith.core.text_utils import (
    pos_matches as _pos_matches,
)
from datasmith.logging_config import get_logger

logger = get_logger("execution.utils")

NON_CORE_PATTERNS = re.compile(
    r"""(
           (^|/)tests?(/|$)        |   # any tests/ directory
           (^|/)doc[s]?(/|$)       |   # docs/, doc/, documentation/
           (^|/)examples?(/|$)     |   # examples/
           (^|/)\.github(/|$)      |   # GitHub meta files
           (^|/)benchmarks?(/|$)   |   # benchmarks/
           (^|/)dist-info(/|$)     |   # wheel metadata
           (^|/)build(/|$)         |   # build artifacts
           (^|/)site-packages(/|$) |   # vendored wheels
           (^|/)__(init|pycache)__ |   # __init__.py, __pycache__
           (^|/)requirements-docs\.txt$|
           (^|/)pyproject\.toml$|
           (^|/)README\.md$        |
           \.rst$                  |   # reStructuredText docs
           \.md$                       # markdown docs
       )""",
    re.VERBOSE,
)


def has_core_file(files_changed: str) -> bool:
    """Return True if any path in ``files_changed`` looks like core code."""
    for path in files_changed.split("\n"):
        path = path.strip()
        if not path:
            continue
        if not NON_CORE_PATTERNS.search(path):
            return True
    return False


def clone_repo(root_path: str | Path, repo_name: str) -> tuple[str, Repo]:
    """Wrapper adding logging around :func:`datasmith.core.git.clone_repo`."""
    repo_name, repo = core_clone_repo(root_path=root_path, repo_name=repo_name)
    logger.debug("Cloned repo %s to %s", repo_name, Path(repo.git_dir))
    return repo_name, repo


def _get_commit_info(repo_name: str, commit_sha: str) -> dict[str, Any]:
    return core_get_commit_info(repo_name, commit_sha)


def get_change_summary(commit: Commit) -> str:
    return core_get_change_summary(commit)


def _get_commit_info_offline(repo: Repo, commit_sha: str, bypass_cache: bool = False) -> dict[str, Any]:
    return core_get_commit_info_offline(repo, commit_sha, bypass_cache=bypass_cache)


def find_file_in_tree(repo: str, filename: str, branch: str | None = None) -> list[str] | None:
    return core_find_file_in_tree(repo, filename, branch)


__all__ = [
    "NON_CORE_PATTERNS",
    "_any_match",
    "_compile_patterns",
    "_get_commit_info",
    "_get_commit_info_offline",
    "_get_grep_params",
    "_neg_matches",
    "_parse_flag_string",
    "_pos_matches",
    "clone_repo",
    "find_file_in_tree",
    "get_change_summary",
    "has_core_file",
]
