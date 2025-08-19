import re
from typing import Any

from git import BadName, GitCommandError, Repo
from requests.exceptions import HTTPError

from datasmith.logging_config import get_logger
from datasmith.utils import CACHE_LOCATION, _get_github_metadata, cache_completion

logger = get_logger("execution.utils")


_FLAG_MAP = {
    "i": re.IGNORECASE,
    "m": re.MULTILINE,
    "s": re.DOTALL,
    "x": re.VERBOSE,
}


def _parse_flag_string(flag_str: str) -> int:
    flags = 0
    for ch in flag_str:
        flags |= _FLAG_MAP.get(ch, 0)
    return flags


def _compile_patterns(raws: list[str], base_flags: int) -> list[re.Pattern[str]]:
    """
    Accepts:
      - raw regex strings (inline flags like (?i) allowed)
      - or delimited tokens `/pattern/imsx` (flags optional)
    Compiles with base_flags OR any trailing delimited flags.
    Invalid patterns are skipped with a warning.
    """
    compiled: list[re.Pattern[str]] = []
    for raw in raws:
        pat = raw
        flags = base_flags

        # /pattern/flags style (e.g., /\bperf\b/i)
        m = re.fullmatch(r"/(.*?)/([imsx]*)", raw)
        if m:
            pat = m.group(1)
            flags |= _parse_flag_string(m.group(2))

        try:
            compiled.append(re.compile(pat, flags))
        except re.error as e:
            logger.warning("Ignoring invalid regex %r: %s", raw, e)
    return compiled


def _any_match(patterns: list[re.Pattern[str]] | str, text: str) -> bool:
    if isinstance(patterns, str):
        return False  # should not happen, but just in case
    return any(p.search(text) for p in patterns)


def _get_grep_params(qs: dict[str, list[str]]) -> dict[str, list[re.Pattern[str]] | str]:
    base_flags = _parse_flag_string(qs.get("grep_flags", [""])[0])
    pos_any = _compile_patterns(qs.get("grep", []), base_flags)
    pos_title = _compile_patterns(qs.get("grep_title", []), base_flags)
    pos_msg = _compile_patterns(qs.get("grep_msg", []), base_flags)

    neg_any = _compile_patterns(qs.get("grep_not", []), base_flags)
    neg_title = _compile_patterns(qs.get("grep_title_not", []), base_flags)
    neg_msg = _compile_patterns(qs.get("grep_msg_not", []), base_flags)

    grep_mode = (qs.get("grep_mode", ["any"])[0] or "any").lower()
    if grep_mode not in {"any", "all"}:
        grep_mode = "any"

    return {
        "pos_any": pos_any,
        "pos_title": pos_title,
        "pos_msg": pos_msg,
        "neg_any": neg_any,
        "neg_title": neg_title,
        "neg_msg": neg_msg,
        "grep_mode": grep_mode,
    }


def _neg_matches(grep_params: dict[str, list[re.Pattern[str]] | str], title: str, message: str) -> bool:
    return (
        _any_match(grep_params["neg_any"], title)
        or _any_match(grep_params["neg_any"], message)
        or _any_match(grep_params["neg_title"], title)
        or _any_match(grep_params["neg_msg"], message)
    )


def _pos_matches(grep_params: dict[str, list[re.Pattern[str]] | str], title: str, message: str) -> bool:
    if grep_params["pos_any"] or grep_params["pos_title"] or grep_params["pos_msg"]:
        checks = []
        if grep_params["pos_any"]:
            checks.append(_any_match(grep_params["pos_any"], title) or _any_match(grep_params["pos_any"], message))
        if grep_params["pos_title"]:
            checks.append(_any_match(grep_params["pos_title"], title))
        if grep_params["pos_msg"]:
            checks.append(_any_match(grep_params["pos_msg"], message))

        ok = any(checks) if grep_params["grep_mode"] == "any" else all(checks)
        if not ok:
            return True
    return True


def _get_commit_info(repo_name: str, commit_sha: str) -> dict:
    try:
        commit_info = _get_github_metadata(endpoint=f"/repos/{repo_name}/commits/{commit_sha}")
        if commit_info is None:
            # Try to bypass cache if the commit info is not found
            commit_info = _get_github_metadata(endpoint=f"/repos/{repo_name}/commits/{commit_sha}")
    except HTTPError:
        logger.exception("Error fetching commit info: %s")
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
        }
    if not commit_info:
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
        }

    if commit_sha != commit_info["sha"]:
        raise ValueError("Commit SHA mismatch")  # noqa: TRY003
    return {
        "sha": commit_info["sha"],
        "date": commit_info["commit"]["committer"]["date"],
        "message": commit_info["commit"]["message"],
        "total_additions": commit_info["stats"]["additions"],
        "total_deletions": commit_info["stats"]["deletions"],
        "total_files_changed": commit_info["stats"]["total"],
        "files_changed": "\n".join([d["filename"] for d in commit_info["files"]]),
    }


@cache_completion(CACHE_LOCATION, "get_commit_info_offline")
def _get_commit_info_offline(repo: Repo, commit_sha: str, bypass_cache=True) -> dict[str, Any]:
    """
    Return commit metadata and diff stats *without* the GitHub REST API.

    The function creates a temporary **treeless** clone
    (`git clone --filter=tree:0 …`) so it transfers only commit objects.
    When we later call `commit.stats`, Git will lazily grab just the blobs
    needed dto compute line-level stats - still far cheaper than an API call.
    """
    try:
        commit = repo.commit(commit_sha)
    except (BadName, ValueError):
        logger.exception("Maybe commit not found: %s", commit_sha)
        repo.git.fetch("--no-filter", "--quiet", "origin", commit_sha)
        commit = repo.commit(commit_sha)  # retry after fetching
    except GitCommandError:
        logger.exception("Error fetching commit info: %s", commit_sha)
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
            "patch": "",
        }

    stats = commit.stats

    # get text based patch patch
    patch = (
        repo.git.format_patch("--stdout", "-1", commit.hexsha)
        .encode("utf-8", "surrogateescape")
        .decode("utf-8", "backslashreplace")
    )

    return {
        "sha": commit.hexsha,
        "date": commit.committed_datetime.isoformat(),
        "message": commit.message,
        "total_additions": stats.total["insertions"],
        "total_deletions": stats.total["deletions"],
        "total_files_changed": stats.total["files"],
        "files_changed": "\n".join(str(k) for k in stats.files),
        "patch": patch,
    }


def find_file_in_tree(repo: str, filename: str, branch: str | None = None) -> list[str] | None:
    if branch is None:
        repo_info = _get_github_metadata(endpoint=f"/repos/{repo}")
        # sometimes the API returns a single-element list
        if isinstance(repo_info, list):
            if len(repo_info) == 1:
                repo_info = repo_info[0]  # pyright: ignore[reportArgumentType]
            else:
                raise ValueError(f"Expected one repo info object, got {len(repo_info)}")  # noqa: TRY003
        branch = repo_info.get("default_branch")  # pyright: ignore[reportOptionalMemberAccess]
        if not branch:
            raise ValueError("Could not determine the default branch for this repository")  # noqa: TRY003

    r = _get_github_metadata(endpoint=f"/repos/{repo}/git/refs/heads/{branch}")
    if isinstance(r, list):
        if len(r) == 1:
            r = r[0]  # pyright: ignore[reportArgumentType]
        else:
            raise ValueError()
    sha = r["object"]["sha"]  # pyright: ignore[reportOptionalSubscript]

    r = _get_github_metadata(endpoint=f"/repos/{repo}/git/trees/{sha}?recursive=1")
    tree = r["tree"]  # pyright: ignore[reportOptionalSubscript]

    # 4) Return any blobs whose path ends with the filename
    matches = [entry["path"] for entry in tree if entry["type"] == "blob" and entry["path"].endswith(filename)]
    # remove matches that are more than two levels deep
    matches = [match for match in matches if match.count("/") <= 2]
    if len(matches) == 0:
        return None
    return matches
