from requests.exceptions import HTTPError

from datasmith.utils import retrieve_commit_info

COMMIT_URL = "https://api.github.com/repos/{repository}/commits/{commit_sha}"
REPO_URL = "https://api.github.com/repos/{repository}"


def _get_commit_info(repo_name: str, commit_sha: str) -> dict:
    url = COMMIT_URL.format(repository=repo_name, commit_sha=commit_sha)
    try:
        commit_info = retrieve_commit_info(url)
    except HTTPError as e:
        print(f"Error fetching commit info: {e}")
        return {
            "sha": commit_sha,
            "date": None,
            "message": None,
            "total_additions": 0,
            "total_deletions": 0,
            "total_files_changed": 0,
            "files_changed": "",
        }

    return {
        "sha": commit_info["sha"],
        "date": commit_info["commit"]["committer"]["date"],
        "message": commit_info["commit"]["message"],
        "total_additions": commit_info["stats"]["additions"],
        "total_deletions": commit_info["stats"]["deletions"],
        "total_files_changed": commit_info["stats"]["total"],
        "files_changed": "\n".join([d["filename"] for d in commit_info["files"]]),
    }


def find_file_in_tree(repo: str, filename: str, branch: str | None = None) -> list[str] | None:
    repo_api = REPO_URL.format(repository=repo)

    # 1) If no branch was given, look up the repos default_branch
    if branch is None:
        repo_info = retrieve_commit_info(repo_api)
        # sometimes the API returns a single-element list
        if isinstance(repo_info, list):
            if len(repo_info) == 1:
                repo_info = repo_info[0]
            else:
                raise ValueError(f"Expected one repo info object, got {len(repo_info)}")  # noqa: TRY003
        branch = repo_info.get("default_branch")
        if not branch:
            raise ValueError("Could not determine the default branch for this repository")  # noqa: TRY003

    # 2) Resolve that branch name to a commit SHA
    ref_url = f"{repo_api}/git/refs/heads/{branch}"
    r = retrieve_commit_info(ref_url)
    if isinstance(r, list):
        if len(r) == 1:
            r = r[0]
        else:
            raise ValueError()
    sha = r["object"]["sha"]

    # 3) Fetch the recursive tree for that SHA
    tree_url = f"{repo_api}/git/trees/{sha}?recursive=1"
    r = retrieve_commit_info(tree_url)
    tree = r["tree"]

    # 4) Return any blobs whose path ends with the filename
    matches = [entry["path"] for entry in tree if entry["type"] == "blob" and entry["path"].endswith(filename)]
    # remove matches that are more than two levels deep
    matches = [match for match in matches if match.count("/") <= 2]
    if len(matches) == 0:
        return None
    return matches
