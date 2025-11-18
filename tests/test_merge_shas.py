from datasmith.execution.collect_commits import search_commits


def test_search_commits() -> list[str]:
    merged_sha_commits = search_commits(
        repo_name="pandas-dev/pandas",
        query="state=closed",
        max_pages=4,
        per_page=100,
    )
    assert isinstance(merged_sha_commits, list)
    assert all(isinstance(sha, str) and len(sha) == 40 for sha in merged_sha_commits)
    assert len(merged_sha_commits) > 0
    print(f"Found {len(merged_sha_commits)} merged SHA commits.")
    return merged_sha_commits
