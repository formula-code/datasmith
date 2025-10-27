from datasmith.execution.collect_commits import search_commits
from datasmith.execution.collect_commits_offline import find_perf_commits


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


def test_find_perf_commits() -> list[str]:
    perf_commits = find_perf_commits(
        repo_name="pandas-dev/pandas",
        n_workers=4,
    )
    assert isinstance(perf_commits, list)
    assert all(isinstance(sha, str) and len(sha) == 40 for sha in perf_commits)
    assert len(perf_commits) > 0
    print(f"Found {len(perf_commits)} performance-related commits.")
    return perf_commits
