"""GitHub Code Search helpers for repo discovery."""

from __future__ import annotations

from datasmith.utils import get_logger

logger = get_logger("github.search")


async def search_repos_by_file(
    gh: object,
    filename: str = "asv.conf.json",
    min_stars: int = 100,
) -> list[tuple[str, str]]:
    """Discover repos containing *filename* via the GitHub Code Search API.

    Returns a deduplicated list of ``(owner, repo)`` tuples for repos that are
    not forks, not archived, and have at least *min_stars* stars.
    """
    from datasmith.github.client import GitHubClient

    if not isinstance(gh, GitHubClient):
        raise TypeError(f"Expected GitHubClient, got {type(gh).__name__}")

    seen: set[str] = set()
    results: list[tuple[str, str]] = []

    async for item in gh.search_code(f"filename:{filename}"):
        repo_data = item.get("repository", {})
        full_name: str = repo_data.get("full_name", "")
        if not full_name or full_name in seen:
            continue
        seen.add(full_name)

        # Skip forks (available in search response)
        if repo_data.get("fork", False):
            continue

        # Skip the ASV tool itself
        if full_name == "airspeed-velocity/asv":
            continue

        owner, repo = full_name.split("/", 1)
        results.append((owner, repo))

    logger.info("Code search found %d candidate repos for filename:%s", len(results), filename)

    # Fetch full metadata to filter by archived/disabled/stars
    filtered: list[tuple[str, str]] = []
    for owner, repo in results:
        resp = await gh._request("GET", f"/repos/{owner}/{repo}")
        if resp is None:
            continue
        data = resp.json()
        if data.get("archived", False) or data.get("disabled", False):
            logger.debug("%s/%s: archived/disabled — skipped", owner, repo)
            continue
        stars = data.get("stargazers_count", 0)
        if stars < min_stars:
            logger.debug("%s/%s: %d stars — below threshold", owner, repo, stars)
            continue
        filtered.append((owner, repo))
        logger.debug("%s/%s: %d stars — included", owner, repo, stars)

    logger.info("Filtered to %d repos with >= %d stars", len(filtered), min_stars)
    return filtered
