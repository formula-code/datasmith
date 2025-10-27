from typing import Any

from datasmith.logging_config import configure_logging
from datasmith.scrape.report_builder import ReportBuilder
from datasmith.scrape.utils import _parse_pr_url
from datasmith.utils import _get_github_metadata

logger = configure_logging(level=10)


def fetch_pr(owner: str, repo: str, num: int) -> dict[str, Any]:
    """Fetch PR metadata from GitHub API."""
    pr = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
    if not isinstance(pr, dict):
        raise TypeError("PR metadata should be a dict")
    return pr


def build_pr_dict(owner: str, repo: str, num: int) -> dict[str, Any]:
    """Build a PR dictionary suitable for ReportBuilder."""
    pr = fetch_pr(owner, repo, num)
    return {"pr_" + k: v for k, v in pr.items()}


def main(argv):
    pr_url = argv[1]
    print(f"Processing pull request: {pr_url}")
    owner, repo, num_s = _parse_pr_url(pr_url)
    pr_dict = build_pr_dict(owner, repo, num_s)
    rb = ReportBuilder(
        enable_llm_backends=True,
        summarize_llm=True,
        anonymize_output=False,
        add_classification=True,
        filter_performance_only=True,
    )
    result = rb.build(pr_dict)
    print("-" * 80)
    print(result.final_md)
    print("-" * 80)
    print()


if __name__ == "__main__":
    import sys

    main(sys.argv)
