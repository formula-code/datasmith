"""Robust issue extraction from GitHub PR/issue descriptions."""

from __future__ import annotations

import re
from typing import Any

from datasmith.logging_config import configure_logging
from datasmith.scrape.build_pr_report import issue_comments, issue_timeline
from datasmith.utils import _get_github_metadata

logger = configure_logging()


def _extract_issue_refs(text: str, default_owner: str, default_repo: str) -> list[tuple[str, str, str]]:
    """Return a de-duplicated list of (owner, repo, number) tuples referenced in `text`.

    Supports:
      - Same-repo shorthand:         #123
      - Cross-repo shorthand:        owner/repo#123
      - Full URLs (issues only):     https://github.com/owner/repo/issues/123

    Notes:
      - PR URLs (/pull/123) are included here; we'll verify via API and
        drop merged PRs but keep abandoned/closed PRs for their discussion value.

    Args:
        text: Text to extract issue references from
        default_owner: Default repository owner for same-repo references
        default_repo: Default repository name for same-repo references

    Returns:
        List of (owner, repo, number) tuples
    """
    refs: set[tuple[str, str, str]] = set()

    # 1) Full URLs -> issues and pulls
    url_pat = re.compile(
        r"https?://github\.com/(?P<owner>[\w\.-]+)/(?P<repo>[\w\.-]+)"
        r"/(?P<kind>issues|pull)/(?P<number>\d+)\b",
        re.IGNORECASE,
    )
    for m in url_pat.finditer(text):
        refs.add((m.group("owner"), m.group("repo"), m.group("number")))

    # 2) Cross-repo shorthand: owner/repo#123
    xrepo_pat = re.compile(r"(?P<owner>[\w\.-]+)/(?P<repo>[\w\.-]+)#(?P<number>\d+)\b")
    for m in xrepo_pat.finditer(text):
        refs.add((m.group("owner"), m.group("repo"), m.group("number")))

    # 3) Same-repo shorthand: #123
    #    - Avoid matching markdown headings (##123) or words like C#.
    same_repo_pat = re.compile(r"(?<![#\w])#(?P<number>\d+)\b")
    for m in same_repo_pat.finditer(text):
        refs.add((default_owner, default_repo, m.group("number")))

    # Optional: prioritize refs immediately following closing keywords
    # (We still include non-keyword refs; this just ensures those are not missed.)
    # Example matches: "resolves #10, fixes org/repo#100"
    kw_block_pat = re.compile(
        r"(?i)\b(close|closes|closed|fix|fixes|fixed|resolve|resolves|resolved)\b" r"(?P<tail>[^.\n\r]{0,200})"
    )
    for m in kw_block_pat.finditer(text):
        tail = m.group("tail") or ""
        for m2 in url_pat.finditer(tail):
            refs.add((m2.group("owner"), m2.group("repo"), m2.group("number")))
        for m2 in xrepo_pat.finditer(tail):
            refs.add((m2.group("owner"), m2.group("repo"), m2.group("number")))
        for m2 in same_repo_pat.finditer(tail):
            refs.add((default_owner, default_repo, m2.group("number")))

    return list(refs)


def _extract_cross_reference_bodies(timeline: list[dict[str, Any]]) -> list[str]:
    """Return a list of cross-reference bodies from the GitHub issue timeline."""
    bodies: list[str] = []
    for event in timeline:
        try:
            src_issue = event.get("source", {}).get("issue")
            if not src_issue:
                continue
            body = src_issue.get("body")
            if body:
                bodies.append(body)
        except Exception as exc:  # pragma: no cover - defensive logging path
            logger.debug("Failed to extract cross-reference body: %s", exc, exc_info=True)
    return bodies


def _format_issue_stat(index: int, title: str, comments: list[str], cross_references: list[str]) -> str:
    """Create a backward-compatible formatted issue summary."""
    stat = f"Issue {index}:{title}\n"
    stat += f"Issue {index} comments:" + "\n".join(comments) + "\n"
    stat += f"Issue {index} cross-referenced comments:" + "\n".join(cross_references) + "\n"
    return stat


def _build_issue_payload(owner: str, repo: str, number: str, index: int) -> dict[str, Any] | None:
    """Fetch issue metadata and assemble the payload used by the report builder."""
    endpoint = f"/repos/{owner}/{repo}/issues/{number}"
    issue_thread = _get_github_metadata(endpoint)
    if not issue_thread or not isinstance(issue_thread, dict):
        return None

    if "pull_request" in issue_thread:
        if issue_thread.get("merged_at") is not None:
            logger.debug("Skipping merged PR %s/%s#%s", owner, repo, number)
            return None
        logger.debug("Keeping unmerged PR %s/%s#%s for discussion", owner, repo, number)

    issue_comments_list = issue_comments(owner, repo, int(number))
    timeline = issue_timeline(owner, repo, int(number))
    xref_bodies = _extract_cross_reference_bodies(timeline)

    # Normalise comment content
    comments = [comment.get("body") or "" for comment in issue_comments_list]
    formatted_text = _format_issue_stat(index, issue_thread.get("title", ""), comments, xref_bodies)

    return {
        "owner": owner,
        "repo": repo,
        "number": number,
        "title": issue_thread.get("title", ""),
        "body": issue_thread.get("body", ""),
        "comments": comments,
        "cross_references": xref_bodies,
        "is_pr": "pull_request" in issue_thread,
        "is_merged": issue_thread.get("merged_at") is not None if "pull_request" in issue_thread else False,
        "formatted_text": formatted_text,
    }


def extract_issues_from_description(
    description: str | None, owner: str, repo: str
) -> tuple[list[str], list[dict[str, Any]]]:
    """Extract and fetch issue details from a PR/issue description.

    Args:
        description: PR or issue body text (can be None for PRs without bodies)
        owner: Repository owner
        repo: Repository name

    Returns:
        Tuple of (problem_statements, issue_data) where:
        - problem_statements: List of formatted issue summaries
        - issue_data: List of dicts containing issue metadata
    """
    # Handle None descriptions (some PRs have no body)
    if description is None:
        description = ""

    prob_stat = [description + "\n"] if description else [""]
    issue_data_list: list[dict[str, Any]] = []

    # Extract all referenced issues
    refs = _extract_issue_refs(description, owner, repo)

    logger.debug("Extracted refs: %s", refs)

    if not refs:
        return prob_stat, issue_data_list

    # Iterate unique (owner, repo, number) refs
    for i, (o, r, num) in enumerate(sorted(refs)):
        payload = _build_issue_payload(o, r, num, i)
        if not payload:
            continue
        issue_data_list.append(payload)

    return prob_stat, issue_data_list
