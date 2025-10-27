"""Robust issue extraction from GitHub PR/issue descriptions."""

from __future__ import annotations

import re
from typing import Any

from datasmith.logging_config import configure_logging
from datasmith.scrape.models import IssueExpanded
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
    cross_ref_timeline = [e for e in timeline if e.get("event") == "cross-referenced"]
    bodies: list[str] = []
    for event in cross_ref_timeline:
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


def _build_issue_payload(
    owner: str,
    repo: str,
    number: str,
    index: int,
    *,
    pr_created_at: str | None = None,
) -> dict[str, Any] | None:
    """Fetch issue metadata and assemble the payload used by the report builder.

    Comments are derived from the issue timeline and optionally filtered to only
    include those created before the given PR's creation time.
    """
    # Local import to avoid circular dependency with report_utils
    from datasmith.scrape.report_utils import issue_timeline, to_datetime

    endpoint = f"/repos/{owner}/{repo}/issues/{number}"
    issue_thread = _get_github_metadata(endpoint)
    if not issue_thread or not isinstance(issue_thread, dict):
        return None

    if "pull_request" in issue_thread:
        if issue_thread.get("merged_at") is not None:
            logger.debug("Skipping merged PR %s/%s#%s", owner, repo, number)
            return None
        logger.debug("Keeping unmerged PR %s/%s#%s for discussion", owner, repo, number)

    timeline = issue_timeline(owner, repo, int(number))
    xref_bodies = _extract_cross_reference_bodies(timeline)

    # Extract comments from timeline and filter out any made after the PR was created
    comments: list[str] = []
    cutoff = to_datetime(pr_created_at) if pr_created_at else None
    for evt in timeline:
        # Only consider actual user comments in the timeline
        if evt.get("event") != "commented":
            continue
        body = (evt.get("body") or "").strip()
        if not body:
            continue
        created_at = evt.get("created_at") or evt.get("updated_at")
        if cutoff and created_at:
            try:
                if to_datetime(created_at) >= cutoff:
                    continue
            except Exception as exc:
                # If parsing fails, include conservatively but record it for diagnostics
                logger.debug("Failed to parse timeline timestamp %s: %s", created_at, exc, exc_info=True)
        comments.append(body)
    formatted_text = _format_issue_stat(index, issue_thread.get("title", ""), comments, xref_bodies)

    return {
        "owner": owner,
        "repo": repo,
        "number": number,
        "title": issue_thread.get("title", ""),
        "body": issue_thread.get("body", ""),
        "created_at": issue_thread.get("created_at"),
        "closed_at": issue_thread.get("closed_at"),
        "comments": comments,
        "cross_references": xref_bodies,
        "is_pr": "pull_request" in issue_thread,
        "is_merged": issue_thread.get("merged_at") is not None if "pull_request" in issue_thread else False,
        "formatted_text": formatted_text,
    }


def extract_issues_from_description(
    description: str | None,
    owner: str,
    repo: str,
    *,
    pr_created_at: str | None = None,
    pr_number_to_ignore: int | None = None,
) -> list[IssueExpanded]:
    """Extract and fetch issue details from a PR/issue description.

    Args:
        description: PR or issue body text (can be None for PRs without bodies)
        owner: Repository owner
        repo: Repository name

    Returns:
        List of IssueExpanded models for referenced issues/PRs.
    """
    if description is None:
        description = ""

    issue_data_list: list[IssueExpanded] = []
    refs = _extract_issue_refs(description, owner, repo)
    # Filter out references to the current PR (self-reference via "#<num>")
    if pr_number_to_ignore is not None:
        refs = [
            (o, r, n) for (o, r, n) in refs if not (o == owner and r == repo and str(n) == str(pr_number_to_ignore))
        ]
    if not refs:
        return issue_data_list
    base = f"https://github.com/{owner}/{repo}"
    for i, (o, r, num) in enumerate(sorted(refs)):
        issue = _build_issue_payload(o, r, num, i, pr_created_at=pr_created_at)
        if not issue:
            continue
        try:
            number_s = str(issue.get("number", "0"))
            number = int(number_s) if number_s.isdigit() else 0
            url = f"{base}/issues/{number}" if number else base
            issue_data_list.append(
                IssueExpanded(
                    number=number,
                    title=issue.get("title", ""),
                    url=url,
                    description=issue.get("body", "") or "",
                    comments=tuple(c for c in issue.get("comments", []) if c),
                    cross_references=tuple(x for x in issue.get("cross_references", []) if x),
                    created_at=issue.get("created_at"),
                    closed_at=issue.get("closed_at"),
                )
            )
        except Exception:
            logger.exception("Failed to build IssueExpanded model")
            continue

    return issue_data_list
