"""Reference extraction and BFS link scraping for GitHub issues/PRs."""

from __future__ import annotations

import re
from collections.abc import Awaitable
from typing import Any, Callable

from datasmith.github.models import IssueExpanded
from datasmith.utils import get_logger

logger = get_logger("github.links")

_HASH_REF = re.compile(r"(?<!\w)#(\d+)")
_CROSS_REF = re.compile(r"(?<!\w)([\w.-]+)/([\w.-]+)#(\d+)")
_URL_REF = re.compile(r"https?://github\.com/([\w.-]+)/([\w.-]+)/(?:issues|pull)/(\d+)")


def extract_references(text: str, owner: str, repo: str) -> list[tuple[str, str, int]]:
    """Extract issue/PR references from text.

    Returns a de-duplicated list of ``(owner, repo, number)`` tuples, preserving
    discovery order.  Handles:
    - Full GitHub URLs (``https://github.com/owner/repo/issues/123``)
    - Cross-repo short refs (``owner/repo#123``)
    - Same-repo hash refs (``#123``)
    """
    refs: list[tuple[str, str, int]] = []
    seen: set[tuple[str, str, int]] = set()

    for m in _URL_REF.finditer(text):
        ref = (m.group(1), m.group(2), int(m.group(3)))
        if ref not in seen:
            refs.append(ref)
            seen.add(ref)

    for m in _CROSS_REF.finditer(text):
        ref = (m.group(1), m.group(2), int(m.group(3)))
        if ref not in seen:
            refs.append(ref)
            seen.add(ref)

    for m in _HASH_REF.finditer(text):
        ref = (owner, repo, int(m.group(1)))
        if ref not in seen:
            refs.append(ref)
            seen.add(ref)

    return refs


async def scrape_links(
    pr: Any,  # PR model with .owner, .repo, .title, .body
    get_issue_fn: Callable[..., Awaitable[IssueExpanded | None]],
    depth: int = 2,
    only_issues: bool = False,
    limit: int = 60,
    before: Any = None,
) -> list[IssueExpanded]:
    """BFS scrape of linked issues/PRs starting from a PR.

    Parameters
    ----------
    pr:
        The seed pull request (must have ``.owner``, ``.repo``, ``.title``, ``.body``).
    get_issue_fn:
        Async callable ``(owner, repo, number) -> IssueExpanded | None``.
    depth:
        Maximum BFS depth.
    only_issues:
        If ``True``, skip results that look like PRs (have ``merged_at``).
    limit:
        Maximum number of results to return.
    before:
        Optional datetime cutoff (unused, reserved for future filtering).
    """
    visited: set[tuple[str, str, int]] = set()
    result: list[IssueExpanded] = []
    queue: list[tuple[str, str, int, int]] = []  # (owner, repo, number, current_depth)

    # Seed from PR body + title
    owner, repo = pr.owner, pr.repo
    seed_text = f"{pr.title}\n{pr.body}"
    for o, r, n in extract_references(seed_text, owner, repo):
        if (o, r, n) not in visited:
            queue.append((o, r, n, 0))
            visited.add((o, r, n))

    while queue and len(result) < limit:
        o, r, n, d = queue.pop(0)

        issue = await get_issue_fn(o, r, n)
        if issue is None:
            continue

        if only_issues and hasattr(issue, "merged_at"):
            continue

        result.append(issue)

        if d < depth:
            ref_text = f"{issue.title}\n{issue.description}\n" + "\n".join(issue.comments)
            for ro, rr, rn in extract_references(ref_text, o, r):
                if (ro, rr, rn) not in visited and len(result) + len(queue) < limit:
                    queue.append((ro, rr, rn, d + 1))
                    visited.add((ro, rr, rn))

    return result[:limit]
