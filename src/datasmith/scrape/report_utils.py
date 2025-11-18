from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from datasmith.logging_config import configure_logging
from datasmith.utils import _get_github_metadata

logger = configure_logging()

# configure_agent_backends()
# configure_agent_backends(PORTKEY_MODEL_NAME="@togetherai/meta-llama/Llama-3.3-70B-Instruct-Turbo")


MAX_LINKS_TO_FOLLOW = 60  # safety cap for level-2 traversal
# ISSUE_STRUCTURER = LLMStructurer()
# COMMENT_SUMMARIZER = LLMCommentSummarizer()
# CLASSIFY_JUDGE = ClassifyJudge()

# Dataframe with git commits to generate reports for
# Known bot usernames to filter out from comments
BOT_USERNAMES = {
    "coveralls",
    "codecov",
    "github-actions",
    "dependabot",
    "dependabot[bot]",
    "renovate",
    "renovate[bot]",
    "netlify",
    "vercel",
    "circleci",
    "travis-ci",
    "appveyor",
    "pre-commit-ci",
    "pre-commit-ci[bot]",
    "sonarcloud",
    "codefactor-io",
    "imgbot",
    "imgbot[bot]",
    "github-advanced-security",
    "gitguardian",
    "snyk-bot",
    "pull",
    "pull[bot]",
    "allcontributors",
    "allcontributors[bot]",
    # GitHub / CI / merge helpers
    "github-actions[bot]",  # official Actions actor
    "k8s-ci-robot",  # Kubernetes Prow bot
    "docker-library-bot",
    "bors[bot]",
    "Mergifyio",
    "kodiakhq[bot]",
    # Releases & automation
    "semantic-release-bot",
    "release-drafter",
    # CLA / governance
    "cla-assistant",
    "cla-bot",
    "google-cla",
    # Translation / localization
    "weblate",
    "crowdin-bot",
    # Dependency / security updaters
    "greenkeeperio-bot",
    "pyup-bot",
    "fossabot",
    "npm-cli-bot",
    # Code quality / legacy services
    "lgtm-com[bot]",
    "codeclimate",
    # Misc automations
    "stale[bot]",
    "autofix-ci",
}


def to_datetime(ts: str) -> datetime:
    return datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)


def iso(ts: str) -> str:
    dt = to_datetime(ts)
    return dt.strftime("%H:%M %d/%m/%Y")


def extract_links(text: str) -> list[str]:
    return re.findall(r"https?://[^\s)<>\]]+", text or "")


# def fetch_commit(owner: str, repo: str, sha: str) -> dict:
#     # endpoint = f"/repos/{owner}/{repo}/commits/{sha}"
#     # logger.debug(endpoint)
#     commit_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{sha}")
#     if commit_metadata and isinstance(commit_metadata, dict):
#         return {
#             "sha": sha,
#             "date_iso": commit_metadata["commit"]["author"]["date"],
#             "message": commit_metadata["commit"]["message"],
#         }
#     return {}


# def prs_for_commit(owner: str, repo: str, sha: str) -> list[dict[str, Any]]:
#     # pulls_metadata: Sequence[dict] = _get_github_metadata(
#     #     endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100"
#     # )
#     pulls_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100")
#     if pulls_metadata and isinstance(pulls_metadata, list):
#         return pulls_metadata
#     return []
#     # url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/pulls"
#     # return gh_get(url, params={"per_page": 100})


# def pr_meta(owner: str, repo: str, num: int) -> dict[str, Any]:
#     pr_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
#     if pr_metadata and isinstance(pr_metadata, dict):
#         return pr_metadata
#     return {}


# UNUSED: kept for reference only
# def _is_bot_comment(comment: dict) -> bool:
#     """Check if a comment is from a bot.
#
#     Args:
#         comment: Comment dict with user information
#
#     Returns:
#         True if comment is from a known bot
#     """
#     if not comment.get("user"):
#         return False
#     username = comment["user"].get("login", "").lower()
#     return "[bot]" in username or username in BOT_USERNAMES or username in {u.lower() for u in BOT_USERNAMES}


def issue_timeline(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    """
    Retrieve the full GitHub issue timeline (all events, including cross-references)
    with pagination support.
    """
    all_events: list[dict[str, Any]] = []
    endpoint = f"/repos/{owner}/{repo}/issues/{num}/timeline"
    timeline_page = _get_github_metadata(endpoint=endpoint)

    if timeline_page and isinstance(timeline_page, list):
        all_events.extend(timeline_page)
    else:
        return []
    return all_events
    # cross_refs = [e for e in all_events if e.get("event") == "cross-referenced"]
    # return cross_refs

    # UNUSED: kept for reference only
    # def issue_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    #     all_comments: list[dict[str, Any]] = []
    #     page = 1
    #
    #     while True:
    #         endpoint = f"/repos/{owner}/{repo}/issues/{num}/comments?per_page=100&page={page}"
    #         issue_metadata = _get_github_metadata(endpoint=endpoint)
    #
    #         if issue_metadata and isinstance(issue_metadata, list):
    #             all_comments.extend(issue_metadata)
    #         else:
    #             break
    #
    #         if len(issue_metadata) < 100:
    #             break
    #         page += 1
    #
    #     if all_comments:
    #         return all_comments
    #     return []

    # UNUSED: kept for reference only
    # def review_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    #     review_comments_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/comments?per_page=100")
    #     if review_comments_metadata and isinstance(review_comments_metadata, list):
    #         return review_comments_metadata
    #     return []

    # UNUSED: kept for reference only
    # def reviews(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    #     reviews_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/reviews?per_page=100")
    #     if reviews_metadata and isinstance(reviews_metadata, list):
    #         return reviews_metadata
    #     return []

    # UNUSED: kept for reference only
    # def classify_gh_link(u: str) -> tuple[str, ...] | None:
    #     """
    #     Return ('type', owner, repo, id)  where type ∈ {'pr', 'issue', 'commit'}
    #     or None if not recognised as such.
    #     """
    #     p = urlparse(u)
    #     if "github.com" not in p.netloc:
    #         return None
    #     parts = p.path.strip("/").split("/")
    #     if len(parts) >= 4 and parts[3] == "pull" and parts[4].isdigit():
    #         return ("pr", parts[1], parts[2], parts[4])
    #     if len(parts) >= 4 and parts[3] == "issues" and parts[4].isdigit():
    #         return ("issue", parts[1], parts[2], parts[4])
    #     if len(parts) >= 4 and parts[3] == "commit":
    #         return ("commit", parts[1], parts[2], parts[4])
    #     return None

    # UNUSED: kept for reference only
    # def summarize_gh_resource(res: tuple[str, ...]) -> str:
    #     typ, owner, repo, ident = res
    #     base = f"https://github.com/{owner}/{repo}"
    #     try:
    #         if (
    #             typ == "pr"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             return f"* PR #{ident}: {j['title']}  \n  <{base}/pull/{ident}>"
    #         if (
    #             typ == "issue"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/issues/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             return f"* Issue #{ident}: {j['title']}  \n  <{base}/issues/{ident}>"
    #         if (
    #             typ == "commit"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             first_line = j["commit"]["message"].splitlines()[0]
    #             return f"* Commit {ident[:7]}: {first_line}  \n  <{base}/commit/{ident}>"
    #     except (KeyError, ValueError, TypeError):
    #         return ""
    #     return ""

    # UNUSED: kept for reference only
    # def summarize_gh_resource_model(res: tuple[str, ...]) -> LinkSummary | None:
    #     """Return a structured `LinkSummary` for a GitHub resource.
    #
    #     Does the same API lookups as `summarize_gh_resource` but returns
    #     a typed object suitable for templating.
    #     """
    #     typ, owner, repo, ident = res
    #     base = f"https://github.com/{owner}/{repo}"
    #     try:
    #         if (
    #             typ == "pr"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             return LinkSummary(
    #                 typ="pr",
    #                 owner=owner,
    #                 repo=repo,
    #                 ident=ident,
    #                 url=f"{base}/pull/{ident}",
    #                 title=j.get("title"),
    #                 created_at=j.get("created_at"),
    #                 merged_at=j.get("merged_at"),
    #             )
    #         if (
    #             typ == "issue"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/issues/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             return LinkSummary(
    #                 typ="issue",
    #                 owner=owner,
    #                 repo=repo,
    #                 ident=ident,
    #                 url=f"{base}/issues/{ident}",
    #                 title=j.get("title"),
    #                 created_at=j.get("created_at"),
    #                 closed_at=j.get("closed_at"),
    #             )
    #         if (
    #             typ == "commit"
    #             and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{ident}"))
    #             and isinstance(j, dict)
    #         ):
    #             first_line = j["commit"]["message"].splitlines()[0]
    #             return LinkSummary(
    #                 typ="commit",
    #                 owner=owner,
    #                 repo=repo,
    #                 ident=ident,
    #                 url=f"{base}/commit/{ident}",
    #                 message=first_line,
    #                 short_sha=ident[:7],
    #                 created_at=(j.get("commit", {}).get("author", {}) or {}).get("date"),
    #             )
    #     except (KeyError, ValueError, TypeError):
    #         return None
    #     return None

    # New shared helpers migrated from ReportBuilder

    # UNUSED: kept for reference only
    # def extract_xrefs_from_timeline(timeline_raw: list[dict]) -> list[str]:
    #     """Extract cross-references from issue timeline events."""
    #     xrefs: list[str] = []
    #     for event in timeline_raw:
    #         try:
    #             src_issue = event.get("source", {}).get("issue")
    #             if not src_issue:
    #                 continue
    #             b = (src_issue.get("body") or "").strip()
    #             if b:
    #                 xrefs.append(b)
    #         except Exception:
    #             logger.exception("Failed to extract cross-reference from event")
    #             continue
    #     return xrefs

    # UNUSED: kept for reference only
    # def expand_issue_details(issue_data: list[dict[str, Any]], anonymize: bool = False) -> str:
    #     """Combine issue metadata into a markdown-friendly string.
    #
    #     Args:
    #         issue_data: List of issue metadata dicts
    #         anonymize: Whether to anonymize content
    #     """
    #     if not issue_data:
    #         return ""
    #
    #     issue_texts: list[str] = []
    #     for index, issue in enumerate(issue_data):
    #         segments: list[str] = [f"Issue {index}: {issue.get('title', '')}\n"]
    #
    #         body = issue.get("body")
    #         if body:
    #             segments.append(f"Description:\n{body}\n")
    #
    #         comments = [comment for comment in issue.get("comments", []) if comment]
    #         if comments:
    #             comment_lines = "\n".join(f"- {comment}" for comment in comments)
    #             segments.append(f"Comments ({len(comments)}):\n{comment_lines}\n")
    #
    #         cross_refs = [xref for xref in issue.get("cross_references", []) if xref]
    #         if cross_refs:
    #             cross_lines = "\n".join(f"- {xref}" for xref in cross_refs)
    #             segments.append(f"Cross-references ({len(cross_refs)}):\n{cross_lines}\n")
    #
    #         issue_texts.append("".join(segments))
    #
    #     combined = " ".join(issue_texts)
    #     return anonymize_github_issue(combined) if anonymize else combined

    # UNUSED: kept for reference only
    # def expand_issue_details_from_model(issues: list[IssueExpanded], anonymize: bool = False) -> str:
    #     """Expand IssueExpanded models into a markdown-friendly string.
    #
    #     Converts models into dicts expected by `expand_issue_details` and delegates rendering.
    #     """
    #     payloads: list[dict[str, Any]] = [
    #         {
    #             "title": it.title,
    #             "body": it.description,
    #             "comments": list(it.comments),
    #             "cross_references": list(it.cross_references),
    #         }
    #         for it in issues
    #     ]
    #     return expand_issue_details(payloads, anonymize=anonymize)

    # UNUSED: kept for reference only
    # def expand_issue_details_model(issue_data: list[dict[str, Any]], owner: str, repo: str) -> list[IssueExpanded]:
    #     """Convert issue payloads into structured `IssueExpanded` objects.
    #
    #     Owner/Repo are used only to create canonical URLs.
    #     """
    # out: list[IssueExpanded] = []
    # base = f"https://github.com/{owner}/{repo}"
    # for issue in issue_data:
    #     try:
    #         number_s = str(issue.get("number", "0"))
    #         number = int(number_s) if number_s.isdigit() else 0
    #         url = f"{base}/issues/{number}" if number else base
    #         out.append(
    #             IssueExpanded(
    #                 number=number,
    #                 title=issue.get("title", ""),
    #                 url=url,
    #                 description=issue.get("body", "") or "",
    #                 comments=tuple(c for c in issue.get("comments", []) if c),
    #                 cross_references=tuple(x for x in issue.get("cross_references", []) if x),
    #                 created_at=issue.get("created_at"),
    #                 closed_at=issue.get("closed_at"),
    #             )
    #         )
    #     except Exception:
    #         logger.exception("Failed to build IssueExpanded model")
    #         continue
    # return out


# UNUSED: kept for reference only
# def build_issue_context(
#     pr_body: str,
#     owner: str,
#     repo: str,
#     pr_number: int | None = None,
#     *,
#     anonymize: bool = False,
#     pr_created_at: str | None = None,
# ) -> tuple[str, str, list[IssueExpanded], str, str]:
#     """Construct issue/problem strings plus raw variants and metadata.
#
#     Returns:
#         Tuple of (git_problem_str, git_issue_str, issue_data, git_problem_str_raw, git_issue_str_raw)
#     """
#     # Prefer provided PR creation time; only fetch if not supplied and a PR number is available
#     if pr_created_at is None and pr_number is not None:
#         try:
#             pr_meta = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}")
#             if isinstance(pr_meta, dict):
#                 pr_created_at = pr_meta.get("created_at") or None
#         except Exception:
#             pr_created_at = None
#     issue_data = extract_issues_from_description(pr_body, owner, repo, pr_created_at=pr_created_at)
#
#     # Fallback: if no issues referenced in body, try early author comments for hints
#     if not issue_data and pr_number is not None:
#         try:
#             pr_meta = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}")
#             pr_author = pr_meta.get("user", {}).get("login") if isinstance(pr_meta, dict) else None
#         except Exception:
#             pr_author = None
#
#         if pr_author:
#             try:
#                 comments = issue_comments(owner, repo, pr_number)
#                 # Concatenate only author comments (early mentions) to avoid late unrelated references
#                 author_text = "\n\n".join(
#                     (c.get("body") or "") for c in comments if c.get("user", {}).get("login") == pr_author
#                 )
#                 if author_text.strip():
#                     from_comments = extract_issues_from_description(
#                         author_text, owner, repo, pr_created_at=pr_created_at
#                     )
#                     # Merge, preserving order and uniqueness by number
#                     seen = {str(i.number) for i in issue_data}
#                     for it in from_comments:
#                         num = str(it.number)
#                         if num and num not in seen:
#                             issue_data.append(it)
#                             seen.add(num)
#             except Exception as e:
#                 # Non-fatal fallback
#                 logger.debug(f"Failed to extract issues from comment: {e}")
#     git_problem_str_raw = pr_body
#     git_problem_str = anonymize_github_issue(git_problem_str_raw) if anonymize else git_problem_str_raw
#
#     git_issue_str_raw = expand_issue_details_from_model(issue_data, anonymize=False)
#     git_issue_str = expand_issue_details_from_model(issue_data, anonymize=anonymize)
#     return git_problem_str, git_issue_str, issue_data, git_problem_str_raw, git_issue_str_raw


# UNUSED: kept for reference only
# def get_pr_change_summary(owner: str, repo: str, pr_number: int) -> str:
#     """Get file change summary for a PR as a markdown table (paginated)."""
#     api = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files"
#     headers = {"Accept": "application/vnd.github+json"}
#     lines = [
#         "| File | Status | Lines Added | Lines Removed | Total Changes |",
#         "|------|--------|-------------|---------------|---------------|",
#     ]
#     total_add = total_del = total_changes = 0
#     page = 1
#
#     while True:
#         resp = requests.get(api, headers=headers, params={"per_page": 100, "page": page}, timeout=30)
#         resp.raise_for_status()
#         files = resp.json()
#         if not files:
#             break
#
#         for f in files:
#             status = f.get("status", "")
#             filename = f.get("filename", "")
#             if status == "renamed" and f.get("previous_filename"):
#                 filename = f"{f['previous_filename']} ➜ {f['filename']}"
#
#             added = f.get("additions", 0)
#             deleted = f.get("deletions", 0)
#             changes = f.get("changes", added + deleted)
#
#             lines.append(f"| {filename} | {status} | {added} | {deleted} | {changes} |")
#
#             total_add += added
#             total_del += deleted
#             total_changes += changes
#
#         if 'rel="next"' not in resp.headers.get("Link", ""):
#             break
#         page += 1
#
#     lines.append(f"| **TOTAL** |  | **{total_add}** | **{total_del}** | **{total_changes}** |")
#     return "\n".join(lines)


# UNUSED: kept for reference only
# def get_pr_change_summary_model(owner: str, repo: str, pr_number: int) -> PRChangeSummary:
#     """Return structured change summary for a PR (paginated)."""
#     api = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files"
#     headers = {"Accept": "application/vnd.github+json"}
#     files: list[PRFileChange] = []
#     total_add = total_del = total_changes = 0
#     page = 1
#
#     while True:
#         resp = requests.get(api, headers=headers, params={"per_page": 100, "page": page}, timeout=30)
#         resp.raise_for_status()
#         rows = resp.json()
#         if not rows:
#             break
#
#         for f in rows:
#             status = f.get("status", "")
#             filename = f.get("filename", "")
#             prev = f.get("previous_filename")
#             display = f"{prev} ➜ {filename}" if status == "renamed" and prev else filename
#             added = int(f.get("additions", 0) or 0)
#             deleted = int(f.get("deletions", 0) or 0)
#             changes = int(f.get("changes", added + deleted) or (added + deleted))
#
#             files.append(
#                 PRFileChange(
#                     filename=display,
#                     status=status,
#                     additions=added,
#                     deletions=deleted,
#                     changes=changes,
#                     previous_filename=prev,
#                 )
#             )
#
#             total_add += added
#             total_del += deleted
#             total_changes += changes
#
#         if 'rel="next"' not in resp.headers.get("Link", ""):
#             break
#         page += 1
#
#     return PRChangeSummary(
#         files=files,
#         total_additions=total_add,
#         total_deletions=total_del,
#         total_changes=total_changes,
#     )


# UNUSED: kept for reference only
# def compose_judge_problem_description(*, pr_title: str, pr_body: str, git_issue_str: str, comments_text: str) -> str:
#     """Compose a rich, self-contained problem description for the judge/classifier."""
#     parts: list[str] = []
#     if pr_title:
#         parts.append(f"PR Title:\n{pr_title}\n")
#     if pr_body:
#         parts.append(f"PR Body:\n{pr_body}\n")
#     if git_issue_str:
#         parts.append(f"Referenced Issues (expanded):\n{git_issue_str}\n")
#     if comments_text:
#         parts.append(f"Discussion Comments:\n{comments_text}\n")
#     return "\n".join(parts).strip()


# UNUSED: kept for reference only
# def md_commit_block(c: dict, owner: str, repo: str) -> str:
#     message = c["message"].replace("\n", "\n  ")
#     return textwrap.dedent(
#         f"""
#         Generic Information:
#          - Commit id: {c["sha"]}
#          - Commit: https://github.com/{owner}/{repo}/commit/{c["sha"]}
#          - Date of Commit: {c["date_iso"]}
#         ## Commit message
#           {message}
#         """
#     ).strip("\n")


# UNUSED: kept for reference only
# def md_pr_header(pr: dict) -> str:
#     if not len(pr):
#         return "_No pull-request metadata available._"
#     labels = ", ".join(label["name"] for label in pr["labels"]) or "—"
#     milestone = pr["milestone"]["title"] if pr["milestone"] else "—"
#     merged = pr["merged_at"] if pr["merged_at"] else "not-merged"
#     merged_by = pr["merged_by"]["login"] if pr["merged_by"] else pr["user"]["login"]
#     return textwrap.dedent(
#         f"""
#         ### Link 1: {pr["title"]} · Pull Request #{pr["number"]} · {pr["base"]["repo"]["full_name"]}
#
#         Merged by **@{merged_by}** on **{merged}**
#         Labels: {labels} — Milestone: {milestone}
#
#         ## GitHub Comments
#         """
#     ).strip("\n")


# UNUSED: kept for reference only
# def md_comment(item: dict, kind: str) -> str:
#     body = item.get("body") or ""
#     excerpt = body.strip().replace("\r\n", "\n")
#     # excerpt = excerpt[:400] + ("…" if len(excerpt) > 400 else "")
#     ts_field = "submitted_at" if kind == "review" else "created_at"
#     ts_iso = item[ts_field]
#     return textwrap.dedent(
#         f"""
#         **{item["user"]["login"]}** — {iso(ts_iso)}
#
#         {excerpt}
#         """
#     ).strip("\n")


def anonymize_github_issue(text: str) -> str:
    """
    Remove identifying information (URLs, emails, usernames, repo names, issue numbers)
    from a GitHub issue description so that it cannot be traced back.

    - Fix: commit SHA rule now requires at least one hex letter [a-f] to avoid eating long numbers.
    - Fix: emails are scrubbed *before* @mentions so emails don't get half-redacted.
    - Extra: handles ssh-style GitHub remotes.
    """

    # 1) Emails FIRST (avoid @mention rule hitting '@example' inside an email)
    text = re.sub(
        r"(?<![\w.+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        "[EMAIL]",
        text,
    )

    # 2) GitHub URLs (https and ssh)
    text = re.sub(r"https?://(?:www\.)?github\.com/[^\s)>\]]+", "[GITHUB_URL]", text)
    text = re.sub(r"git@github\.com:[\w.-]+/[\w.-]+(?:\.git)?", "[GITHUB_SSH_URL]", text)

    # 3) Issue/PR references
    text = re.sub(r"(?<!\w)#\d+\b", "[ISSUE_NUM]", text)
    text = re.sub(r"\bGH-\d+\b", "[ISSUE_NUM]", text, flags=re.IGNORECASE)

    # 4) User mentions (avoid decorators like @pytest.mark by not matching when a dot follows)
    text = re.sub(r"(?<!\w)@[A-Za-z0-9-]{1,39}(?!\.[A-Za-z])", "[USER]", text)

    # 5) Commit SHAs (7-40 hex chars) — MUST contain at least one letter to avoid numeric runs
    def _sha_repl(m: re.Match) -> str:
        s = m.group(0)
        return "[COMMIT_SHA]" if re.search(r"[A-Fa-f]", s) else s

    text = re.sub(r"\b[0-9A-Fa-f]{7,40}\b", _sha_repl, text)

    # If you decide you also want to redact bare owner/repo slugs in prose, uncomment below.
    # Beware this can be noisy if your issues contain generic path-like strings.
    # text = re.sub(r"\b[\w.-]+/[\w.-]+\b", "[REPO]", text)

    return text


# def get_pr_change_summary_from_url(owner: str, repo: str, pr_number: int) -> str:
#     api = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files"

#     headers = {"Accept": "application/vnd.github+json"}
#     lines = [
#         "| File | Status | Lines Added | Lines Removed | Total Changes |",
#         "|------|--------|-------------|---------------|---------------|",
#     ]
#     total_add = total_del = total_changes = 0
#     page = 1

#     while True:
#         resp = requests.get(api, headers=headers, params={"per_page": 100, "page": page}, timeout=30)
#         resp.raise_for_status()
#         files = resp.json()
#         if not files:
#             break

#         for f in files:
#             status = f.get("status", "")
#             filename = f.get("filename", "")
#             if status == "renamed" and f.get("previous_filename"):
#                 filename = f"{f['previous_filename']} ➜ {f['filename']}"

#             added = f.get("additions", 0)
#             deleted = f.get("deletions", 0)
#             changes = f.get("changes", added + deleted)

#             lines.append(f"| {filename} | {status} | {added} | {deleted} | {changes} |")

#             total_add += added
#             total_del += deleted
#             total_changes += changes

#         # pagination: check if there's another page
#         if 'rel="next"' not in resp.headers.get("Link", ""):
#             break
#         page += 1

#     lines.append(f"| **TOTAL** |  | **{total_add}** | **{total_del}** | **{total_changes}** |")
#     return "\n".join(lines)


# def problem_statement(owner: str, repo: str, num: int) -> tuple[str, str]:
#     """Returns a summary of the main issue of the pull request."""
#     pr = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
#     if not pr or not isinstance(pr, dict):
#         return "NOT_A_VALID_PR", ""
#     issue_url = pr["issue_url"]
#     git_problem_str, git_issue_str = summarize(issue_url)

#     return git_problem_str, git_issue_str


# def summarize(issue_url: str) -> tuple[str, str]:
#     """Takes in a URL and provides a summary of the comments in it.
#     Used as helpr function to generate problem_statement and hints.
#     Purely heuristc in nature (No LLM calls being made.)
#     """
#     parsed = urlparse(issue_url)
#     endpoint = parsed.path.lstrip("/")
#     issue = _get_github_metadata(endpoint)  # THIS IS THE PR ITSELF
#     description = ""
#     if issue and isinstance(issue, dict) and issue["body"]:
#         description += issue["body"]
#     marker = "<!-- Optional opt-out -->"
#     if marker in description:
#         description = description.split(marker, 1)[0].strip()
#     parts = parsed.path.strip("/").split("/")
#     owner, repo = parts[1], parts[2]

#     prob_stat = [description + "\n"]
#     issue_stat = []

#     # Extracting all issue numbers
#     issues = re.findall(r"#(\d+)", description)
#     logger.debug(description)
#     if issues == []:
#         return "NOT_A_VALID_PR", ""
#     for i, iss in enumerate(issues):
#         endpoint = f"/repos/{owner}/{repo}/issues/{iss}"
#         issue_thread = _get_github_metadata(endpoint)
#         stat = ""
#         if issue_thread and isinstance(issue_thread, dict):
#             issue_comments_list = issue_comments(owner, repo, iss)
#             stat = f"Issue {i}:" + issue_thread["title"] + "\n"
#             stat = (
#                 stat
#                 + f"Issue {i} comments:"
#                 + "\n".join(comment.get("body", "") for comment in issue_comments_list)
#                 + "\n"
#             )
#             stat = (
#                 stat
#                 + f"Issue {i} cross-referenced comments:"
#                 + "\n".join(comment["source"]["issue"]["body"] for comment in issue_timeline(owner, repo, iss))
#                 + "\n"
#             )
#         issue_stat.append(stat)

#     git_problem_str = " ".join(str(x) for x in prob_stat)
#     git_problem_str = anonymize_github_issue(git_problem_str)
#     git_issue_str = " ".join(str(x) for x in issue_stat)
#     git_issue_str = anonymize_github_issue(git_issue_str)
#     # classify whether performance improving or not.

#     return git_problem_str, git_issue_str


# def summarize_llm(issue_history: str, issue_stat: str) -> str:
#     try:
#         pred = ISSUE_STRUCTURER(issue_history, issue_stat)
#         # pred is a dspy.Prediction with attribute .summary
#         summ = getattr(pred, "structured_issue", "NOT FOUND")
#         return str(summ).strip()
#     except Exception as e:
#         # Fallback behavior if the LLM call fails for any reason
#         return f"[structure failed: {e}]"


# def summarize_comments(github_comments: str) -> str:
#     try:
#         pred = COMMENT_SUMMARIZER(message=github_comments)
#         # pred is a dspy.Prediction with attribute .summary
#         out = getattr(pred, "summary", "NOT FOUND")
#         return str(out).strip()
#     except Exception as e:
#         # Fallback behavior if the LLM call fails for any reason
#         return f"[summarization failed: {e}]"


# UNUSED: kept for reference only
# def _collect_pr_comments(owner: str, repo: str, num: int) -> tuple[list[str], set[str]]:
#     """Collect all comments from a PR and extract links."""
#     comment_links: set[str] = set()
#     github_comments = []
#
#     for c in issue_comments(owner, repo, num):
#         comment_links.update(extract_links(c["body"]))
#         github_comments.append(md_comment(c, "issue"))
#     logger.debug("got issue comments")
#
#     for rc in review_comments(owner, repo, num):
#         comment_links.update(extract_links(rc["body"]))
#         github_comments.append(md_comment(rc, "review_comment"))
#     logger.debug("got review comments")
#
#     for rv in reviews(owner, repo, num):
#         comment_links.update(extract_links(rv["body"]))
#         github_comments.append(md_comment(rv, "review"))
#     logger.debug("got reviews")
#
#     return github_comments, comment_links


# UNUSED: kept for reference only
# def _process_linked_resources(comment_links: set[str], visited_links: set[str]) -> list[str]:
#     """Process and summarize linked resources from comments."""
#     link_summaries = []
#     sub_links = [label for label in comment_links if label not in visited_links][:MAX_LINKS_TO_FOLLOW]
#     if sub_links:
#         link_summaries.append("\n### Links found inside comments (level 2)\n")
#     for link in sub_links:
#         visited_links.add(link)
#         cls = classify_gh_link(link)
#         if cls:
#             link_summaries.append(summarize_gh_resource(cls))
#         else:
#             link_summaries.append(f"* <{link}>")
#     logger.debug("got links found inside comments")
#     return link_summaries


# def build_report(
#     owner: str, repo: str, num: int, patch: str, llm: bool, add_classification: bool = False
# ) -> tuple[str, str, str, str, str, bool]:
#     out_parts = []
#     visited_links: set[str] = {""}
#     logger.debug("got meta-data")

#     # Collect comments and links
#     github_comments, comment_links = _collect_pr_comments(owner, repo, num)

#     # Summarize comments if LLM is enabled
#     comment_summary = ""
#     out_parts.append("\n### Hints\n")
#     if llm:
#         comment_summary = summarize_comments("\n\n".join(github_comments))
#         out_parts.append(comment_summary)
#     else:
#         print(github_comments, "GITHUB_COMMENTS")
#         comment_summary = "\n\n".join(github_comments)
#     logger.debug("got comment summary")

#     # Process linked resources
#     out_parts.extend(_process_linked_resources(comment_links, visited_links))

#     # Build problem statement
#     issue_history, issue_stat = problem_statement(owner, repo, num)
#     if issue_history == "NOT_A_VALID_PR":
#         return "NOT_A_VALID_PR", "", "", "", "", False

#     # Check if the issue is a performance issue
#     is_performance_commit = False
#     if llm:
#         file_change = get_pr_change_summary_from_url(owner, repo, num)
#         is_performance_commit, json_response = PERF_CLASSIFIER.get_response(
#             message=issue_history, file_change_summary=file_change, git_patch=patch
#         )
#         if is_performance_commit:
#             out_parts.append("\n### Performance Issue")
#             out_parts.append(json_response)
#         else:
#             logger.debug("NOT A PERFORMANCE COMMIT")
#             return "NOT_A_PERFORMANCE_COMMIT", "", "", "", "", False

#     problem_stat = ""
#     if llm:
#         out_parts.append("\n### LLM Generated summary")
#         problem_stat = summarize_llm(issue_history, issue_stat)
#         out_parts.append(problem_stat)
#     else:
#         out_parts.append("\n### Problem Statement\n")
#         out_parts.append(issue_history)
#         problem_stat = issue_history
#     logger.debug("got problem statement")

#     # Add classification if requested
#     cat, diff = ("", "")
#     if add_classification:
#         out_parts.append("\n### Classification")
#         cat, diff = classification(issue_history, owner, repo, patch)
#         logger.debug("got classification")
#         out_parts.append(cat)
#         out_parts.append("\n### Difficulty")
#         out_parts.append(diff)
#         logger.debug("got difficulty")

#     return "\n\n".join(out_parts), problem_stat, comment_summary, cat, diff, is_performance_commit


# UNUSED: kept for reference only
# def save_markdown(report: str, filepath: str) -> None:
#     """Save a Markdown string to a .md file, creating directories if needed."""
#     path = Path(filepath)
#     path.parent.mkdir(parents=True, exist_ok=True)
#     path.write_text(report, encoding="utf-8")


# def breakpoints_scrape_comments(
#     breakpoints_df: pd.DataFrame, coverage_df: pd.DataFrame, index_data: dict[str, typing.Any]
# ) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """Generate GitHub commit reports and return an enriched *merged* DataFrame.

#     * `coverage_df` **must** exist - it is produced by `--compute-coverage`.
#     * Each report is saved as `<reports_dir>/<commit_hash>.md`.
#     * The returned DataFrame includes an `n_tokens` column.
#     """
#     bp = breakpoints_df.copy()
#     bp["gt_url"] = bp["gt_hash"].astype(str).map(lambda h: urllib.parse.urljoin(index_data["show_commit_url"], h))

#     if coverage_df is not None:
#         # Average coverage per commit for the ground-truth hash
#         gt_hashes = coverage_df.dropna().query("typ == 'gt_hash'").groupby(["url"])["coverage"].mean().reset_index()
#         merged_df = bp.merge(gt_hashes, how="inner", left_on="gt_url", right_on="url")
#     else:
#         merged_df = bp.copy()

#     # ---------------------------------------------------------------- reports
#     reports = []
#     encoding = tiktoken.encoding_for_model("gpt-4o-mini")
#     url2token: dict[str, int] = {}
#     for gt_url in tqdm.tqdm(merged_df.gt_url.unique(), desc="Reports", unit="commit"):
#         owner, repo, sha = _parse_commit_url(gt_url)
#         logger.debug("Patch required for full usuage")
#         report, _, _, _, _, _ = build_report(owner, repo, int(sha), "", llm=False)
#         commit_hash = urllib.parse.urlparse(gt_url).path.split("/")[-1]
#         n_tokens = len(encoding.encode(report))
#         reports.append({
#             "commit_hash": commit_hash,
#             "report": report,
#             "gt_url": gt_url,
#             "n_tokens": n_tokens,
#         })

#         url2token[gt_url] = n_tokens

#     merged_df["n_tokens"] = merged_df["gt_url"].map(url2token)
#     reports_df = pd.DataFrame(reports)
#     return merged_df, reports_df


# def classification(problem_desc: str, owner: str, repo: str, git_patch: str) -> ClassificationDecision:
#     """Classify the given git commit and associated solution to one of these categories:
#     - Better data structure
#     - Better algorithm
#     - Use a lower-level system
#     - Accept a less-precise solution
#     - Use parallelization
#     - Remove redundancy
#     - Cache and reuse
#     - Improve/introduce scaling
#     - Database and storage tuning
#     - Micro-optimizations

#     Inputs: problem description, git patch (the proposed solution)

#     Returns:
#         A ``ClassificationDecision`` with rationale, category, difficulty, and confidence.
#     """
#     try:
#         return CLASSIFY_JUDGE(message=problem_desc, patch=git_patch)
#     except Exception as e:
#         logger.error(f"Classification failed: {e}", exc_info=True)
#         return ClassificationDecision(
#             reason=f"classification failed: {e}",
#             category="",
#             difficulty="",
#             confidence=None,
#         )


# def build(df: pd.DataFrame, args: argparse.Namespace) -> None:
#     result_map: dict[str, dict[str, str]] = {}
#     mark = args.markdown
#     save_folder = args.save_location
#     for i in range(len(df)):
#         name = df["container_name"][i]
#         owner = name.split("-", 2)[0]
#         repo = name.split("-", 2)[1]
#         try:
#             report, prob_stat, hints, classif, diffi, perf = build_report(
#                 owner, repo, int(df["base_commit"][i]), df["patch"][i], args.summarize_llm
#             )
#         except Exception:
#             logger.debug("ERROR")
#             continue
#         if mark:
#             save_markdown(report, f"{args.save_location}/commit_report_{i}.md")
#         result_map[df["base_commit"][i]] = {
#             "problem_statement": prob_stat,
#             "hints": hints,
#             "classification": classif,
#             "difficulty": diffi,
#         }
#     full_path = os.path.join(save_folder, "data.json")
#     with open(full_path, "w") as f:
#         json.dump(result_map, f, indent=4)


# def build_pr_report(
#     link: str, summarize_llm: bool, add_classification: bool, patch: str
# ) -> tuple[str, str, str, str, str, bool]:
#     owner, repo, num = _parse_pr_url(link)
#     # logger.debug(problem_statement(owner=owner, repo=repo, num=num))
#     report, prob_stat, hints, classif, diffi, perf = build_report(
#         owner=owner,
#         repo=repo,
#         num=int(num),
#         patch=patch,
#         llm=summarize_llm,
#         add_classification=add_classification,
#     )

#     return report, prob_stat, hints, classif, diffi, perf
#     # if report == "NOT_A_VALID_PR":
#     #     logger.debug(f"{link}: NOT_A_VALID_PR")
#     #     return


# build(commits_df)

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         "--df_location",
#         # default="/mnt/sdd1/atharvas/formulacode/datasmith/scratch/artifacts/processed/useful_commits.csv",
#         default="/mnt/sdd1/atharvas/formulacode/datasmith/scratch/artifacts/processed/downloads/useful_enriched.tbformat_2025-09-13T23:38:29.017709.parquet",
#         help="location of dataframe with the commits",
#     )
#     parser.add_argument(
#         "--save_location",
#         default="/mnt/sdd1/akanksha/formulacode/datasmith/src/datasmith/scrape/data",
#         help="location to save instruction markdown and json with problem statement and hints",
#     )
#     parser.add_argument(
#         "--markdown",
#         default=True,
#         help="boolean (marks whether to create markdown file or not)",
#     )
#     parser.add_argument(
#         "--summarize_llm",
#         default=True,
#         help="boolean (marks whether to use llm summarization or just heuristics)",
#     )
#     parser.add_argument(
#         "--link",
#         default="https://github.com/astropy/astropy/pull/16088",
#         help="to test individual links",
#     )
#     args = parser.parse_args()

#     # Get just the issue number

#     MAX_LINKS_TO_FOLLOW = 60
#     ISSUE_STRUCTURER = LLMStructurer()
#     COMMENT_SUMMARIZER = LLMCommentSummarizer()
#     CLASSIFY_JUDGE = ClassifyJudge()

#     # _______________________________________________________________________________________

#     # # Dataframe with git commits to generate reports for

#     # # meta = pq.read_metadata(Path(args.df_location))     # if this fails, the file/ footer is broken
#     # # logger.debug(meta.num_row_groups, meta.schema)
#     # path = Path(args.df_location)
#     # from pathlib import Path
#     # p = Path(args.df_location)
#     # pf = pq.ParquetFile(p)
#     # logger.debug(pf)                # nice summary (row groups, columns, etc.)
#     # logger.debug(pf.schema)         # full Parquet schema
#     # logger.debug(pf.metadata)       # detailed file metadata
#     # logger.debug(pf.num_row_groups) # number of row groups

#     # # Try reading in chunks
#     # parquet_file = pq.ParquetFile(p)
#     # columns = parquet_file.schema.names

#     # try:
#     #     pf = fastparquet.ParquetFile(p)
#     #     logger.debug("FastParquet can open file")
#     #     logger.debug("Columns:", pf.columns)
#     #     logger.debug("Schema:", pf.schema)

#     #     # Try to read with fastparquet directly
#     #     df = pf.to_pandas()
#     #     logger.debug("FastParquet read successful!")
#     # except Exception as e:
#     #     logger.debug(f"FastParquet also failed: {e}")

#     # logger.debug(df.columns)
#     # logger.debug(len(df))
#     # small_df = df.head()
#     # logger.debug(small_df)
#     # build(df, args)

#     # _______________________________________________________________________________________
#     report, prob_stat, hints, classif, diffi, perf = build_pr_report(
#         args.link, args.summarize_llm, add_classification=False, patch=args.patch
#     )
#     if report and "NOT_A_VALID_PR" not in report and Path(args.save_location).exists():
#         save_markdown(report, f"{args.save_location}/pr_report_{args.link}.md")
