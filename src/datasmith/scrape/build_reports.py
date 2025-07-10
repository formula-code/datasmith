from __future__ import annotations

import re
import textwrap
import typing
import urllib.parse
from collections.abc import Sequence
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
import tiktoken
import tqdm

from datasmith.scrape.utils import _parse_commit_url
from datasmith.utils import CACHE_LOCATION, _get_github_metadata, cache_completion

MAX_LINKS_TO_FOLLOW = 60  # safety cap for level-2 traversal


def iso(ts: str) -> str:
    dt = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)
    return dt.strftime("%H:%M %d/%m/%Y")


def extract_links(text: str) -> list[str]:
    return re.findall(r"https?://[^\s)<>\]]+", text or "")


def fetch_commit(owner: str, repo: str, sha: str) -> dict:
    commit_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{sha}")
    return {
        "sha": sha,
        "date_iso": commit_metadata["commit"]["author"]["date"],
        "message": commit_metadata["commit"]["message"],
    }


def prs_for_commit(owner: str, repo: str, sha: str) -> Sequence[dict]:
    pulls_metadata: Sequence[dict] = _get_github_metadata(
        endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100"
    )
    return pulls_metadata
    # url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/pulls"
    # return gh_get(url, params={"per_page": 100})


def pr_meta(owner: str, repo: str, num: int) -> dict:
    pr_metadata: dict[str, typing.Any] = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
    if not pr_metadata:
        return {}
    return pr_metadata


def issue_comments(owner: str, repo: str, num: int) -> list[dict]:
    issue_metadata: list[dict] = _get_github_metadata(
        endpoint=f"/repos/{owner}/{repo}/issues/{num}/comments?per_page=100"
    )
    if not issue_metadata:
        return []
    return issue_metadata


def review_comments(owner: str, repo: str, num: int) -> list[dict]:
    review_comments_metadata: list[dict] = _get_github_metadata(
        endpoint=f"/repos/{owner}/{repo}/pulls/{num}/comments?per_page=100"
    )
    if not review_comments_metadata:
        return []
    return review_comments_metadata


def reviews(owner: str, repo: str, num: int) -> list[dict]:
    reviews_metadata: list[dict] = _get_github_metadata(
        endpoint=f"/repos/{owner}/{repo}/pulls/{num}/reviews?per_page=100"
    )
    if not reviews_metadata:
        return []
    return reviews_metadata


def classify_gh_link(u: str) -> tuple[str, ...] | None:
    """
    Return ('type', owner, repo, id)  where type ∈ {'pr', 'issue', 'commit'}
    or None if not recognised as such.
    """
    p = urlparse(u)
    if p.netloc != "github.com":
        return None
    parts = p.path.strip("/").split("/")
    if len(parts) >= 4 and parts[2] == "pull" and parts[3].isdigit():
        return ("pr", parts[0], parts[1], parts[3])
    if len(parts) >= 4 and parts[2] == "issues" and parts[3].isdigit():
        return ("issue", parts[0], parts[1], parts[3])
    if len(parts) >= 4 and parts[2] == "commit":
        return ("commit", parts[0], parts[1], parts[3])
    return None


def summarize_gh_resource(res: tuple[str, ...]) -> str:
    typ, owner, repo, ident = res
    base = f"https://github.com/{owner}/{repo}"
    try:
        if typ == "pr" and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{ident}")):
            return f"* PR #{ident}: {j['title']}  \n  <{base}/pull/{ident}>"
        if typ == "issue" and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/issues/{ident}")):
            return f"* Issue #{ident}: {j['title']}  \n  <{base}/issues/{ident}>"
        if typ == "commit" and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{ident}")):
            first_line = j["commit"]["message"].splitlines()[0]
            return f"* Commit {ident[:7]}: {first_line}  \n  <{base}/commit/{ident}>"
    except (KeyError, ValueError, TypeError):
        return ""
    return ""


def md_commit_block(c: dict, owner: str, repo: str) -> str:
    message = c["message"].replace("\n", "\n  ")
    return textwrap.dedent(
        f"""
        Generic Information:
         - Commit id: {c["sha"]}
         - Commit: https://github.com/{owner}/{repo}/commit/{c["sha"]}
         - Date of Commit: {c["date_iso"]}
        ## Commit message
          {message}
        """
    ).strip("\n")


def md_pr_header(pr: dict) -> str:
    if not len(pr):
        return "_No pull-request metadata available._"
    labels = ", ".join(label["name"] for label in pr["labels"]) or "—"
    milestone = pr["milestone"]["title"] if pr["milestone"] else "—"
    merged = pr["merged_at"] if pr["merged_at"] else "not-merged"
    merged_by = pr["merged_by"]["login"] if pr["merged_by"] else pr["user"]["login"]
    return textwrap.dedent(
        f"""
        ### Link 1: {pr["title"]} · Pull Request #{pr["number"]} · {pr["base"]["repo"]["full_name"]}

        Merged by **@{merged_by}** on **{merged}**
        Labels: {labels} — Milestone: {milestone}

        ## GitHub Comments
        """
    ).strip("\n")


def md_comment(item: dict, kind: str) -> str:
    body = item.get("body") or ""
    excerpt = body.strip().replace("\r\n", "\n")
    # excerpt = excerpt[:400] + ("…" if len(excerpt) > 400 else "")
    ts_field = "submitted_at" if kind == "review" else "created_at"
    ts_iso = item[ts_field]
    return textwrap.dedent(
        f"""
        **{item["user"]["login"]}** — {iso(ts_iso)}

        {excerpt}

        Links mentioned: {", ".join(extract_links(body)) or "—"}
        """
    ).strip("\n")


@cache_completion(CACHE_LOCATION, "build_report")
def build_report(commit_url: str) -> str:
    owner, repo, sha = _parse_commit_url(commit_url)
    commit = fetch_commit(owner, repo, sha)
    out_parts = [md_commit_block(commit, owner, repo), ""]

    prs = prs_for_commit(owner, repo, sha)
    if not prs:
        out_parts.append("_No pull-requests reference this commit._")
        return "\n\n".join(out_parts)

    visited_links: set[str] = {commit_url}

    for pr in prs:
        num = pr["number"]
        pr_meta_full = pr_meta(owner, repo, num)
        out_parts.extend(["", md_pr_header(pr_meta_full)])

        # Collect links from all comment bodies
        comment_links: set[str] = set()

        for c in issue_comments(owner, repo, num):
            out_parts.append(md_comment(c, "issue"))
            comment_links.update(extract_links(c["body"]))

        for rc in review_comments(owner, repo, num):
            out_parts.append(md_comment(rc, "review_comment"))
            comment_links.update(extract_links(rc["body"]))

        for rv in reviews(owner, repo, num):
            out_parts.append(md_comment(rv, "review"))
            comment_links.update(extract_links(rv["body"]))

        # LEVEL-2 SECTION
        sub_links = [label for label in comment_links if label not in visited_links][:MAX_LINKS_TO_FOLLOW]
        if sub_links:
            out_parts.append("\n### Links found inside comments (level 2)\n")
        for link in sub_links:
            visited_links.add(link)
            cls = classify_gh_link(link)
            if cls:
                out_parts.append(summarize_gh_resource(cls))
            else:
                out_parts.append(f"* <{link}>")

    return "\n\n".join(out_parts)


def breakpoints_scrape_comments(
    breakpoints_df: pd.DataFrame, coverage_df: pd.DataFrame, index_data: dict[str, typing.Any]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate GitHub commit reports and return an enriched *merged* DataFrame.

    * `coverage_df` **must** exist - it is produced by `--compute-coverage`.
    * Each report is saved as `<reports_dir>/<commit_hash>.md`.
    * The returned DataFrame includes an `n_tokens` column.
    """
    bp = breakpoints_df.copy()
    bp["gt_url"] = bp["gt_hash"].astype(str).map(lambda h: urllib.parse.urljoin(index_data["show_commit_url"], h))

    if coverage_df is not None:
        # Average coverage per commit for the ground-truth hash
        gt_hashes = coverage_df.dropna().query("typ == 'gt_hash'").groupby(["url"])["coverage"].mean().reset_index()
        merged_df = bp.merge(gt_hashes, how="inner", left_on="gt_url", right_on="url")
    else:
        merged_df = bp.copy()

    # ---------------------------------------------------------------- reports
    reports = []
    encoding = tiktoken.encoding_for_model("gpt-4o-mini")
    url2token: dict[str, int] = {}
    for gt_url in tqdm.tqdm(merged_df.gt_url.unique(), desc="Reports", unit="commit"):
        report = build_report(gt_url)
        commit_hash = urllib.parse.urlparse(gt_url).path.split("/")[-1]
        n_tokens = len(encoding.encode(report))
        reports.append({
            "commit_hash": commit_hash,
            "report": report,
            "gt_url": gt_url,
            "n_tokens": n_tokens,
        })

        url2token[gt_url] = n_tokens

    merged_df["n_tokens"] = merged_df["gt_url"].map(url2token)
    reports_df = pd.DataFrame(reports)
    return merged_df, reports_df
