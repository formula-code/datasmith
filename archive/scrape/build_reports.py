# from __future__ import annotations

# import re
# import textwrap
# import urllib.parse
# from collections.abc import Mapping, Sequence
# from datetime import datetime, timezone
# from typing import Any, cast
# from urllib.parse import urlparse

# import pandas as pd
# import tiktoken
# import tqdm

# from datasmith.core.api.github_client import get_github_metadata
# from datasmith.core.cache import CACHE_LOCATION, cache_completion
# from datasmith.core.file_utils import parse_commit_url
# from datasmith.scrape.scrape_dashboards import get_commit_url_from_index

# MAX_LINKS_TO_FOLLOW = 60  # safety cap for level-2 traversal


# def _fetch_metadata_dict(endpoint: str) -> dict[str, Any] | None:
#     data = get_github_metadata(endpoint=endpoint)
#     return data if isinstance(data, dict) else None


# def _fetch_metadata_sequence(endpoint: str) -> list[dict[str, Any]]:
#     data = get_github_metadata(endpoint=endpoint)
#     if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
#         return [item for item in data if isinstance(item, dict)]
#     return []


# def iso(ts: str) -> str:
#     try:
#         dt = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)
#     except ValueError:
#         return ts
#     return dt.strftime("%H:%M %d/%m/%Y")


# def extract_links(text: str) -> list[str]:
#     return re.findall(r"https?://[^\s)<>\]]+", text or "")


# def fetch_commit(owner: str, repo: str, sha: str) -> dict[str, Any]:
#     commit_metadata = _fetch_metadata_dict(endpoint=f"/repos/{owner}/{repo}/commits/{sha}")
#     if not commit_metadata:
#         raise ValueError(f"No commit metadata available for {repo}@{sha}")
#     commit_info = commit_metadata.get("commit")
#     if not isinstance(commit_info, dict):
#         raise TypeError("Malformed commit payload: missing commit field")
#     author_info = commit_info.get("author")
#     if not isinstance(author_info, dict):
#         raise TypeError("Malformed commit payload: missing author field")

#     message = commit_info.get("message")
#     if not isinstance(message, str):
#         message = ""

#     date_iso = author_info.get("date")
#     if not isinstance(date_iso, str):
#         date_iso = ""

#     return {
#         "sha": sha,
#         "date_iso": date_iso,
#         "message": message,
#     }


# def prs_for_commit(owner: str, repo: str, sha: str) -> list[dict[str, Any]]:
#     return _fetch_metadata_sequence(endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100")
#     # url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/pulls"
#     # return gh_get(url, params={"per_page": 100})


# def pr_meta(owner: str, repo: str, num: int) -> dict[str, Any]:
#     return _fetch_metadata_dict(endpoint=f"/repos/{owner}/{repo}/pulls/{num}") or {}


# def issue_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
#     return _fetch_metadata_sequence(endpoint=f"/repos/{owner}/{repo}/issues/{num}/comments?per_page=100")


# def review_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
#     return _fetch_metadata_sequence(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/comments?per_page=100")


# def reviews(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
#     return _fetch_metadata_sequence(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/reviews?per_page=100")


# def classify_gh_link(u: str) -> tuple[str, ...] | None:
#     """
#     Return ('type', owner, repo, id)  where type ∈ {'pr', 'issue', 'commit'}
#     or None if not recognised as such.
#     """
#     p = urlparse(u)
#     if p.netloc != "github.com":
#         return None
#     parts = p.path.strip("/").split("/")
#     if len(parts) >= 4 and parts[2] == "pull" and parts[3].isdigit():
#         return ("pr", parts[0], parts[1], parts[3])
#     if len(parts) >= 4 and parts[2] == "issues" and parts[3].isdigit():
#         return ("issue", parts[0], parts[1], parts[3])
#     if len(parts) >= 4 and parts[2] == "commit":
#         return ("commit", parts[0], parts[1], parts[3])
#     return None


# def summarize_gh_resource(res: tuple[str, ...]) -> str:
#     typ, owner, repo, ident = res
#     base = f"https://github.com/{owner}/{repo}"
#     try:
#         if typ == "pr":
#             meta = _fetch_metadata_dict(endpoint=f"/repos/{owner}/{repo}/pulls/{ident}")
#             if meta and isinstance(meta.get("title"), str):
#                 return f"* PR #{ident}: {meta['title']}  \n  <{base}/pull/{ident}>"
#         if typ == "issue":
#             meta = _fetch_metadata_dict(endpoint=f"/repos/{owner}/{repo}/issues/{ident}")
#             if meta and isinstance(meta.get("title"), str):
#                 return f"* Issue #{ident}: {meta['title']}  \n  <{base}/issues/{ident}>"
#         if typ == "commit":
#             meta = _fetch_metadata_dict(endpoint=f"/repos/{owner}/{repo}/commits/{ident}")
#             if meta:
#                 commit_info = meta.get("commit")
#                 if isinstance(commit_info, dict):
#                     message = commit_info.get("message")
#                     if isinstance(message, str):
#                         first_line = message.splitlines()[0]
#                         return f"* Commit {ident[:7]}: {first_line}  \n  <{base}/commit/{ident}>"
#     except (KeyError, ValueError, TypeError):
#         return ""
#     return ""


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


# def _labels_text(pr: Mapping[str, Any]) -> str:
#     labels_raw = pr.get("labels")
#     if isinstance(labels_raw, Sequence):
#         names = [lab.get("name") for lab in labels_raw if isinstance(lab, Mapping)]
#         filtered = [name for name in names if isinstance(name, str) and name]
#         if filtered:
#             return ", ".join(filtered)
#     return "—"


# def _milestone_text(pr: Mapping[str, Any]) -> str:
#     milestone_obj = pr.get("milestone")
#     if isinstance(milestone_obj, Mapping):
#         title = milestone_obj.get("title")
#         if isinstance(title, str) and title:
#             return title
#     return "—"


# def _merged_by_text(pr: Mapping[str, Any]) -> str:
#     merged_by_obj = pr.get("merged_by")
#     if isinstance(merged_by_obj, Mapping):
#         login = merged_by_obj.get("login")
#         if isinstance(login, str) and login:
#             return login
#     user_obj = pr.get("user")
#     if isinstance(user_obj, Mapping):
#         login = user_obj.get("login")
#         if isinstance(login, str) and login:
#             return login
#     return "unknown"


# def _merged_at_text(pr: Mapping[str, Any]) -> str:
#     merged_at = pr.get("merged_at")
#     if isinstance(merged_at, str) and merged_at:
#         return merged_at
#     return "not-merged"


# def _base_repo_name(pr: Mapping[str, Any]) -> str:
#     base_repo = pr.get("base")
#     if isinstance(base_repo, Mapping):
#         repo_info = base_repo.get("repo")
#         if isinstance(repo_info, Mapping):
#             full_name = repo_info.get("full_name")
#             if isinstance(full_name, str):
#                 return full_name
#     return ""


# def md_pr_header(pr: dict[str, Any]) -> str:
#     if not pr:
#         return "_No pull-request metadata available._"
#     labels = _labels_text(pr)
#     milestone = _milestone_text(pr)
#     merged = _merged_at_text(pr)
#     merged_by = _merged_by_text(pr)
#     full_name = _base_repo_name(pr)
#     title = pr.get("title") if isinstance(pr.get("title"), str) else ""
#     number = pr.get("number") if isinstance(pr.get("number"), int) else 0
#     return textwrap.dedent(
#         f"""
#         ### Link 1: {title} · Pull Request #{number} · {full_name}

#         Merged by **@{merged_by}** on **{merged}**
#         Labels: {labels} — Milestone: {milestone}

#         ## GitHub Comments
#         """
#     ).strip("\n")


# def md_comment(item: dict[str, Any], kind: str) -> str:
#     body = cast(str, item.get("body") or "")
#     excerpt = body.strip().replace("\r\n", "\n")
#     ts_field = "submitted_at" if kind == "review" else "created_at"
#     ts_value = item.get(ts_field)
#     ts_iso = ts_value if isinstance(ts_value, str) else ""

#     user_obj = item.get("user")
#     user_login = "unknown"
#     if isinstance(user_obj, Mapping):
#         login = user_obj.get("login")
#         if isinstance(login, str):
#             user_login = login
#     ts_formatted = iso(ts_iso) if ts_iso else "unknown"

#     return textwrap.dedent(
#         f"""
#         **{user_login}** — {ts_formatted}

#         {excerpt}
#         """
#     ).strip("\n")


# @cache_completion(CACHE_LOCATION, "build_report")
# def build_report(commit_url: str) -> str:
#     owner, repo, sha = parse_commit_url(commit_url)
#     commit = fetch_commit(owner, repo, sha)
#     out_parts = [md_commit_block(commit, owner, repo), ""]

#     prs = prs_for_commit(owner, repo, sha)
#     if not prs:
#         out_parts.append("_No pull-requests reference this commit._")
#         return "\n\n".join(out_parts)

#     visited_links: set[str] = {commit_url}

#     for pr in prs:
#         pr_number = pr.get("number")
#         if not isinstance(pr_number, int):
#             continue
#         pr_meta_full = pr_meta(owner, repo, pr_number)
#         out_parts.extend(["", md_pr_header(pr_meta_full)])

#         # Collect links from all comment bodies
#         comment_links: set[str] = set()

#         for c in issue_comments(owner, repo, pr_number):
#             out_parts.append(md_comment(c, "issue"))
#             comment_links.update(extract_links(cast(str, c.get("body", ""))))

#         for rc in review_comments(owner, repo, pr_number):
#             out_parts.append(md_comment(rc, "review_comment"))
#             comment_links.update(extract_links(cast(str, rc.get("body", ""))))

#         for rv in reviews(owner, repo, pr_number):
#             out_parts.append(md_comment(rv, "review"))
#             comment_links.update(extract_links(cast(str, rv.get("body", ""))))

#         # LEVEL-2 SECTION
#         sub_links = [label for label in comment_links if label not in visited_links][:MAX_LINKS_TO_FOLLOW]
#         if sub_links:
#             out_parts.append("\n### Links found inside comments (level 2)\n")
#         for link in sub_links:
#             visited_links.add(link)
#             cls = classify_gh_link(link)
#             if cls:
#                 out_parts.append(summarize_gh_resource(cls))
#             else:
#                 out_parts.append(f"* <{link}>")

#     return "\n\n".join(out_parts)


# def breakpoints_scrape_comments(
#     breakpoints_df: pd.DataFrame, coverage_df: pd.DataFrame | None, index_data: dict[str, Any]
# ) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """Generate GitHub commit reports and return an enriched *merged* DataFrame.

#     * `coverage_df` **must** exist - it is produced by `--compute-coverage`.
#     * Each report is saved as `<reports_dir>/<commit_hash>.md`.
#     * The returned DataFrame includes an `n_tokens` column.
#     """
#     bp = breakpoints_df.copy()
#     repo_url = get_commit_url_from_index(index_data)
#     repo_url = repo_url.strip("/") if repo_url else index_data.get("show_commit_url", "")
#     bp["gt_url"] = bp["gt_hash"].astype(str).map(lambda h: urllib.parse.urljoin(repo_url + "/", f"commit/{h}"))

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
#         report = build_report(gt_url)
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
