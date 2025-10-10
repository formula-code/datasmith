from __future__ import annotations

import argparse
import json
import os
import re
import textwrap
import typing
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import tiktoken
import tqdm

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.summ_judge import ClassifyJudge, LLMCommentSummarizer, LLMStructurer
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url, _parse_pr_url
from datasmith.utils import _get_github_metadata

logger = configure_logging()

# configure_agent_backends()
configure_agent_backends(PORTKEY_MODEL_NAME="@togetherai/meta-llama/Llama-3.3-70B-Instruct-Turbo")


MAX_LINKS_TO_FOLLOW = 60  # safety cap for level-2 traversal
ISSUE_STRUCTURER = LLMStructurer()
COMMENT_SUMMARIZER = LLMCommentSummarizer()
CLASSIFY_JUDGE = ClassifyJudge()

# Dataframe with git commits to generate reports for


def iso(ts: str) -> str:
    dt = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)
    return dt.strftime("%H:%M %d/%m/%Y")


def extract_links(text: str) -> list[str]:
    return re.findall(r"https?://[^\s)<>\]]+", text or "")


def fetch_commit(owner: str, repo: str, sha: str) -> dict:
    # endpoint = f"/repos/{owner}/{repo}/commits/{sha}"
    # logger.debug(endpoint)
    commit_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{sha}")
    if commit_metadata and isinstance(commit_metadata, dict):
        return {
            "sha": sha,
            "date_iso": commit_metadata["commit"]["author"]["date"],
            "message": commit_metadata["commit"]["message"],
        }
    return {}


def prs_for_commit(owner: str, repo: str, sha: str) -> list[dict[str, Any]]:
    # pulls_metadata: Sequence[dict] = _get_github_metadata(
    #     endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100"
    # )
    pulls_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{sha}/pulls?per_page=100")
    if pulls_metadata and isinstance(pulls_metadata, list):
        return pulls_metadata
    return []
    # url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}/pulls"
    # return gh_get(url, params={"per_page": 100})


def pr_meta(owner: str, repo: str, num: int) -> dict[str, Any]:
    pr_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
    if pr_metadata and isinstance(pr_metadata, dict):
        return pr_metadata
    return {}


def issue_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    issue_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/issues/{num}/comments?per_page=100")
    if issue_metadata and isinstance(issue_metadata, list):
        return issue_metadata
    return []


def review_comments(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    review_comments_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/comments?per_page=100")
    if review_comments_metadata and isinstance(review_comments_metadata, list):
        return review_comments_metadata
    return []


def reviews(owner: str, repo: str, num: int) -> list[dict[str, Any]]:
    reviews_metadata = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}/reviews?per_page=100")
    if reviews_metadata and isinstance(reviews_metadata, list):
        return reviews_metadata
    return []


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
        if (
            typ == "pr"
            and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{ident}"))
            and isinstance(j, dict)
        ):
            return f"* PR #{ident}: {j['title']}  \n  <{base}/pull/{ident}>"
        if (
            typ == "issue"
            and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/issues/{ident}"))
            and isinstance(j, dict)
        ):
            return f"* Issue #{ident}: {j['title']}  \n  <{base}/issues/{ident}>"
        if (
            typ == "commit"
            and (j := _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/commits/{ident}"))
            and isinstance(j, dict)
        ):
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


def anonymize_github_issue(text: str) -> str:
    """
    Remove identifying information (URLs, emails, usernames, repo names, issue numbers)
    from a GitHub issue description so that it cannot be traced back.
    INPUT:
    text : str (Raw GitHub issue description text.)
    OUTPUT:
    str (Sanitized issue text safe for model input.)
    """

    # GitHub URLs (issues, pulls, commits, repos)
    text = re.sub(r"https?://(?:www\.)?github\.com/[^\s)]+", "[GITHUB_URL]", text)

    #  user mentions --> @username
    text = re.sub(r"@\w+", "[USER]", text)

    # email addresses
    text = re.sub(r"[\w\.-]+@[\w\.-]+\.\w+", "[EMAIL]", text)

    # issue/PR references
    text = re.sub(r"(?<!\w)#\d+", "[ISSUE_NUM]", text)
    text = re.sub(r"GH-\d+", "[ISSUE_NUM]", text)

    # owner/repo patterns
    # text = re.sub(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", "[REPO]", text)

    #  commit SHAs (7-40 hex characters)
    text = re.sub(r"\b[0-9a-f]{7,40}\b", "[COMMIT_SHA]", text)

    # text = re.sub(r"\s{2,}", " ", text).strip() whitespace?
    return text


def problem_statement(owner: str, repo: str, num: int) -> str:
    """Returns a summary of the main issue of the pull request."""
    pr = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{num}")
    out_parts = []
    if not pr or not isinstance(pr, dict):
        out_parts.append("_This pull request is not valid._")
        return "\n\n".join(out_parts)
    problem_stat = []
    issue_url = pr["issue_url"]
    problem_stat.append(summarize(issue_url))

    return " ".join(str(x) for x in problem_stat)


def summarize(issue_url: str) -> str:
    """Takes in a URL and provides a summary of the comments in it.
    Used as helpr function to generate problem_statement and hints.
    Purely heuristc in nature (No LLM calls being made.)
    """
    parsed = urlparse(issue_url)
    endpoint = parsed.path.lstrip("/")
    issue = _get_github_metadata(endpoint)
    description = ""
    if issue and isinstance(issue, dict) and issue["body"]:
        description += issue["body"]
    marker = "<!-- Optional opt-out -->"
    if marker in description:
        description = description.split(marker, 1)[0].strip()
    parts = parsed.path.strip("/").split("/")
    owner, repo = parts[1], parts[2]

    prob_stat = [description + "\n"]

    # Extracting all issue numbers
    issues = re.findall(r"#(\d+)", description)
    logger.debug(description)
    if issues == []:
        return "NOT_A_VALID_PR"
    for iss in issues:
        endpoint = f"/repos/{owner}/{repo}/issues/{iss}"
        issue_thread = _get_github_metadata(endpoint)
        stat = ""
        if issue_thread and isinstance(issue_thread, dict):
            stat = f"Issue {iss}:" + issue_thread["title"] + "\n"
        prob_stat.append(stat)

    git_issue_str = " ".join(str(x) for x in prob_stat)
    git_issue_str = anonymize_github_issue(git_issue_str)
    # classify whether performance improving or not.

    return git_issue_str


def summarize_llm(issue_history: str) -> str:
    try:
        pred = ISSUE_STRUCTURER(issue_history)
        # pred is a dspy.Prediction with attribute .summary
        summ = getattr(pred, "structured_issue", "NOT FOUND")
        return str(summ).strip()
    except Exception as e:
        # Fallback behavior if the LLM call fails for any reason
        return f"[structure failed: {e}]"


def summarize_comments(github_comments: str) -> str:
    try:
        pred = COMMENT_SUMMARIZER(message=github_comments)
        # pred is a dspy.Prediction with attribute .summary
        out = getattr(pred, "summary", "NOT FOUND")
        return str(out).strip()
    except Exception as e:
        # Fallback behavior if the LLM call fails for any reason
        return f"[summarization failed: {e}]"


def _collect_pr_comments(owner: str, repo: str, num: int) -> tuple[list[str], set[str]]:
    """Collect all comments from a PR and extract links."""
    comment_links: set[str] = set()
    github_comments = []

    for c in issue_comments(owner, repo, num):
        comment_links.update(extract_links(c["body"]))
        github_comments.append(md_comment(c, "issue"))
    logger.debug("got issue comments")

    for rc in review_comments(owner, repo, num):
        comment_links.update(extract_links(rc["body"]))
        github_comments.append(md_comment(rc, "review_comment"))
    logger.debug("got review comments")

    for rv in reviews(owner, repo, num):
        comment_links.update(extract_links(rv["body"]))
        github_comments.append(md_comment(rv, "review"))
    logger.debug("got reviews")

    return github_comments, comment_links


def _process_linked_resources(comment_links: set[str], visited_links: set[str]) -> list[str]:
    """Process and summarize linked resources from comments."""
    link_summaries = []
    sub_links = [label for label in comment_links if label not in visited_links][:MAX_LINKS_TO_FOLLOW]
    if sub_links:
        link_summaries.append("\n### Links found inside comments (level 2)\n")
    for link in sub_links:
        visited_links.add(link)
        cls = classify_gh_link(link)
        if cls:
            link_summaries.append(summarize_gh_resource(cls))
        else:
            link_summaries.append(f"* <{link}>")
    logger.debug("got links found inside comments")
    return link_summaries


def build_report(
    owner: str, repo: str, num: int, patch: str, llm: bool, add_classification: bool = False
) -> tuple[str, str, str, str, str]:
    out_parts = []
    visited_links: set[str] = {""}
    logger.debug("got meta-data")

    # Collect comments and links
    github_comments, comment_links = _collect_pr_comments(owner, repo, num)

    # Summarize comments if LLM is enabled
    comment_summary = ""
    if llm:
        out_parts.append("\n### Hints\n")
        comment_summary = summarize_comments("\n\n".join(github_comments))
        out_parts.append(comment_summary)
    logger.debug("got comment summary")

    # Process linked resources
    out_parts.extend(_process_linked_resources(comment_links, visited_links))

    # Build problem statement
    issue_history = problem_statement(owner, repo, num)
    if issue_history == "NOT_A_VALID_PR":
        return "NOT_A_VALID_PR", "", "", "", ""

    problem_stat = ""
    if llm:
        out_parts.append("\n### LLM Generated summary")
        problem_stat = summarize_llm(issue_history)
        out_parts.append(problem_stat)
    else:
        out_parts.append("\n### Problem Statement\n")
        out_parts.append(issue_history)
        problem_stat = issue_history
    logger.debug("got problem statement")

    # Add classification if requested
    cat, diff = ("", "")
    if add_classification:
        out_parts.append("\n### Classification")
        cat, diff = classification(issue_history, owner, repo, patch)
        logger.debug("got classification")
        out_parts.append(cat)
        out_parts.append("\n### Difficulty")
        out_parts.append(diff)
        logger.debug("got difficulty")

    return "\n\n".join(out_parts), problem_stat, comment_summary, cat, diff


def save_markdown(report: str, filepath: str) -> None:
    """Save a Markdown string to a .md file, creating directories if needed."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


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
        owner, repo, sha = _parse_commit_url(gt_url)
        logger.debug("Patch required for full usuage")
        report, _, _, _, _ = build_report(owner, repo, int(sha), "", llm=False)
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


def classification(problem_desc: str, owner: str, repo: str, git_patch: str) -> tuple[str, str]:
    """Classify the given git commit and associated solution to one of these categories:
    - Better data structure
    - Better algorithm
    - Use a lower-level system
    - Accept a less-precise solution
    - Use parallelization
    - Remove redundancy
    - Cache and reuse
    - Improve/introduce scaling
    - Database and storage tuning
    - Micro-optimizations

    Inputs: problem description, git patch (the proposed solution)
    """
    try:
        out = CLASSIFY_JUDGE(message=problem_desc, patch=git_patch)
        cat = getattr(out, "category", "NOT FOUND")
        diff = getattr(out, "difficulty", "NOT FOUND")
        return str(cat), str(diff)
    except Exception as e:
        return f"[classification failed: {e}]", ""


def build(df: pd.DataFrame, args: argparse.Namespace) -> None:
    result_map: dict[str, dict[str, str]] = {}
    mark = args.markdown
    save_folder = args.save_location
    for i in range(len(df)):
        name = df["container_name"][i]
        owner = name.split("-", 2)[0]
        repo = name.split("-", 2)[1]
        try:
            report, prob_stat, hints, classif, diffi = build_report(
                owner, repo, int(df["base_commit"][i]), df["patch"][i], args.summarize_llm
            )
        except Exception:
            logger.debug("ERROR")
            continue
        if mark:
            save_markdown(report, f"{args.save_location}/commit_report_{i}.md")
        result_map[df["base_commit"][i]] = {
            "problem_statement": prob_stat,
            "hints": hints,
            "classification": classif,
            "difficulty": diffi,
        }
    full_path = os.path.join(save_folder, "data.json")
    with open(full_path, "w") as f:
        json.dump(result_map, f, indent=4)


def build_pr_report(link: str, summarize_llm: bool, add_classification: bool) -> str | None:
    owner, repo, num = _parse_pr_url(link)
    # logger.debug(problem_statement(owner=owner, repo=repo, num=num))
    report, prob_stat, hints, classif, diffi = build_report(
        owner=owner,
        repo=repo,
        num=int(num),
        patch="",
        llm=summarize_llm,
        add_classification=add_classification,
    )

    return report
    # if report == "NOT_A_VALID_PR":
    #     logger.debug(f"{link}: NOT_A_VALID_PR")
    #     return


# build(commits_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--df_location",
        # default="/mnt/sdd1/atharvas/formulacode/datasmith/scratch/artifacts/processed/useful_commits.csv",
        default="/mnt/sdd1/atharvas/formulacode/datasmith/scratch/artifacts/processed/downloads/useful_enriched.tbformat_2025-09-13T23:38:29.017709.parquet",
        help="location of dataframe with the commits",
    )
    (
        parser.add_argument(
            "--save_location",
            default="/mnt/sdd1/akanksha/formulacode/datasmith/src/datasmith/scrape/data",
            help="location to save instruction markdown and json with problem statement and hints",
        ),
    )
    (
        parser.add_argument(
            "--markdown",
            default=True,
            help="boolean (marks whether to create markdown file or not)",
        ),
    )
    (
        parser.add_argument(
            "--summarize_llm",
            default=True,
            help="boolean (marks whether to use llm summarization or just heuristics)",
        )
    )
    (
        parser.add_argument(
            "--link",
            default="https://github.com/astropy/astropy/pull/16088",
            help="to test individual links",
        )
    )
    args = parser.parse_args()

    # Get just the issue number

    MAX_LINKS_TO_FOLLOW = 60
    ISSUE_STRUCTURER = LLMStructurer()
    COMMENT_SUMMARIZER = LLMCommentSummarizer()
    CLASSIFY_JUDGE = ClassifyJudge()

    # _______________________________________________________________________________________

    # # Dataframe with git commits to generate reports for

    # # meta = pq.read_metadata(Path(args.df_location))     # if this fails, the file/ footer is broken
    # # logger.debug(meta.num_row_groups, meta.schema)
    # path = Path(args.df_location)
    # from pathlib import Path
    # p = Path(args.df_location)
    # pf = pq.ParquetFile(p)
    # logger.debug(pf)                # nice summary (row groups, columns, etc.)
    # logger.debug(pf.schema)         # full Parquet schema
    # logger.debug(pf.metadata)       # detailed file metadata
    # logger.debug(pf.num_row_groups) # number of row groups

    # # Try reading in chunks
    # parquet_file = pq.ParquetFile(p)
    # columns = parquet_file.schema.names

    # try:
    #     pf = fastparquet.ParquetFile(p)
    #     logger.debug("FastParquet can open file")
    #     logger.debug("Columns:", pf.columns)
    #     logger.debug("Schema:", pf.schema)

    #     # Try to read with fastparquet directly
    #     df = pf.to_pandas()
    #     logger.debug("FastParquet read successful!")
    # except Exception as e:
    #     logger.debug(f"FastParquet also failed: {e}")

    # logger.debug(df.columns)
    # logger.debug(len(df))
    # small_df = df.head()
    # logger.debug(small_df)
    # build(df, args)

    # _______________________________________________________________________________________
    report = build_pr_report(args.link, args.summarize_llm, add_classification=False)
    if report and "NOT_A_VALID_PR" not in report and Path(args.save_location).exists():
        save_markdown(report, f"{args.save_location}/pr_report_{args.link}.md")
