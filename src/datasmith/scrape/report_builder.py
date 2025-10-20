"""ReportBuilder class for generating PR reports with configurable options."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.agents.summ_judge import ClassifyJudge
from datasmith.agents.problem_extractor import ProblemExtractor
from datasmith.logging_config import configure_logging
from datasmith.scrape.build_pr_report import (
    anonymize_github_issue,
    classify_gh_link,
    issue_comments,
    review_comments,
    reviews,
    summarize_gh_resource,
)
from datasmith.scrape.issue_extractor import extract_issues_from_description
from datasmith.scrape.models import ReportResult
from datasmith.scrape.utils import _parse_pr_url
from datasmith.utils import _get_github_metadata

logger = configure_logging()

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


class ReportBuilder:
    """Builder for generating structured PR reports.

    This class provides a configurable interface for generating reports from
    GitHub pull requests, with optional LLM-based summarization and classification.

    Attributes:
        enable_llm_backends: Whether to initialize LLM agent backends
        summarize_llm: Whether to use LLM for summarization
        add_classification: Whether to add performance classification
        filter_performance_only: Whether to only generate reports for performance commits
        include_bot_comments: Whether to include bot comments in hints
        anonymize_output: Whether to anonymize GitHub URLs, usernames, etc.
        max_links_to_follow: Maximum number of level-2 links to traverse
        model_name: LLM model name to use for agent backends
    """

    def __init__(
        self,
        enable_llm_backends: bool = False,
        summarize_llm: bool = False,
        add_classification: bool = False,
        filter_performance_only: bool = False,
        include_bot_comments: bool = False,
        anonymize_output: bool = True,
        max_links_to_follow: int = 60,
        model_name: str = "@togetherai/meta-llama/Llama-3.3-70B-Instruct-Turbo",
    ):
        """Initialize the ReportBuilder with configuration.

        Args:
            enable_llm_backends: Whether to configure LLM agent backends
            summarize_llm: Whether to use LLM for comment/issue summarization
            add_classification: Whether to add performance classification
            filter_performance_only: Whether to only generate reports for performance commits
                (requires enable_llm_backends=True to perform performance detection)
            include_bot_comments: Whether to include bot comments (coveralls, codecov, etc.)
                in the hints section (default: False, filters them out)
            anonymize_output: Whether to anonymize GitHub URLs, usernames, issue numbers, etc.
                Set to False for better human readability (default: True for privacy)
            max_links_to_follow: Safety cap for level-2 link traversal
            model_name: LLM model name for agent backends
        """
        self.enable_llm_backends = enable_llm_backends
        self.summarize_llm = summarize_llm
        self.add_classification = add_classification
        self.filter_performance_only = filter_performance_only
        self.include_bot_comments = include_bot_comments
        self.anonymize_output = anonymize_output
        self.max_links_to_follow = max_links_to_follow
        self.model_name = model_name

        # Validate configuration
        if self.summarize_llm and not self.enable_llm_backends:
            raise ValueError(
                "summarize_llm=True requires enable_llm_backends=True. "
                "LLM backends must be initialized to use LLM-based summarization."
            )
        if self.add_classification and not self.enable_llm_backends:
            raise ValueError(
                "add_classification=True requires enable_llm_backends=True. "
                "LLM backends must be initialized to use performance classification."
            )
        if self.filter_performance_only and not self.enable_llm_backends:
            raise ValueError(
                "filter_performance_only=True requires enable_llm_backends=True. "
                "LLM backends must be initialized to perform performance detection."
            )

        # Initialize LLM agents if enabled
        self.problem_extractor: ProblemExtractor | None
        self.classify_judge: ClassifyJudge | None
        self.perf_classifier: PerfClassifier | None

        if self.enable_llm_backends:
            configure_agent_backends(PORTKEY_MODEL_NAME=model_name)
            self.problem_extractor = ProblemExtractor(
                validate_lcs=True,  # Enable LCS validation
                min_lcs=0.85,  # 85% verbatim threshold
                log_validation=True,  # Log validation metrics
            )
            self.classify_judge = ClassifyJudge()
            self.perf_classifier = PerfClassifier()
        else:
            self.problem_extractor = None
            self.classify_judge = None
            self.perf_classifier = None

        # Set up Jinja2 template environment
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=False,  #
            trim_blocks=True,
        )

    def _iso_format(self, ts: str) -> str:
        """Convert ISO timestamp to readable format.

        Args:
            ts: ISO format timestamp string

        Returns:
            Formatted timestamp string (HH:MM DD/MM/YYYY)
        """
        dt = datetime.fromisoformat(ts.rstrip("Z")).replace(tzinfo=timezone.utc)
        return dt.strftime("%H:%M %d/%m/%Y")

    def _extract_links(self, text: str) -> list[str]:
        """Extract HTTP(S) URLs from text.

        Args:
            text: Text to extract links from

        Returns:
            List of URLs found in text
        """
        return re.findall(r"https?://[^\s)<>\]]+", text or "")

    def _format_comment(self, item: dict, kind: str) -> str:
        """Format a comment using the Jinja2 template.

        Args:
            item: Comment data dict
            kind: Type of comment ('issue', 'review_comment', 'review')

        Returns:
            Formatted markdown string
        """
        body = item.get("body") or ""
        excerpt = body.strip().replace("\r\n", "\n")
        ts_field = "submitted_at" if kind == "review" else "created_at"
        ts_iso = item[ts_field]

        template = self.jinja_env.get_template("comment.md.j2")
        return template.render(
            user_login=item["user"]["login"],
            timestamp=self._iso_format(ts_iso),
            body=excerpt,
            links_str=", ".join(self._extract_links(body)) or "—",
        )

    def _is_bot_comment(self, comment: dict) -> bool:
        """Check if a comment is from a bot.

        Args:
            comment: Comment dict with user information

        Returns:
            True if comment is from a known bot
        """
        if not comment.get("user"):
            return False
        username = comment["user"].get("login", "").lower()
        return username in BOT_USERNAMES or username in {u.lower() for u in BOT_USERNAMES}

    def _collect_pr_comments(self, owner: str, repo: str, num: int) -> tuple[list[str], set[str]]:
        """Collect all comments from a PR and extract links.

        Args:
            owner: Repository owner
            repo: Repository name
            num: PR number

        Returns:
            Tuple of (formatted_comments, comment_links)
        """
        comment_links: set[str] = set()
        github_comments = []

        for c in issue_comments(owner, repo, num):
            # Filter bots unless explicitly included
            if not self.include_bot_comments and self._is_bot_comment(c):
                logger.debug(f"Filtered out bot comment from {c['user']['login']}")
                continue

            comment_links.update(self._extract_links(c["body"]))
            github_comments.append(self._format_comment(c, "issue"))
        logger.debug("got issue comments")

        for rc in review_comments(owner, repo, num):
            # Filter bots unless explicitly included
            if not self.include_bot_comments and self._is_bot_comment(rc):
                logger.debug(f"Filtered out bot review comment from {rc['user']['login']}")
                continue

            comment_links.update(self._extract_links(rc["body"]))
            github_comments.append(self._format_comment(rc, "review_comment"))
        logger.debug("got review comments")

        for rv in reviews(owner, repo, num):
            # Filter bots unless explicitly included
            if not self.include_bot_comments and self._is_bot_comment(rv):
                logger.debug(f"Filtered out bot review from {rv['user']['login']}")
                continue

            comment_links.update(self._extract_links(rv["body"]))
            github_comments.append(self._format_comment(rv, "review"))
        logger.debug("got reviews")

        return github_comments, comment_links

    def _process_linked_resources(self, comment_links: set[str], visited_links: set[str]) -> list[str]:
        """Process and summarize linked resources from comments.

        Args:
            comment_links: Set of links found in comments
            visited_links: Set of already visited links (updated in-place)

        Returns:
            List of formatted link summaries
        """
        link_summaries = []
        sub_links = [label for label in comment_links if label not in visited_links][: self.max_links_to_follow]

        for link in sub_links:
            visited_links.add(link)
            cls = classify_gh_link(link)
            if cls:
                link_summaries.append(summarize_gh_resource(cls))
            else:
                link_summaries.append(f"* <{link}>")
        logger.debug("got links found inside comments")

        return link_summaries

    def _extract_discussion(self, github_comments: str) -> str:
        """Extract (not summarize) key discussion points from comments.

        Uses extractive approach to maintain 85%+ LCS ratio with source material.
        Preserves technical details, code snippets, and disagreements verbatim.

        Args:
            github_comments: Concatenated comment text

        Returns:
            Extracted discussion or raw comments if extraction disabled
        """
        if not self.summarize_llm:  # Keep flag name for backward compatibility
            return github_comments

        if self.problem_extractor is None:
            return github_comments

        try:
            extracted, validation_report = self.problem_extractor.extract_comments(
                comment_thread=github_comments
            )

            # Log validation metrics
            if validation_report.get("validation_enabled"):
                avg_lcs = validation_report.get("avg_lcs", 0)
                avg_ngram = validation_report.get("avg_ngram", 0)
                logger.info(
                    f"Comment extraction validation: LCS={avg_lcs:.2%}, n-gram={avg_ngram:.2%}"
                )

                # Warn if quality is low
                if not validation_report.get("overall_pass"):
                    logger.warning(
                        f"Comment extraction has low extractiveness:\n"
                        f"{validation_report.get('details', 'No details available')}"
                    )

            return extracted.strip()
        except Exception as e:
            logger.error(f"Comment extraction failed: {e}", exc_info=True)
            return f"[extraction failed: {e}]"

    def _extract_problem_statement(self, issue_history: str, issue_stat: str) -> str:
        """Extract (not summarize) problem statement from issue.

        Uses extractive approach to maintain 85%+ LCS ratio with source material.
        Preserves original wording, code examples, and technical details verbatim.

        Args:
            issue_history: Main issue description
            issue_stat: Related issue statistics/content

        Returns:
            Extracted problem statement
        """
        if self.problem_extractor is None:
            return issue_history

        try:
            extracted, validation_report = self.problem_extractor.extract_problem(
                message=issue_history,
                related_issues=issue_stat
            )

            # Log validation metrics
            if validation_report.get("validation_enabled"):
                avg_lcs = validation_report.get("avg_lcs", 0)
                avg_ngram = validation_report.get("avg_ngram", 0)
                logger.info(
                    f"Problem statement extraction validation: LCS={avg_lcs:.2%}, n-gram={avg_ngram:.2%}"
                )

                # Warn if quality is low
                if not validation_report.get("overall_pass"):
                    logger.warning(
                        f"Problem statement has low extractiveness:\n"
                        f"{validation_report.get('details', 'No details available')}"
                    )

            return extracted.strip()
        except Exception as e:
            logger.error(f"Problem statement extraction failed: {e}", exc_info=True)
            return f"[extraction failed: {e}]"

    def _get_pr_change_summary(self, owner: str, repo: str, pr_number: int) -> str:
        """Get file change summary for a PR.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            Markdown table of file changes
        """
        api = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files"
        headers = {"Accept": "application/vnd.github+json"}
        lines = [
            "| File | Status | Lines Added | Lines Removed | Total Changes |",
            "|------|--------|-------------|---------------|---------------|",
        ]
        total_add = total_del = total_changes = 0
        page = 1

        while True:
            resp = requests.get(api, headers=headers, params={"per_page": 100, "page": page}, timeout=30)
            resp.raise_for_status()
            files = resp.json()
            if not files:
                break

            for f in files:
                status = f.get("status", "")
                filename = f.get("filename", "")
                if status == "renamed" and f.get("previous_filename"):
                    filename = f"{f['previous_filename']} ➜ {f['filename']}"

                added = f.get("additions", 0)
                deleted = f.get("deletions", 0)
                changes = f.get("changes", added + deleted)

                lines.append(f"| {filename} | {status} | {added} | {deleted} | {changes} |")

                total_add += added
                total_del += deleted
                total_changes += changes

            if 'rel="next"' not in resp.headers.get("Link", ""):
                break
            page += 1

        lines.append(f"| **TOTAL** |  | **{total_add}** | **{total_del}** | **{total_changes}** |")
        return "\n".join(lines)

    def _classify_performance(
        self, issue_history: str, owner: str, repo: str, pr_number: int, patch: str
    ) -> tuple[str, str]:
        """Classify commit for performance optimization category and difficulty.

        Args:
            issue_history: Issue description
            owner: Repository owner
            repo: Repository name
            pr_number: PR number
            patch: Git patch content

        Returns:
            Tuple of (category, difficulty)
        """
        if self.classify_judge is None:
            return "[classification unavailable: judge not configured]", ""

        try:
            out = self.classify_judge(message=issue_history, patch=patch)
            cat = getattr(out, "category", "NOT FOUND")
            diff = getattr(out, "difficulty", "NOT FOUND")
            return str(cat), str(diff)
        except Exception as e:
            return f"[classification failed: {e}]", ""

    def _resolve_pr_body(self, pr_dict: dict[str, Any], owner: str, repo: str, pr_number: int) -> str:
        """Resolve the PR body from the provided dict or GitHub metadata."""
        pr_body = pr_dict.get("pr_body")
        if pr_body is not None:
            return str(pr_body)

        pr_meta = _get_github_metadata(endpoint=f"/repos/{owner}/{repo}/pulls/{pr_number}")
        if isinstance(pr_meta, dict):
            return str(pr_meta.get("body") or "")
        return ""

    def _expand_issue_details(self, issue_data: list[dict[str, Any]]) -> str:
        """Combine issue metadata into a markdown-friendly string."""
        if not issue_data:
            return ""

        issue_texts: list[str] = []
        for index, issue in enumerate(issue_data):
            segments: list[str] = [f"Issue {index}: {issue.get('title', '')}\n"]

            body = issue.get("body")
            if body:
                segments.append(f"Description:\n{body}\n")

            comments = [comment for comment in issue.get("comments", []) if comment]
            if comments:
                comment_lines = "\n".join(f"- {comment}" for comment in comments)
                segments.append(f"Comments ({len(comments)}):\n{comment_lines}\n")

            cross_refs = [xref for xref in issue.get("cross_references", []) if xref]
            if cross_refs:
                cross_lines = "\n".join(f"- {xref}" for xref in cross_refs)
                segments.append(f"Cross-references ({len(cross_refs)}):\n{cross_lines}\n")

            issue_texts.append("".join(segments))

        combined = " ".join(issue_texts)
        return anonymize_github_issue(combined) if self.anonymize_output else combined

    def _build_issue_context(self, pr_body: str, owner: str, repo: str) -> tuple[str, str, list[dict[str, Any]]]:
        """Construct the problem and issue strings alongside raw issue metadata."""
        prob_stat_list, issue_data = extract_issues_from_description(pr_body, owner, repo)
        git_problem_str = " ".join(str(item) for item in prob_stat_list)
        if self.anonymize_output:
            git_problem_str = anonymize_github_issue(git_problem_str)

        git_issue_str = self._expand_issue_details(issue_data)
        return git_problem_str, git_issue_str, issue_data

    def _compose_problem_statement(self, git_problem_str: str, git_issue_str: str) -> tuple[str, str]:
        """Return the problem section title and statement text."""
        if self.summarize_llm:
            extracted = self._extract_problem_statement(git_problem_str, git_issue_str)
            return "Problem Statement", extracted  # Changed from "LLM Generated summary"

        if git_issue_str:
            statement = f"{git_problem_str}\n\n## Referenced Issues\n\n{git_issue_str}"
            return "Problem Statement", statement

        return "Problem Statement", git_problem_str

    def _evaluate_performance_detection(
        self, git_problem_str: str, owner: str, repo: str, pr_number: int, patch: str
    ) -> tuple[bool, str]:
        """Return performance detection outputs when enabled."""
        if not (self.filter_performance_only or self.add_classification):
            return False, ""

        if self.perf_classifier is None:
            return False, ""

        file_change = self._get_pr_change_summary(owner, repo, pr_number)
        return self.perf_classifier.get_response(
            message=git_problem_str,
            file_change_summary=file_change,
            git_patch=patch,
        )

    def _maybe_classify(
        self, git_problem_str: str, owner: str, repo: str, pr_number: int, patch: str
    ) -> tuple[str, str]:
        """Return classification outputs when enabled."""
        if not self.add_classification:
            return "", ""
        return self._classify_performance(git_problem_str, owner, repo, pr_number, patch)

    def build(self, pr_dict: dict[str, Any]) -> ReportResult:
        """Build a report from a PR dictionary.

        Args:
            pr_dict: Dictionary containing PR data with keys like:
                - pr_url: GitHub API URL for the PR
                - pr_body: PR description
                - patch: Git patch content
                - pr_number: PR number
                - pr_merged_at: Merge timestamp
                And other PR metadata fields

        Returns:
            ReportResult containing the generated report and metadata

        Raises:
            ValueError: If required fields are missing from pr_dict
        """
        # Extract basic info from pr_dict, fallback to API if needed
        pr_url = pr_dict.get("pr_url")
        if not pr_url:
            raise ValueError("pr_dict must contain 'pr_url' field")

        # Parse owner/repo/number from URL
        owner, repo, pr_number_str = _parse_pr_url(pr_url)
        pr_number = int(pr_number_str)

        # Get patch from dict or empty string
        patch = pr_dict.get("patch", "")

        # Get PR body from dict, or fetch from API
        pr_body = self._resolve_pr_body(pr_dict, owner, repo, pr_number)

        logger.debug("Building report for %s/%s PR #%d", owner, repo, pr_number)

        # Collect comments and links
        github_comments, comment_links = self._collect_pr_comments(owner, repo, pr_number)

        # Extract discussion from comments
        visited_links: set[str] = {""}
        comment_summary = self._extract_discussion("\n\n".join(github_comments))
        logger.debug("got comment extraction")

        # Process linked resources
        link_summaries = self._process_linked_resources(comment_links, visited_links)

        # Extract and process referenced issues
        git_problem_str, git_issue_str, issue_data = self._build_issue_context(pr_body, owner, repo)

        if not issue_data and not pr_body:
            return ReportResult(
                report_md="NOT_A_VALID_PR",
                report_data={},
                problem_statement="",
                hints="",
                classification="",
                difficulty="",
                is_performance_commit=False,
            )

        # Check if performance commit (if filtering or classification enabled)
        is_performance_commit, perf_json = self._evaluate_performance_detection(
            git_problem_str, owner, repo, pr_number, patch
        )
        if self.filter_performance_only and not is_performance_commit:
            logger.debug("NOT A PERFORMANCE COMMIT (filtered out by filter_performance_only=True)")
            return ReportResult(
                report_md="NOT_A_PERFORMANCE_COMMIT",
                report_data={},
                problem_statement="",
                hints="",
                classification="",
                difficulty="",
                is_performance_commit=False,
            )

        # Generate problem statement
        problem_section_title, problem_stat = self._compose_problem_statement(git_problem_str, git_issue_str)

        logger.debug("got problem statement")

        # Add classification if requested
        cat, diff = self._maybe_classify(git_problem_str, owner, repo, pr_number, patch)
        if self.add_classification:
            logger.debug("got classification")

        # Build final report using template
        report_data = {
            "hints": comment_summary,
            "links_section": "\n".join(link_summaries) if link_summaries else "",
            "performance_section": perf_json if is_performance_commit else "",
            "problem_section_title": problem_section_title,
            "problem_statement": problem_stat,
            "classification": cat if self.add_classification else "",
            "difficulty": diff if self.add_classification else "",
        }
        template = self.jinja_env.get_template("report.md.j2")
        report_md = template.render(**report_data)

        return ReportResult(
            report_md=report_md,
            report_data=report_data,
            problem_statement=problem_stat,
            hints=comment_summary,
            classification=cat,
            difficulty=diff,
            is_performance_commit=is_performance_commit,
        )
