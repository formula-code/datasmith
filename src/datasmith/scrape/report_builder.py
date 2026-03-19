"""ReportBuilder class for generating PR reports with configurable options."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.problem_extractor import ProblemExtraction, ProblemExtractor
from datasmith.agents.summ_judge import ClassificationDecision, ClassifyJudge, PerfClassifier
from datasmith.logging_config import configure_logging
from datasmith.scrape.issue_extractor import extract_issues_from_description
from datasmith.scrape.models import HintComment, HintsContext, IssueExpanded, ReportResult
from datasmith.scrape.report_utils import (
    anonymize_github_issue,
    extract_links,
    iso,
    issue_timeline,
    to_datetime,
)
from datasmith.scrape.utils import _parse_pr_url

logger = configure_logging()


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

    problem_extractor: ProblemExtractor | None
    classify_judge: ClassifyJudge | None
    perf_classifier: PerfClassifier | None

    def __init__(
        self,
        enable_llm_backends: bool = False,
        summarize_llm: bool = False,
        add_classification: bool = False,
        filter_performance_only: bool = False,
        include_bot_comments: bool = False,
        summarize_prs: bool = False,
        anonymize_output: bool = False,
        max_links_to_follow: int = 60,
        model_name: str = "local/meta-llama/Llama-3.3-70B-Instruct",
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
        self.summarize_prs = summarize_prs
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

        if self.enable_llm_backends:
            configure_agent_backends(PORTKEY_MODEL_NAME=model_name.replace("local/", ""), local="local" in model_name)
            # Disable LCS/verbatim validation to simplify extraction and reduce noise
            self.problem_extractor = ProblemExtractor()
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
            autoescape=False,  # noqa: S701
            trim_blocks=True,
        )

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
            timestamp=iso(ts_iso),
            body=excerpt,
            links_str=", ".join(extract_links(body)) or "—",
        )

    def _collect_pr_discussions(
        self, owner: str, repo: str, num: int, pr_dict: dict
    ) -> tuple[list[HintComment], set[IssueExpanded]]:
        """Collect structured discussion items for the Hints section.

        Returns a tuple of (items, links) where items are `HintComment`s and
        links is the set of URLs mentioned across comments.
        """
        comment_links: set[IssueExpanded] = set()
        items: list[HintComment] = []

        def _mk_item(obj: dict, kind: str) -> HintComment:
            body = (obj.get("body") or "").strip().replace("\r\n", "\n")
            ts_field = "submitted_at" if kind == "reviewed" else "created_at"
            ts_iso = obj.get(ts_field, "") or ""
            linked_issues = extract_issues_from_description(
                body,
                owner,
                repo,
                pr_created_at=pr_dict.get("pr_created_at", "") or None,
                pr_number_to_ignore=num,
            )
            return HintComment(
                user_login=obj["user"]["login"],
                timestamp=iso(ts_iso) if ts_iso else "",
                created_at=ts_iso,
                body=body,
                links=linked_issues,
                kind=kind,
            )

        timeline = issue_timeline(owner, repo, num)
        pr_merged_at = to_datetime(pr_dict["pr_merged_at"])

        for event in timeline:
            # Prefer created_at for cutoff; fall back to updated_at conservatively
            event_time = event.get("created_at") or event.get("updated_at")
            if (not event_time) or to_datetime(event_time) >= pr_merged_at:
                continue
            # Keep only events that look like user-authored comments with bodies
            if not (event.get("body") and isinstance(event.get("user"), dict) and event["user"].get("login")):
                continue
            item = _mk_item(event, event.get("event", "pr_comment"))
            comment_links.update(item.links)
            items.append(item)

        return items, comment_links

    def _extract_problem_statement(
        self,
        pr_body: str,
        pr_comments: str,
        pr_title: str,
        # pr_patch: str | None = None
    ) -> ProblemExtraction:
        """Extract (not summarize) problem statement from issue.

        Uses an extractive approach to keep high overlap (typically 60%+ LCS)
        with source material. Preserves original wording, code examples, and
        technical details verbatim where possible.
        Preserves original wording, code examples, and technical details verbatim.

        Args:
            pr_body: Main issue description
            pr_comments: Related issue statistics/content
            pr_title: The GitHub PR title
            # pr_patch: The GitHub PR code patch/diff

        Returns:
            A structured ``ProblemExtraction`` instance.
        """
        if self.problem_extractor is None or (not self.summarize_llm):
            return ProblemExtraction(
                initial_observations="",
                triage_attempts=None,
                solution_overview=pr_body + "\n\n" + pr_comments + "\n\n",
                solution_observations=None,
            )

        try:
            extraction = self.problem_extractor.extract_problem(
                pr_title=pr_title,
                pr_body=pr_body,
                pr_comments=pr_comments,
                # pr_patch=pr_patch,
            )

        except Exception as e:
            logger.error(f"Problem statement extraction failed: {e}", exc_info=True)
            return ProblemExtraction(
                initial_observations="",
                triage_attempts=None,
                solution_overview=pr_body + "\n\n" + pr_comments + "\n\n",
                solution_observations=None,
            )
        else:
            return extraction

    def _classify_performance(self, issue_history: str, patch: str) -> ClassificationDecision | None:
        """Classify commit for performance optimization details.

        Args:
            issue_history: Issue description
            patch: Git patch content

        Returns:
            ``ClassificationDecision`` with structured outputs, or ``None`` when unavailable.
        """
        if self.classify_judge is None:
            return None

        try:
            return self.classify_judge(message=issue_history, patch=patch)  # type: ignore[no-any-return]  # pyright: ignore[reportReturnType]
        except Exception as e:
            logger.error(f"Classification failed: {e}", exc_info=True)
            return ClassificationDecision(
                reason=f"classification failed: {e}",
                category="",
                difficulty="",
                confidence=None,
            )

    def _evaluate_performance_detection(
        self, judge_problem_description: str, file_change_summary: str, patch: str
    ) -> tuple[bool, str]:
        """Return performance detection outputs when enabled."""
        if not (self.filter_performance_only or self.add_classification):
            return False, ""

        if self.perf_classifier is None:
            return False, ""

        return self.perf_classifier.get_response(
            message=judge_problem_description,
            file_change_summary=file_change_summary,
            git_patch=patch,
        )

    def build(self, pr_dict: dict[str, Any]) -> ReportResult:  # noqa: C901
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
        # pr_url may occasionally arrive as a non-string (e.g., numpy/object), avoid truthiness on arrays
        pr_url_obj = pr_dict.get("pr_url")
        pr_url = pr_url_obj if isinstance(pr_url_obj, str) else ("" if pr_url_obj is None else str(pr_url_obj))
        if pr_url.strip() == "":
            raise ValueError("pr_dict must contain 'pr_url' field")

        # Parse owner/repo/number from URL
        owner, repo, pr_number_str = _parse_pr_url(pr_url)
        pr_number = int(pr_number_str)
        patch = pr_dict.get("patch", "")
        pr_body = pr_dict.get("pr_body", "")
        pr_title = pr_dict.get("pr_title", "")
        pr_created_at: str = pr_dict.get("pr_created_at", "")
        file_change_summary = pr_dict.get("file_change_summary", "")

        repo_template = self.jinja_env.get_template("repo.md.j2")
        pr_base = pr_dict.get("pr_base")
        if not isinstance(pr_base, dict):
            pr_base = {}
        _repo = pr_base.get("repo", {}) if isinstance(pr_base.get("repo"), dict) else {}
        # Normalize repo topics to a comma-separated string safely
        _topics_val = _repo.get("topics", [])
        _topics_list = [] if not isinstance(_topics_val, (list, tuple, set)) else list(_topics_val)
        _topics_str = ", ".join([str(t) for t in _topics_list]) if _topics_list else ""
        repo_description_rendered = repo_template.render(
            repo_name=f"{owner}/{repo}",
            repo_description=_repo.get("description", ""),
            repo_topics=_topics_str,
            repo_language=_repo.get("language", ""),
        )

        pr_template = self.jinja_env.get_template("pr_header.md.j2")
        labels_raw = pr_dict.get("pr_labels", [])
        if isinstance(labels_raw, dict):
            labels_seq = [labels_raw]
        elif isinstance(labels_raw, (list, tuple, set)):
            labels_seq = list(labels_raw)
        else:
            labels_seq = []
        labels_joined = ", ".join([str(label.get("name", "")) for label in labels_seq if isinstance(label, dict)])
        labels_joined = labels_joined if labels_joined else "—"
        pr_body_text = pr_template.render(
            title=pr_title,
            number=pr_number,
            repo_full_name=f"{owner}/{repo}",
            labels=labels_joined,
            body=pr_body,
        )

        logger.debug("Building report for %s/%s PR #%d", owner, repo, pr_number)
        all_issues: set[IssueExpanded] = set()
        if issue_data := extract_issues_from_description(
            pr_body_text, owner, repo, pr_created_at=pr_created_at or None, pr_number_to_ignore=pr_number
        ):
            all_issues.update(issue_data)
        hint_items, comment_links = self._collect_pr_discussions(owner, repo, pr_number, pr_dict)
        all_issues.update(comment_links)
        pr_raw_comments_text = "\n\n".join(item.body for item in hint_items)
        hints_ctx = HintsContext(items=hint_items, summary=pr_raw_comments_text, prefer_summary=self.summarize_llm)

        raw_pr_text = pr_body_text + "\n\nComments:\n" + pr_raw_comments_text
        # Build a stable, sorted list of referenced issues
        issues_sorted: list[IssueExpanded] = sorted(all_issues, key=lambda it: it.number)
        issues_template = self.jinja_env.get_template("issues.md.j2")
        issues_rendered = issues_template.render(issues=issues_sorted)
        if self.anonymize_output:
            issues_rendered = anonymize_github_issue(issues_rendered)

        # Reject PRs with no referenced issues and no body content
        if (not issues_sorted) or (not (pr_body or "").strip()):
            return ReportResult(
                final_md="NOT_A_VALID_PR",
                all_data={
                    "issues_expanded": issues_sorted,
                    "raw_issue_data": [],
                    "git_issue_str": "",
                },
                problem_statement="",
                hints="",
                classification="",
                difficulty="",
                is_performance_commit=False,
            )

        is_performance_commit, perf_json = self._evaluate_performance_detection(raw_pr_text, file_change_summary, patch)
        # If filtering to performance-only, optionally short-circuit but preserve metadata
        if self.filter_performance_only and not is_performance_commit:
            logger.debug("NOT A PERFORMANCE COMMIT (filtered out by filter_performance_only=True)")
            return ReportResult(
                final_md="NOT_A_PERFORMANCE_COMMIT",
                all_data={
                    "performance_section": perf_json,
                    "issues_expanded": issues_sorted,
                    "raw_issue_data": [
                        {
                            "number": it.number,
                            "title": it.title,
                            "url": it.url,
                            "description": it.description,
                            "comments": list(it.comments),
                            "cross_references": list(it.cross_references),
                            "created_at": it.created_at,
                            "closed_at": it.closed_at,
                        }
                        for it in issues_sorted
                    ],
                    "git_issue_str": issues_rendered,
                },
                problem_statement="",
                hints="",
                classification="",
                difficulty="",
                is_performance_commit=False,
            )
        bisected_pr_information = self._extract_problem_statement(
            pr_body=pr_body_text,
            pr_comments=pr_raw_comments_text,
            pr_title=pr_title,
            # pr_patch="",
        )

        # if bisected_pr_information.initial_observations is empty or issues_rendered is empty,
        # treat it as NOT_A_VALID_PR
        if not (bisected_pr_information.initial_observations or "").strip() or not issues_rendered.strip():
            logger.debug("NOT A VALID PR (no problem statement or no linked issues)")
            return ReportResult(
                final_md="NOT_A_VALID_PR",
                all_data={
                    "issues_expanded": issues_sorted,
                    "raw_issue_data": [
                        {
                            "number": it.number,
                            "title": it.title,
                            "url": it.url,
                            "description": it.description,
                            "comments": list(it.comments),
                            "cross_references": list(it.cross_references),
                            "created_at": it.created_at,
                            "closed_at": it.closed_at,
                        }
                        for it in issues_sorted
                    ],
                    "git_issue_str": issues_rendered,
                },
                problem_statement="",
                hints="",
                classification="",
                difficulty="",
                is_performance_commit=False,
            )

        # if there are no linked issues, but we have a problem statement,
        # we should set problem statement to the hints block,
        # and not give any other hints.
        hints_block = ""
        if not issues_sorted and (bisected_pr_information.initial_observations or "").strip():
            issues_rendered = ""

        # now make a structured final.md.j2
        final_template = self.jinja_env.get_template("final.md.j2")
        final_template_rendered = final_template.render(
            repo_description=repo_description_rendered,
            initial_observations=bisected_pr_information.initial_observations,
            issues=issues_rendered,
        )
        final_template_no_hints_rendered = final_template.render(
            repo_description=repo_description_rendered,
            initial_observations=bisected_pr_information.initial_observations,
            issues="",
        )
        if self.anonymize_output:
            final_template_rendered = anonymize_github_issue(final_template_rendered)
            final_template_no_hints_rendered = anonymize_github_issue(final_template_no_hints_rendered)

        # Add classification only if this is a performance commit
        classification_decision = None
        if self.add_classification and is_performance_commit:
            # classification_decision = self._maybe_classify(raw_pr_text, owner, repo, pr_number, patch)
            classification_decision = self._classify_performance(raw_pr_text, patch)

        classification = ""
        difficulty = ""
        classification_reason = ""
        classification_confidence: int | None = None

        if self.add_classification and classification_decision:
            classification = classification_decision.category
            difficulty = classification_decision.difficulty
            classification_reason = classification_decision.reason
            classification_confidence = classification_decision.confidence

        # Assemble final results map with both the composed report and section pieces
        final_results: dict[str, str] = {
            "final_report": final_template_rendered,
            "repo_description": repo_description_rendered,
            "pr_header": pr_body_text,
            "issues": issues_rendered,
            "hints": hints_block,
        }

        # Construct ReportResult with both human-readable strings and structured fields
        return ReportResult(
            final_md=final_template_rendered,
            final_md_no_hints=final_template_no_hints_rendered,
            pr_created_at=pr_created_at,
            pr_merged_at=pr_dict.get("pr_merged_at"),
            final_with_sol="",
            all_data={
                # Structured + raw context for downstream consumers
                "hints_context": hints_ctx,
                "issues_expanded": issues_sorted,
                "performance_section": perf_json if is_performance_commit else "",
                "raw_comments": pr_raw_comments_text,
                "raw_pr_title": pr_title,
                "raw_pr_body": pr_body,
                "raw_patch": patch,
                "raw_file_change_summary": file_change_summary,
                "classification": classification,
                "difficulty": difficulty,
                "classification_reason": classification_reason,
                "classification_confidence": classification_confidence,
                # Back-compat fields expected by tests
                "raw_issue_data": [
                    {
                        "number": it.number,
                        "title": it.title,
                        "url": it.url,
                        "description": it.description,
                        "comments": list(it.comments),
                        "cross_references": list(it.cross_references),
                        "created_at": it.created_at,
                        "closed_at": it.closed_at,
                    }
                    for it in issues_sorted
                ],
                "git_issue_str": issues_rendered,
            },
            problem_statement=bisected_pr_information.initial_observations or "",
            problem_statement_with_sol=bisected_pr_information.to_problem_with_solution_markdown()
            if bisected_pr_information
            else "",
            raw_problem_statement=raw_pr_text,
            hints=hints_block,
            classification=classification,
            difficulty=difficulty,
            is_performance_commit=is_performance_commit,
            classification_reason=classification_reason,
            classification_confidence=classification_confidence,
            problem_sections=bisected_pr_information,
            final_results=final_results,
        )
