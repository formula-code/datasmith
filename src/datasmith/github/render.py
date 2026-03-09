"""Problem statement rendering with Jinja2 templates."""

from __future__ import annotations

import os
import re

from jinja2 import Environment, FileSystemLoader

from datasmith.github.models import PR, IssueExpanded
from datasmith.utils import get_logger

logger = get_logger("github.render")

_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def _get_env() -> Environment:
    """Return a Jinja2 environment pointing at the templates directory."""
    return Environment(
        loader=FileSystemLoader(_TEMPLATES_DIR),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )


class Anonymizer:
    """Replace usernames, emails, and other PII with deterministic placeholders."""

    _SIGNOFF = re.compile(
        r"((?:Signed-off-by|Co-authored-by)\s*:\s*)(.*?)(\s*<[^>]+>)",
        re.IGNORECASE,
    )
    _EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
    _MENTION = re.compile(r"@([\w-]+)")
    _GITHUB_URL = re.compile(r"(https?://github\.com/)([\w.-]+)")
    _USER_IMAGES = re.compile(r"https?://user-images\.githubusercontent\.com/[^\s)>\]]+")
    _HOME_PATH = re.compile(r"(/(?:home|Users)/|[Cc]:\\Users\\)([\w.-]+)")
    _IP_ADDR = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b")

    def __init__(self, known_usernames: set[str] | None = None) -> None:
        self._map: dict[str, str] = {}
        self._counter = 0
        self._known_usernames = known_usernames or set()

    def _placeholder(self, username: str) -> str:
        """Return a deterministic placeholder for *username* (case-insensitive)."""
        key = username.lower()
        if key not in self._map:
            self._counter += 1
            self._map[key] = f"user_{self._counter}"
        return self._map[key]

    def anonymize(self, text: str) -> str:
        """Strip PII and replace identifiers with deterministic placeholders.

        Processing order:

        1. Signed-off-by / Co-authored-by lines (before email stripping)
        2. Email addresses → ``[email]``
        3. ``@user`` mentions → ``@user_N``
        4. GitHub profile/repo URLs (username segment)
        5. ``user-images.githubusercontent.com`` URLs → ``[image]``
        6. Home-directory paths (``/home/x``, ``/Users/x``, ``C:\\Users\\x``)
        7. IP addresses → ``[ip]``
        8. Known bare usernames (word-boundary, case-insensitive)
        """
        # 1. Sign-off lines (must precede email stripping so we can match the <email>)
        text = self._SIGNOFF.sub(lambda m: f"{m.group(1)}[name] <[email]>", text)

        # 2. Emails
        text = self._EMAIL.sub("[email]", text)

        # 3. @mentions
        text = self._MENTION.sub(lambda m: f"@{self._placeholder(m.group(1))}", text)

        # 4. GitHub user/org URLs
        text = self._GITHUB_URL.sub(lambda m: f"{m.group(1)}{self._placeholder(m.group(2))}", text)

        # 5. User-uploaded image URLs
        text = self._USER_IMAGES.sub("[image]", text)

        # 6. Home-directory paths
        text = self._HOME_PATH.sub(lambda m: f"{m.group(1)}{self._placeholder(m.group(2))}", text)

        # 7. IP addresses
        text = self._IP_ADDR.sub("[ip]", text)

        # 8. Known bare usernames (longest first to avoid partial replacement)
        for uname in sorted(self._known_usernames, key=len, reverse=True):
            placeholder = self._placeholder(uname)
            text = re.sub(rf"\b{re.escape(uname)}\b", placeholder, text, flags=re.IGNORECASE)

        return text


def render_problem_statement(
    pr: PR,
    issues: list[IssueExpanded] | None = None,
    repo_description: str = "",
    anonymize: bool = False,
    known_usernames: set[str] | None = None,
    extract: bool = True,
) -> str:
    """Render the full problem statement for a FormulaCode task.

    Parameters
    ----------
    pr:
        The pull request providing the initial observations.
    issues:
        Optional list of linked issues to include.
    repo_description:
        A short description of the repository.
    anonymize:
        If ``True``, replace usernames and emails with placeholders.
    known_usernames:
        Additional usernames to scrub even without an ``@`` prefix.
        Only used when *anonymize* is ``True``.
    extract:
        If ``True`` (default), use :class:`ProblemExtractor` to separate
        problem observations from solution details, preventing information
        leakage.  Falls back to raw ``pr.body`` on failure.
    """
    env = _get_env()
    anon = Anonymizer(known_usernames=known_usernames) if anonymize else None

    # Extract problem observations (strip solution details) or use raw body
    if extract:
        try:
            from datasmith.agents.extractors import ProblemExtractor

            extractor = ProblemExtractor()
            extraction = extractor.extract_problem(
                pr_title=getattr(pr, "title", ""),
                pr_body=getattr(pr, "body", ""),
            )
            initial_observations = extraction.to_problem_markdown() or getattr(pr, "body", "")
        except Exception:
            logger.warning("ProblemExtractor failed, falling back to raw PR body")
            initial_observations = getattr(pr, "body", "")
    else:
        initial_observations = getattr(pr, "body", "")

    # Render issues section
    issues_text = ""
    if issues:
        tpl = env.get_template("issues.md.j2")
        issues_text = tpl.render(issues=issues)

    # Render final
    tpl = env.get_template("final.md.j2")
    rendered = tpl.render(
        repo_description=repo_description,
        initial_observations=initial_observations,
        issues=issues_text,
    )

    if anon:
        rendered = anon.anonymize(rendered)

    return rendered
