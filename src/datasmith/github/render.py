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
    """Replace usernames and emails with deterministic placeholders."""

    def __init__(self) -> None:
        self._map: dict[str, str] = {}
        self._counter = 0
        self._email_re = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
        self._user_re = re.compile(r"@([\w-]+)")

    def anonymize(self, text: str) -> str:
        """Replace emails with ``[email]`` and ``@user`` mentions with ``@user_N``."""
        text = self._email_re.sub("[email]", text)

        def _replace_user(m: re.Match[str]) -> str:
            username = m.group(1)
            if username not in self._map:
                self._counter += 1
                self._map[username] = f"@user_{self._counter}"
            return self._map[username]

        return self._user_re.sub(_replace_user, text)


def render_problem_statement(
    pr: PR,
    issues: list[IssueExpanded] | None = None,
    repo_description: str = "",
    anonymize: bool = False,
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
    """
    env = _get_env()
    anon = Anonymizer() if anonymize else None

    # Render issues section
    issues_text = ""
    if issues:
        tpl = env.get_template("issues.md.j2")
        issues_text = tpl.render(issues=issues)

    # Render final
    tpl = env.get_template("final.md.j2")
    rendered = tpl.render(
        repo_description=repo_description,
        initial_observations=pr.body,
        issues=issues_text,
    )

    if anon:
        rendered = anon.anonymize(rendered)

    return rendered
