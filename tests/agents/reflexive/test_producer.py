"""The producer half: consume a report, edit only the two scripts it owns.

The producer never sees the verifier's reasoning -- only the typed report and
its evidence. Keeping it out of the deliberation is what protects both
contexts; keeping it short of evidence would only make it guess.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from datasmith.agents.installed.base import AgentResult
from datasmith.agents.reflexive.producer import parse_plan, revise
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import grade
from datasmith.docker.context import DockerContext

_PLAN = (
    '{"understood_rejections":["backend missing"],'
    '"planned_edits":[{"file":"docker_build_pkg.sh","change":"install hatchling first",'
    '"addresses_check_id":"pep517_backend"}],"evidence_requests":[]}'
)


def _graded():
    return grade(
        RejectionReport(
            verdict=Verdict.REJECT,
            mode="build_failed",
            checks=[
                CheckResult(
                    id="pep517_backend",
                    verdict="fail",
                    cause=Cause.MODULE_NOT_FOUND,
                    evidence="Cannot import 'hatchling.build'",
                    remedy="install hatchling",
                )
            ],
        )
    )


class TestParsePlan:
    def test_plain_json_parses(self) -> None:
        plan = parse_plan(_PLAN)
        assert plan is not None
        assert plan.planned_edits[0].file == "docker_build_pkg.sh"

    def test_a_fenced_block_parses(self) -> None:
        assert parse_plan(f"```json\n{_PLAN}\n```") is not None

    def test_garbage_returns_none(self) -> None:
        assert parse_plan("I will fix it, trust me.") is None

    def test_a_plan_editing_a_forbidden_file_returns_none(self) -> None:
        """docker_build_final.sh holds the sealer. Out of bounds."""
        bad = _PLAN.replace("docker_build_pkg.sh", "docker_build_final.sh")
        assert parse_plan(bad) is None


class TestRevise:
    @staticmethod
    def _agent(output: str, success: bool = True, files: list[str] | None = None) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=success, output=output, files_changed=files or [])
        return agent

    def test_the_prompt_contains_the_report_evidence(self, tmp_path: Path) -> None:
        agent = self._agent(_PLAN)
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        revise(DockerContext(), _graded(), agent, tmp_path)
        prompt = agent.exec.call_args[0][0]
        assert "Cannot import 'hatchling.build'" in prompt

    def test_the_prompt_never_contains_verifier_reasoning(self, tmp_path: Path) -> None:
        """Only the typed report crosses the boundary."""
        agent = self._agent(_PLAN)
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        revise(DockerContext(), _graded(), agent, tmp_path)
        prompt = agent.exec.call_args[0][0].lower()
        for leak in ("transcript", "reasoning", "chain of thought"):
            assert leak not in prompt

    def test_edited_scripts_are_read_back_into_the_context(self, tmp_path: Path) -> None:
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\nEDITED_BY_PRODUCER\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, plan = revise(DockerContext(), _graded(), self._agent(_PLAN), tmp_path)
        assert context is not None
        assert "EDITED_BY_PRODUCER" in context.build_pkg_sh
        assert plan is not None

    def test_an_agent_that_fails_returns_none(self, tmp_path: Path) -> None:
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, _plan = revise(DockerContext(), _graded(), self._agent("", success=False), tmp_path)
        assert context is None

    def test_an_agent_that_raises_returns_none_rather_than_propagating(self, tmp_path: Path) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = RuntimeError("agent died")
        context, plan = revise(DockerContext(), _graded(), agent, tmp_path)
        assert context is None
        assert plan is None

    def test_an_unparseable_plan_still_picks_up_script_edits(self, tmp_path: Path) -> None:
        """The scripts on disk are the deliverable. The plan is commentary."""
        (tmp_path / "docker_build_pkg.sh").write_text("#!/bin/bash\nEDITED\n")
        (tmp_path / "docker_build_run.sh").write_text("#!/bin/bash\n")
        context, plan = revise(DockerContext(), _graded(), self._agent("no json here"), tmp_path)
        assert context is not None and "EDITED" in context.build_pkg_sh
        assert plan is None
