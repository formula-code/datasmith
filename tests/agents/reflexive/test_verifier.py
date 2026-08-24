"""Parsing and grading the verifier agent's reply.

An error is a rejection, never an acceptance. Every failure path below must
end in accepted=False. The predecessor inverted this once: a host-side timeout
returned success and silently verified about 34% of candidate_containers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from datasmith.agents.installed.base import AgentResult
from datasmith.agents.reflexive.verifier import parse_report, verify

_GOOD = (
    '{"verdict":"reject","mode":"container_built","checks":'
    '[{"id":"pytest_collect","verdict":"fail","cause":"pytest_version_incompat",'
    '"evidence":"parametrize names (1)","remedy":"pin pytest"}],"evidence_you_lack":[]}'
)
_CLEAN = '{"verdict":"accept","mode":"container_built","checks":[],"evidence_you_lack":[]}'


class TestParseReport:
    def test_plain_json_parses(self) -> None:
        report = parse_report(_GOOD)
        assert report is not None
        assert report.checks[0].id == "pytest_collect"

    def test_a_fenced_code_block_parses(self) -> None:
        """Agents add fences even when told not to."""
        assert parse_report(f"```json\n{_GOOD}\n```") is not None

    def test_json_with_surrounding_prose_parses(self) -> None:
        assert parse_report(f"Here is my verdict:\n{_GOOD}\nLet me know.") is not None

    def test_garbage_returns_none_rather_than_raising(self) -> None:
        assert parse_report("I could not determine a verdict.") is None

    def test_valid_json_of_the_wrong_shape_returns_none(self) -> None:
        assert parse_report('{"hello":"world"}') is None


class TestVerify:
    @staticmethod
    def _agent(output: str, success: bool = True) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=success, output=output)
        return agent

    @staticmethod
    def _runner(*_a, **_k):
        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            return ("ok", "", 0)

        return runner

    def test_a_clean_report_is_accepted(self) -> None:
        graded = verify("img:1", "", self._agent(_CLEAN), mode="container_built", runner=self._runner())
        assert graded.accepted is True

    def test_a_hard_failure_is_rejected(self) -> None:
        graded = verify("img:1", "", self._agent(_GOOD), mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert graded.hard_failures == ("pytest_collect",)

    def test_unparseable_output_retries_once_then_rejects(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [
            AgentResult(success=True, output="nonsense"),
            AgentResult(success=True, output="still nonsense"),
        ]
        graded = verify("img:1", "", agent, mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert agent.exec.call_count == 2, "exactly one retry"

    def test_the_retry_succeeds_if_the_second_reply_is_valid(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [
            AgentResult(success=True, output="nonsense"),
            AgentResult(success=True, output=_CLEAN),
        ]
        assert verify("img:1", "", agent, mode="container_built", runner=self._runner()).accepted is True

    def test_an_agent_that_fails_outright_is_rejected(self) -> None:
        agent = self._agent("", success=False)
        assert verify("img:1", "", agent, mode="container_built", runner=self._runner()).accepted is False

    def test_an_agent_that_raises_is_rejected_not_propagated(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = RuntimeError("agent process died")
        graded = verify("img:1", "", agent, mode="container_built", runner=self._runner())
        assert graded.accepted is False
        assert any("agent process died" in c.evidence for c in graded.report.checks)

    def test_a_crashed_battery_command_forces_a_rejection_regardless_of_the_agent(self) -> None:
        """The agent says accept; a battery command could not run. Reject."""

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            raise RuntimeError("docker daemon went away")

        graded = verify("img:1", "", self._agent(_CLEAN), mode="container_built", runner=runner)
        assert graded.accepted is False

    def test_build_failed_mode_does_not_run_the_battery(self) -> None:
        """Mode A has no image. Running a battery against None would crash."""
        called = []

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            called.append(argv)
            return ("", "", 0)

        verify(None, "BackendUnavailable: hatchling.build", self._agent(_CLEAN), mode="build_failed", runner=runner)
        assert called == [], "no battery in build_failed mode"

    def test_the_prompt_carries_the_build_log_in_build_failed_mode(self) -> None:
        agent = self._agent(_CLEAN)
        verify(None, "SENTINEL_LOG_LINE", agent, mode="build_failed", runner=self._runner())
        assert "SENTINEL_LOG_LINE" in agent.exec.call_args[0][0]

    def test_the_prompt_never_contains_the_producer_scripts_as_writable(self) -> None:
        """Read-only copies are fine; an instruction to edit is not."""
        agent = self._agent(_CLEAN)
        verify("img:1", "", agent, mode="container_built", runner=self._runner())
        prompt = agent.exec.call_args[0][0]
        assert "never edit" in prompt.lower() or "read-only" in prompt.lower()


class TestTheVerifierStructurallyCannotSeeTheBuildScripts:
    """A substring check is not an isolation guarantee.

    `test_the_prompt_never_contains_the_producer_scripts_as_writable` asserts
    only that "read-only" or "never edit" appears in the prompt. It would pass
    unchanged if the prompt embedded the full text of docker_build_pkg.sh.

    The real guarantee is structural: `verify` is never handed a DockerContext,
    so it has nothing to leak. Enforced here so a later signature change cannot
    quietly weaken it.
    """

    def test_verify_does_not_accept_a_docker_context(self) -> None:
        import inspect

        from datasmith.agents.reflexive.verifier import verify

        params = inspect.signature(verify).parameters
        for name, param in params.items():
            annotation = str(param.annotation)
            assert "DockerContext" not in annotation, (
                f"verify() takes {name}: {annotation} -- the verifier must not receive the build context"
            )

    def test_the_prompt_never_carries_shell_script_bodies(self) -> None:
        """Belt and braces: a real prompt, checked for script content."""
        from unittest.mock import MagicMock

        from datasmith.agents.installed.base import AgentResult
        from datasmith.agents.reflexive.verifier import verify

        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(
            success=True, output='{"verdict":"accept","mode":"build_failed","checks":[],"evidence_you_lack":[]}'
        )
        verify(None, "some build log", agent, mode="build_failed")
        prompt = agent.exec.call_args[0][0]
        for marker in ("#!/usr/bin/env bash", "#!/bin/bash", "micromamba activate", "PIP_NO_BUILD_ISOLATION"):
            assert marker not in prompt, f"prompt carries build-script content: {marker!r}"
