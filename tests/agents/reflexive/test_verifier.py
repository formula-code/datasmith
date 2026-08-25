"""Parsing and grading the verifier agent's reply.

An error is a rejection, never an acceptance. Every failure path below must
end in accepted=False. The predecessor inverted this once: a host-side timeout
returned success and silently verified about 34% of candidate_containers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from datasmith.agents.installed.base import AgentResult
from datasmith.agents.reflexive.image_integrity import ImageIntegrity, IntegrityFinding
from datasmith.agents.reflexive.verifier import parse_report, verify


def _clean_scan(image: str) -> ImageIntegrity:
    """A host image scan that found nothing wrong.

    Every `container_built` call below must pass one. `verify` now scans the
    image from the host before it runs anything, and an image it cannot scan
    is a rejection -- so a test that omits this collector would be measuring
    `docker create img:1` failing, not the behaviour it names.
    """
    return ImageIntegrity(image=image, collected=True, members_scanned=1, facts={})


def _tampered_scan(image: str) -> ImageIntegrity:
    return ImageIntegrity(
        image=image,
        collected=True,
        members_scanned=1,
        facts={},
        findings=(IntegrityFinding("tamper_audit", "sitecustomize.py on the environment's sys.path"),),
    )


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
        graded = verify(
            "img:1",
            "",
            self._agent(_CLEAN),
            mode="container_built",
            runner=self._runner(),
            integrity_collector=_clean_scan,
        )
        assert graded.accepted is True

    def test_a_hard_failure_is_rejected(self) -> None:
        graded = verify(
            "img:1",
            "",
            self._agent(_GOOD),
            mode="container_built",
            runner=self._runner(),
            integrity_collector=_clean_scan,
        )
        assert graded.accepted is False
        assert graded.hard_failures == ("pytest_collect",)

    def test_unparseable_output_retries_once_then_rejects(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [
            AgentResult(success=True, output="nonsense"),
            AgentResult(success=True, output="still nonsense"),
        ]
        graded = verify(
            "img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan
        )
        assert graded.accepted is False
        assert agent.exec.call_count == 2, "exactly one retry"

    def test_the_retry_succeeds_if_the_second_reply_is_valid(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = [
            AgentResult(success=True, output="nonsense"),
            AgentResult(success=True, output=_CLEAN),
        ]
        assert (
            verify(
                "img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan
            ).accepted
            is True
        )

    def test_an_agent_that_fails_outright_is_rejected(self) -> None:
        agent = self._agent("", success=False)
        assert (
            verify(
                "img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan
            ).accepted
            is False
        )

    def test_an_agent_that_raises_is_rejected_not_propagated(self) -> None:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.side_effect = RuntimeError("agent process died")
        graded = verify(
            "img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan
        )
        assert graded.accepted is False
        assert any("agent process died" in c.evidence for c in graded.report.checks)

    def test_a_crashed_battery_command_forces_a_rejection_regardless_of_the_agent(self) -> None:
        """The agent says accept; a battery command could not run. Reject."""

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            raise RuntimeError("docker daemon went away")

        graded = verify(
            "img:1", "", self._agent(_CLEAN), mode="container_built", runner=runner, integrity_collector=_clean_scan
        )
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
        verify("img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan)
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


class TestTheHostScanOverridesTheAgent:
    """The defect this whole rewrite exists to close.

    `formulacode/attack-demo:1` was ACCEPTED. Its `sitecustomize.py` patched
    `Path.is_file` and `shutil.which`, the in-container probe reported a clean
    container, and the verifier reasoned correctly from false facts. So the
    question these tests ask is not "does the agent notice" -- an agent
    reading true facts might still be talked round -- but "can the agent's
    answer change the verdict at all". It cannot.
    """

    @staticmethod
    def _agent(output: str) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=True, output=output)
        return agent

    @staticmethod
    def _runner():
        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            return ("ok", "", 0)

        return runner

    def test_an_accepting_agent_cannot_rescue_a_tampered_image(self) -> None:
        graded = verify(
            "img:1",
            "",
            self._agent(_CLEAN),
            mode="container_built",
            runner=self._runner(),
            integrity_collector=_tampered_scan,
        )
        assert graded.accepted is False
        assert "tamper_audit" in graded.hard_failures

    def test_the_agent_is_never_asked_about_a_tampered_image(self) -> None:
        """No prompt is worth spending, and no reply could change the answer."""
        agent = self._agent(_CLEAN)
        verify("img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_tampered_scan)
        assert agent.exec.call_count == 0

    def test_the_battery_never_runs_inside_a_tampered_container(self) -> None:
        """Eight commands executing an image already proven adversarial."""
        called: list[list[str]] = []

        def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
            called.append(argv)
            return ("ok", "", 0)

        verify(
            "img:1", "", self._agent(_CLEAN), mode="container_built", runner=runner, integrity_collector=_tampered_scan
        )
        assert called == []

    def test_a_scan_that_could_not_run_is_a_rejection(self) -> None:
        """Nothing known about the image is not the same as nothing wrong."""

        def failed_scan(image: str) -> ImageIntegrity:
            return ImageIntegrity(
                image=image,
                collected=False,
                error="docker create failed",
                findings=(IntegrityFinding("image_scan_failed", "docker create failed"),),
            )

        graded = verify(
            "img:1",
            "",
            self._agent(_CLEAN),
            mode="container_built",
            runner=self._runner(),
            integrity_collector=failed_scan,
        )
        assert graded.accepted is False

    def test_a_collector_that_raises_is_a_rejection_not_a_crash(self) -> None:
        def exploding_scan(image: str) -> ImageIntegrity:
            raise RuntimeError("docker daemon went away")

        graded = verify(
            "img:1",
            "",
            self._agent(_CLEAN),
            mode="container_built",
            runner=self._runner(),
            integrity_collector=exploding_scan,
        )
        assert graded.accepted is False
        assert "image_scan_failed" in graded.hard_failures

    def test_the_default_collector_is_the_host_scan_not_the_container_probe(self) -> None:
        """The seam must default to the module that never runs the image.

        A default of None, or of anything that executes inside the container,
        reinstates the exact defect: stage 6 calls `verify` without passing a
        collector, so whatever the default is IS the production behaviour.
        """
        import inspect

        from datasmith.agents.reflexive import image_integrity

        source = inspect.getsource(verify)
        assert "collect_and_evaluate" in source
        assert inspect.signature(verify).parameters["integrity_collector"].default is None
        assert image_integrity.collect_and_evaluate.__module__ == "datasmith.agents.reflexive.image_integrity"

    def test_the_host_scan_facts_reach_the_prompt(self) -> None:
        """A clean scan is still evidence, and the agent must see it labelled."""
        agent = self._agent(_CLEAN)
        verify("img:1", "", agent, mode="container_built", runner=self._runner(), integrity_collector=_clean_scan)
        prompt = agent.exec.call_args[0][0]
        assert "host_image_scan" in prompt

    def test_omitting_the_collector_actually_calls_the_host_scan(self) -> None:
        """The production call site omits it, so the DEFAULT is the behaviour.

        `synthesizer._run_produce_verify` passes
        `lambda image, log, mode: verifier_verify(image, log, agent, mode)` --
        no collector. A default that is merely present in the signature but
        never reached would leave stage 6 exactly as blind as before, and a
        test that greps the source for `collect_and_evaluate` would not notice.
        So monkeypatch the module attribute and assert it is invoked.
        """
        import datasmith.agents.reflexive.verifier as verifier_module

        seen: list[str] = []

        def spy(image: str) -> ImageIntegrity:
            seen.append(image)
            return ImageIntegrity(image=image, collected=True, members_scanned=1, facts={})

        original = verifier_module.collect_and_evaluate
        verifier_module.collect_and_evaluate = spy  # type: ignore[assignment]
        try:
            verifier_module.verify("img:1", "", self._agent(_CLEAN), mode="container_built", runner=self._runner())
        finally:
            verifier_module.collect_and_evaluate = original  # type: ignore[assignment]

        assert seen == ["img:1"], "verify() did not reach the host scan when no collector was passed"


class TestABuildThatFailedIsNeverAccepted:
    """The false accept that reached `candidate_containers`.

    `grade()` computes `accepted = not hard`, so a report with an EMPTY checks
    list IS an acceptance. In build_failed mode there are no battery facts to
    report checks about, so an agent that dutifully reports nothing accepts a
    container that does not exist.

    Observed on mars-project/mars#3329 (2026-08-25): round 1 failed to build,
    the verifier accepted, `_save_context` stored the context, the runner
    logged "Successfully synthesized image", and the next step died with a
    DockerException building an image whose build had already failed.

    The 16-container labelled set structurally cannot catch this -- it only
    runs mode="container_built" against images that already exist -- which is
    why the guard lives here as an invariant rather than in that set.
    """

    @staticmethod
    def _agent(output: str) -> MagicMock:
        agent = MagicMock()
        agent.name.return_value = "codex"
        agent.exec.return_value = AgentResult(success=True, output=output)
        return agent

    _EMPTY = '{"verdict":"accept","mode":"build_failed","checks":[],"evidence_you_lack":[]}'

    def test_an_empty_report_in_build_failed_mode_is_rejected(self) -> None:
        graded = verify(None, "ERROR: failed to build", self._agent(self._EMPTY), mode="build_failed")
        assert graded.accepted is False
        assert "build_failed" in graded.hard_failures

    def test_an_explicit_accept_in_build_failed_mode_is_rejected(self) -> None:
        """Even a confident agent does not get to invent a container."""
        report = (
            '{"verdict":"accept","mode":"build_failed","checks":'
            '[{"id":"pytest_run","verdict":"pass","cause":"other","evidence":"looks fine","remedy":""}],'
            '"evidence_you_lack":[]}'
        )
        assert verify(None, "boom", self._agent(report), mode="build_failed").accepted is False

    def test_the_build_log_travels_with_the_rejection(self) -> None:
        """The producer needs the cause, not just the verdict."""
        graded = verify(None, "SENTINEL_BUILD_ERROR", self._agent(self._EMPTY), mode="build_failed")
        assert any("SENTINEL_BUILD_ERROR" in c.evidence for c in graded.report.checks)

    def test_the_agents_own_checks_are_preserved(self) -> None:
        """Its diagnosis is still what tells the producer what to fix."""
        report = (
            '{"verdict":"reject","mode":"build_failed","checks":'
            '[{"id":"import_sweep","verdict":"fail","cause":"module_not_found",'
            '"evidence":"No module named salem","remedy":"add salem"}],"evidence_you_lack":[]}'
        )
        graded = verify(None, "boom", self._agent(report), mode="build_failed")
        assert graded.accepted is False
        ids = [c.id for c in graded.report.checks]
        assert "import_sweep" in ids and "build_failed" in ids

    def test_container_built_mode_can_still_accept(self) -> None:
        """The guard must not make every verdict a rejection."""
        graded = verify(
            "img:1",
            "",
            self._agent(_CLEAN),
            mode="container_built",
            runner=lambda i, a, t: ("ok", "", 0),
            integrity_collector=_clean_scan,
        )
        assert graded.accepted is True
