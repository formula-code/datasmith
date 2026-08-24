"""PRODUCE_VERIFY replaces LLM_GENERATE, behind a flag that is off by default.

The states before it are untouched, so a repository the stock template already
builds never invokes an agent at all.
"""

from __future__ import annotations

import importlib

from datasmith.agents.synthesizer import SynthesisState


def test_the_new_state_exists() -> None:
    assert SynthesisState.PRODUCE_VERIFY.value == "produce_verify"


def test_the_flag_is_off_by_default() -> None:
    import datasmith.agents.reflexive.loop as loop_mod

    importlib.reload(loop_mod)
    assert loop_mod.DATASMITH_PV_ENABLED is False, "must not go live without an explicit opt-in"


def test_the_legacy_states_are_unchanged() -> None:
    """TRY_DEFAULT now succeeds without an agent for a growing share of
    repositories, and that path must keep costing nothing."""
    for name in ("CHECK_CACHE", "FIND_SIMILAR", "TRY_SIMILAR", "TRY_DEFAULT", "FAIL"):
        assert hasattr(SynthesisState, name)


def test_llm_generate_still_exists_for_the_disabled_path() -> None:
    """With the flag off, the old behaviour must remain reachable."""
    assert SynthesisState.LLM_GENERATE.value == "llm_generate"


def test_produce_verify_runs_the_tamper_audit_on_the_producer_context() -> None:
    """Otherwise producer-side tampering is checked by nobody.

    The legacy path calls classify_context after TRY_DEFAULT and after every
    LLM_GENERATE attempt. The battery collects functional facts only, so
    without this `tamper_audit` is a check id nothing can ever emit -- and
    attack-demo:1 and pysindy#139, both labelled reject in the validation set,
    would have nothing to be rejected on. Neither breaks function; both defeat
    detection.
    """
    import inspect

    from datasmith.agents.synthesizer import Synthesizer

    source = inspect.getsource(Synthesizer._run_produce_verify)
    assert "classify_context" in source, "PRODUCE_VERIFY must run the tamper audit"
    assert "tamper.tampered" in source, "and must act on its result"


class TestTheBranchActuallyRuns:
    """Executing coverage for PRODUCE_VERIFY, which the plan's tests lack.

    The five tests above check enum values, the default flag state, and grep
    `inspect.getsource` for two strings. Every one of them would stay green if
    the wiring were wrong -- if build() forgot run_tests_gate=False, if both
    roles shared one agent instance, if a tampered revision were accepted, or
    if the manifest never reached _save_context.

    The Task 8 auditor exercised the branch with throwaway stubs and found it
    correct. These make that permanent.
    """

    @staticmethod
    def _synth():
        from datasmith.agents.synthesizer import Synthesizer

        return Synthesizer.__new__(Synthesizer)

    @staticmethod
    def _sandbox_result(success: bool = True):
        from datasmith.agents.sandbox import SandboxResult

        return SandboxResult(
            success=success,
            image_tag="formulacode/x:1" if success else "",
            build_manifest={"build": {"owner": "o"}},
            resource_metrics={"peak_mem_gb": 1.5},
        )

    def _run(self, monkeypatch, *, verify_accepts: bool, tampered: bool = False):
        """Drive one round with every collaborator stubbed."""
        from unittest.mock import MagicMock

        import datasmith.agents.synthesizer as syn
        from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
        from datasmith.agents.reflexive.severity import grade

        calls: dict = {"verify_context": [], "get_agent": []}

        def fake_verify_context(**kwargs):
            calls["verify_context"].append(kwargs)
            return self._sandbox_result(success=True)

        def fake_get_agent(pref):
            calls["get_agent"].append(pref)
            return MagicMock(name=f"agent-{pref}")

        report = RejectionReport(
            verdict=Verdict.ACCEPT if verify_accepts else Verdict.REJECT,
            mode="container_built",
            checks=[]
            if verify_accepts
            else [
                CheckResult(id="pytest_collect", verdict="fail", cause=Cause.MODULE_NOT_FOUND, evidence="e", remedy="r")
            ],
        )
        tamper = MagicMock()
        tamper.tampered = tampered
        tamper.as_list.return_value = ["forged_logs_json"] if tampered else []

        monkeypatch.setattr(syn, "verify_context", fake_verify_context)
        monkeypatch.setattr("datasmith.agents.installed.base.get_agent", fake_get_agent)
        monkeypatch.setattr("datasmith.agents.reflexive.verifier.verify", lambda *a, **k: grade(report))
        monkeypatch.setattr(
            "datasmith.agents.reflexive.producer.revise",
            lambda ctx, graded, agent, workdir: (ctx.model_copy(update={"build_pkg_sh": "edited"}), None),
        )
        monkeypatch.setattr(syn, "classify_context", lambda ctx: tamper)

        synth = self._synth()
        synth._trace = []
        synth._log_tamper = MagicMock()

        outcome = synth._run_produce_verify(
            owner="o",
            repo="r",
            sha="s",
            issue_number=1,
            repo_image="img",
            env_payload="{}",
            python_version="3.12",
            base_sha="b",
            solution_patch="",
        )
        return outcome, calls, synth

    def test_an_accepting_round_returns_the_context_with_manifest_and_metrics(self, monkeypatch) -> None:
        outcome, _calls, _s = self._run(monkeypatch, verify_accepts=True)
        assert outcome.accepted is True
        assert outcome.stop_reason == "accepted"
        assert outcome.build_manifest == {"build": {"owner": "o"}}, "the manifest must reach candidate_containers"
        assert outcome.resource_metrics == {"peak_mem_gb": 1.5}

    def test_the_build_never_applies_the_legacy_pytest_gate(self, monkeypatch) -> None:
        """If this regresses, the verifier never receives an image to judge."""
        _outcome, calls, _s = self._run(monkeypatch, verify_accepts=True)
        assert calls["verify_context"], "verify_context was never called"
        for kwargs in calls["verify_context"]:
            assert kwargs["run_tests_gate"] is False

    def test_producer_and_verifier_get_separate_agent_instances(self, monkeypatch) -> None:
        """Shared context is the failure this whole design exists to prevent."""
        _outcome, calls, _s = self._run(monkeypatch, verify_accepts=True)
        assert len(calls["get_agent"]) == 2, "one agent instance per role"

    def test_a_rejecting_round_does_not_accept(self, monkeypatch) -> None:
        outcome, _calls, _s = self._run(monkeypatch, verify_accepts=False)
        assert outcome.accepted is False

    def test_a_tampered_revision_is_refused_and_logged(self, monkeypatch) -> None:
        """The audit the battery cannot supply. Without it, producer-side
        tampering in PRODUCE_VERIFY is checked by nobody."""
        outcome, _calls, synth = self._run(monkeypatch, verify_accepts=False, tampered=True)
        assert outcome.accepted is False
        assert outcome.stop_reason == "producer_failed"
        assert synth._log_tamper.called, "a tampered revision must reach error_logs"
        assert synth._log_tamper.call_args[0][-1] == "produce_verify"
