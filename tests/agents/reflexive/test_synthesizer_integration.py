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

    def _run(self, monkeypatch, *, verify_accepts: bool, tampered: bool = False, stored_rows=None, db_raises=False):
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

        # The stored-context read must never reach a live database from a test.
        def fake_fetch_all(table, **kwargs):
            calls.setdefault("fetch_all", []).append((table, kwargs))
            if db_raises:
                raise RuntimeError("postgrest is down")
            return list(stored_rows or [])

        monkeypatch.setattr("datasmith.utils.db.fetch_all", fake_fetch_all)

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


class TestTheLoopStartsFromWhatAlreadyWorked:
    """`--force` used to restart every task from the stock template.

    A container that earned a pass under older code then had to repeat its
    entire repair to earn it again: xdslproject/xdsl#1332 was rejected at
    round 1 on `pytest_collect` even though a prior run had already solved
    exactly that, because the scripts holding the fix were in the database and
    the loop never read them. With 248 already-built rows to re-run, that is
    the difference between one round and five.

    Seeding changes where the loop STARTS, never what it concludes: the image
    is rebuilt, the host scan re-run, and the verifier re-grades from scratch.
    """

    def _context_seen_by_the_build(self, calls):
        assert calls["verify_context"], "verify_context was never called"
        return calls["verify_context"][0]["context"]

    def test_the_stored_scripts_seed_the_first_round(self, monkeypatch) -> None:
        runner = TestTheBranchActuallyRuns()
        _outcome, calls, _s = runner._run(
            monkeypatch,
            verify_accepts=True,
            stored_rows=[{"build_pkg_sh": "STORED PKG", "build_run_sh": "STORED RUN"}],
        )
        context = self._context_seen_by_the_build(calls)
        assert context.build_pkg_sh == "STORED PKG"
        assert context.build_run_sh == "STORED RUN"

    def test_the_template_is_used_when_nothing_is_stored(self, monkeypatch) -> None:
        from datasmith.agents.synthesizer import _load_default_context

        runner = TestTheBranchActuallyRuns()
        _outcome, calls, _s = runner._run(monkeypatch, verify_accepts=True, stored_rows=[])
        context = self._context_seen_by_the_build(calls)
        assert context.build_pkg_sh == _load_default_context().build_pkg_sh

    def test_a_database_failure_falls_back_to_the_template(self, monkeypatch) -> None:
        """An unreadable row must not abort synthesis -- it is an optimisation."""
        from datasmith.agents.synthesizer import _load_default_context

        runner = TestTheBranchActuallyRuns()
        outcome, calls, _s = runner._run(monkeypatch, verify_accepts=True, db_raises=True)
        context = self._context_seen_by_the_build(calls)
        assert context.build_pkg_sh == _load_default_context().build_pkg_sh
        assert outcome.accepted is True

    def test_only_the_producer_owned_scripts_carry_over(self, monkeypatch) -> None:
        """Template fixes to the other scripts must stay live.

        If a stored row could overwrite `docker_build_env.sh`, every re-run
        would resurrect the environment script that shipped whenever that row
        was written, and template-level fixes would silently not apply.
        """
        from datasmith.agents.synthesizer import _load_default_context

        runner = TestTheBranchActuallyRuns()
        _outcome, calls, _s = runner._run(
            monkeypatch,
            verify_accepts=True,
            stored_rows=[{"build_pkg_sh": "STORED PKG", "build_run_sh": "STORED RUN", "build_env_sh": "STALE ENV"}],
        )
        context = self._context_seen_by_the_build(calls)
        assert context.build_env_sh == _load_default_context().build_env_sh
        assert context.build_env_sh != "STALE ENV"

    def test_an_empty_stored_row_does_not_blank_the_scripts(self, monkeypatch) -> None:
        """A row whose scripts are empty strings must not produce an empty build."""
        from datasmith.agents.synthesizer import _load_default_context

        runner = TestTheBranchActuallyRuns()
        _outcome, calls, _s = runner._run(
            monkeypatch,
            verify_accepts=True,
            stored_rows=[{"build_pkg_sh": "   ", "build_run_sh": ""}],
        )
        context = self._context_seen_by_the_build(calls)
        assert context.build_pkg_sh == _load_default_context().build_pkg_sh

    def test_a_tampered_stored_context_is_not_seeded(self, monkeypatch) -> None:
        """The hazard TRY_SIMILAR is switched off to avoid.

        Stored contexts are agent-authored, and this module's header records
        that 128 repositories' stored contexts install a sitecustomize shim
        into site-packages. Seeding without an audit would carry those back in
        through a door the `DATASMITH_SKIP_SIMILAR_CONTEXTS` switch does not
        cover. The host image scan is still the gate; this keeps a known-bad
        row from spending eight rounds proving it.
        """
        from datasmith.agents.synthesizer import _load_default_context

        runner = TestTheBranchActuallyRuns()
        _outcome, calls, _s = runner._run(
            monkeypatch,
            verify_accepts=True,
            tampered=True,
            stored_rows=[{"build_pkg_sh": "SHIM PKG", "build_run_sh": "SHIM RUN"}],
        )
        context = self._context_seen_by_the_build(calls)
        assert context.build_pkg_sh == _load_default_context().build_pkg_sh
        assert context.build_pkg_sh != "SHIM PKG"
