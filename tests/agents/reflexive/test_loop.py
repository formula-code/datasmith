"""Rounds, budget, no-progress, fail-closed.

Rule 3 (no progress) is the one that matters. A build costs 300 to 700
seconds, so a round that cannot learn anything must never be spent.
"""

from __future__ import annotations

from pathlib import Path

from datasmith.agents.reflexive.loop import progress_key, run_loop
from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
from datasmith.agents.reflexive.severity import grade
from datasmith.docker.context import DockerContext


def _graded(*check_ids: str, mode: str = "container_built"):
    return grade(
        RejectionReport(
            verdict=Verdict.REJECT if check_ids else Verdict.ACCEPT,
            mode=mode,
            checks=[
                CheckResult(id=c, verdict="fail", cause=Cause.MODULE_NOT_FOUND, evidence="e", remedy="r")
                for c in check_ids
            ],
        )
    )


class TestProgressKey:
    def test_mode_b_uses_the_hard_failure_ids(self) -> None:
        assert progress_key(_graded("a", "b"), build_log="") == ("a", "b")

    def test_ids_are_order_independent(self) -> None:
        assert progress_key(_graded("b", "a"), "") == progress_key(_graded("a", "b"), "")

    def test_mode_a_uses_the_build_log_signature_not_invented_ids(self) -> None:
        """Agent-invented ids are unstable: the same model said 'pytest-suite'
        in one run and 'pep517_editable_backend_import' in another."""
        log = "2.15 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n------"
        a = grade(
            RejectionReport(
                verdict=Verdict.REJECT,
                mode="build_failed",
                checks=[CheckResult(id="pytest-suite", verdict="fail", cause=Cause.OTHER, evidence="e", remedy="")],
            )
        )
        b = grade(
            RejectionReport(
                verdict=Verdict.REJECT,
                mode="build_failed",
                checks=[
                    CheckResult(
                        id="pep517_editable_backend_import", verdict="fail", cause=Cause.OTHER, evidence="e", remedy=""
                    )
                ],
            )
        )
        assert progress_key(a, log) == progress_key(b, log), "same log means no progress, whatever the agent called it"

    def test_a_different_build_log_is_progress(self) -> None:
        a = _graded("x", mode="build_failed")
        assert progress_key(a, "ModuleNotFoundError: No module named 'salem'") != progress_key(
            a, "ModuleNotFoundError: No module named 'dask'"
        )


class TestRunLoop:
    @staticmethod
    def _build(results: list[tuple[bool, str]]):
        calls = {"n": 0}

        def build(context: DockerContext) -> tuple[bool, str | None, str]:
            i = min(calls["n"], len(results) - 1)
            calls["n"] += 1
            ok, log = results[i]
            return ok, ("img:1" if ok else None), log

        build.calls = calls  # type: ignore[attr-defined]
        return build

    def test_an_immediately_clean_build_accepts_in_one_round(self) -> None:
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(True, "")]),
            verify=lambda image, log, mode: _graded(),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is True
        assert outcome.rounds == 1
        assert outcome.stop_reason == "accepted"

    def test_a_fixed_build_accepts_on_the_second_round(self) -> None:
        verdicts = [_graded("pep517_backend"), _graded()]
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "BackendUnavailable"), (True, "")]),
            verify=lambda image, log, mode: verdicts.pop(0),
            revise=lambda ctx, graded: (ctx.model_copy(update={"build_pkg_sh": "fixed"}), None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is True
        assert outcome.rounds == 2

    def test_no_progress_stops_before_the_budget_is_spent(self) -> None:
        """Same hard failures twice. Stop -- a third build learns nothing."""
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "same log")]),
            verify=lambda image, log, mode: _graded("pep517_backend"),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
            max_rounds=5,
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "no_progress"
        assert outcome.rounds == 2, "round 1 establishes the key, round 2 repeats it"

    def test_the_budget_stops_a_loop_that_keeps_changing(self) -> None:
        logs = [(False, f"distinct failure {i}") for i in range(10)]
        outcome = run_loop(
            context=DockerContext(),
            build=self._build(logs),
            verify=lambda image, log, mode: _graded(f"check_{log}"),
            revise=lambda ctx, graded: (ctx.model_copy(update={"build_pkg_sh": log_marker()}), None),
            workdir=Path("/tmp"),
            max_rounds=3,
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "budget"
        assert outcome.rounds == 3

    def test_a_producer_that_cannot_revise_ends_the_loop(self) -> None:
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "boom")]),
            verify=lambda image, log, mode: _graded("x"),
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "producer_failed"

    def test_a_verify_that_raises_is_a_rejection_not_a_crash(self) -> None:
        def boom(image, log, mode):
            raise RuntimeError("verifier exploded")

        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(True, "")]),
            verify=boom,
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert outcome.accepted is False

    def test_build_failure_uses_build_failed_mode(self) -> None:
        seen: list[str] = []

        def verify(image, log, mode):
            seen.append(mode)
            return _graded("x")

        run_loop(
            context=DockerContext(),
            build=self._build([(False, "boom")]),
            verify=verify,
            revise=lambda ctx, graded: (None, None),
            workdir=Path("/tmp"),
        )
        assert seen == ["build_failed"]


def log_marker() -> str:
    """Distinct script text per round, so the context genuinely changes."""
    import uuid

    return f"# {uuid.uuid4()}"


def test_the_duplicated_signature_agrees_with_the_prepass_one() -> None:
    """loop.py duplicates scripts/prepass_trial.py's scan because scripts/ is
    not importable. The two must not drift."""
    import importlib.util
    import sys
    from pathlib import Path as _P

    from datasmith.agents.reflexive.loop import _signature as loop_sig

    root = _P(__file__).parents[3]
    spec = importlib.util.spec_from_file_location("_pp", root / "scripts" / "prepass_trial.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_pp"] = mod
    spec.loader.exec_module(mod)

    cases = [
        "2.15 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n------",
        "!!!!!! Interrupted: 2 errors during collection !!!!!!\nFORMULACODE_SNAPSHOT_END",
        "8.6 ModuleNotFoundError: No module named 'salem'\n------",
        "0.3 fatal: reference is not a tree: abc123\n------",
        "5.5 requirements are unsatisfiable.\n------",
    ]
    for case in cases:
        assert loop_sig(case) == mod._signature({"error_message": case}), f"drifted on {case[:40]!r}"
