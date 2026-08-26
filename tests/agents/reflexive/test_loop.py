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
        """The same hard failure, round after round, must end the loop.

        It ends on the repeat COUNT, not on the first repetition. Over 38
        recorded PV failures the detector -- not the round budget -- ended 27
        of them, 12 at round 2, which gave the producer a single attempt at
        each failure. It still stops well short of the budget.
        """
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "same log")]),
            verify=lambda image, log, mode: _graded("pep517_backend"),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
            max_rounds=8,
        )
        assert outcome.accepted is False
        assert outcome.stop_reason == "no_progress"
        assert outcome.rounds == 3, "round 1 sets the key; rounds 2 and 3 repeat it"
        assert outcome.rounds < 8, "the detector, not the budget, must end it"

    def test_the_producer_gets_a_second_attempt_at_the_same_failure(self, monkeypatch) -> None:
        """One attempt per failure was the single largest source of dead runs.

        A producer whose SECOND edit fixes the build must reach that edit. With
        a single-shot detector the loop returned `no_progress` at round 2 and
        the fix was never built.
        """
        builds = [(False, "same log"), (False, "same log"), (True, "fixed")]
        seen: list[str] = []

        def build(ctx):
            ok, log = builds[min(len(seen), len(builds) - 1)]
            seen.append(log)
            return ok, ("formulacode/x:1" if ok else None), log

        def verify(image, log, mode):
            return _graded() if log == "fixed" else _graded("pep517_backend")

        outcome = run_loop(
            context=DockerContext(),
            build=build,
            verify=verify,
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
            max_rounds=8,
        )
        assert outcome.accepted is True, "the third build must be reached"
        assert outcome.rounds == 3

    def test_the_stall_threshold_is_tunable(self, monkeypatch) -> None:
        """A single-shot detector must remain reachable via the env knob."""
        import datasmith.agents.reflexive.loop as loop_mod

        monkeypatch.setattr(loop_mod, "DATASMITH_PV_STALL_REPEATS", 1)
        outcome = run_loop(
            context=DockerContext(),
            build=self._build([(False, "same log")]),
            verify=lambda image, log, mode: _graded("pep517_backend"),
            revise=lambda ctx, graded: (ctx, None),
            workdir=Path("/tmp"),
            max_rounds=8,
        )
        assert outcome.stop_reason == "no_progress"
        assert outcome.rounds == 2

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
        # branch 1: a named cause
        "2.15 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n------",
        "!!!!!! Interrupted: 2 errors during collection !!!!!!\nFORMULACODE_SNAPSHOT_END",
        "8.6 ModuleNotFoundError: No module named 'salem'\n------",
        "0.3 fatal: reference is not a tree: abc123\n------",
        "5.5 requirements are unsatisfiable.\n------",
        # branch 2: no named cause, so the last non-noise line wins. The
        # original five cases all hit branch 1, so the two implementations
        # could differ here (they did: [:110] against [:90]) and this test
        # still passed.
        "9.9 " + ("x" * 200) + "\n------",
        "3.1 something went wrong in a way we do not name\n------",
        # a pipe: prepass replaces it to protect its markdown table, and the
        # loop must do the same or the two disagree.
        "8.6 ModuleNotFoundError: No module named 'a|b'\n------",
        # nothing usable at all
        "------\nFORMULACODE_SNAPSHOT_END",
        # local_ci.py's stage-only summary. It must be treated as noise by
        # BOTH, or a signature drifts the moment a build fails without naming
        # a cause -- which is the common case.
        "9.1 could not find a compiler\nBuild failed at stage 'pkg': Docker build failed (rc=1)",
        "Build failed at stage 'env': Docker build failed (rc=1)",
        "Build failed: something generic",
        # `_default_failure_message` renders the same summary STAGE-PREFIXED,
        # which a startswith test misses. Both copies must skip it, or a
        # signature drifts the moment the raw build log is absent.
        "pkg: Build failed at stage 'pkg': Docker build failed (rc=1)",
        "9.2 error: no C compiler\nenv: Build failed at stage 'env': Docker build failed (rc=1)",
    ]
    for case in cases:
        assert loop_sig(case) == mod._signature({"error_message": case}), f"drifted on {case[:40]!r}"


class TestTheBuildLogHandedToTheLoopCanBeSigned:
    """The defect that stopped OGGM/oggm#1830 after three rounds.

    `synthesizer._run_produce_verify.build()` used to hand the loop
    `json.dumps(result.failure_json or {})`. That is ONE line beginning with
    `{`, and `_signature` skips lines beginning with `{` -- correctly, since in
    a real build log they are noise. So the entire log was discarded and every
    mode-A round that did not happen to contain a named cause signed as
    "no signature". Two genuinely different failures compared equal, rule 3
    fired, and the loop stopped while the producer was still making progress.

    These tests are about the SHAPE of the log the loop is given, which is why
    they live next to `_signature` rather than in the synthesizer's tests.
    """

    def test_a_single_line_json_log_cannot_be_signed(self) -> None:
        """The old shape. Kept as the thing the fix has to avoid producing."""
        import json as _json

        from datasmith.agents.reflexive.loop import _signature

        one = _json.dumps({"stage": "build", "return_code": 1, "error_message": "gcc: fatal error"})
        two = _json.dumps({"stage": "build", "return_code": 1, "error_message": "pyproj wheel failed"})
        assert _signature(one) == _signature(two) == "no signature", (
            "if this ever stops holding, the fix below is no longer needed"
        )

    def test_indented_json_survives_the_brace_filter(self) -> None:
        """The fallback shape, for a failure with no captured stdout."""
        import json as _json

        from datasmith.agents.reflexive.loop import _signature

        one = _json.dumps({"stage": "build", "error_message": "gcc: fatal error, no input files"}, indent=2)
        two = _json.dumps({"stage": "build", "error_message": "pyproj wheel build failed"}, indent=2)
        assert _signature(one) != "no signature"
        assert _signature(one) != _signature(two), "two different failures must not collapse"

    def test_two_different_builds_get_different_progress_keys(self) -> None:
        """End to end over `progress_key`, which is what rule 3 compares."""
        import json as _json

        from datasmith.agents.reflexive.loop import progress_key
        from datasmith.agents.reflexive.schema import RejectionReport, Verdict
        from datasmith.agents.reflexive.severity import grade

        graded = grade(RejectionReport(verdict=Verdict.REJECT, mode="build_failed", checks=[]))

        def log_for(failure: dict, stdout: str) -> str:
            detail = _json.dumps(failure, indent=2) if failure else ""
            return f"{detail}\n{stdout}"[-200000:]

        first = log_for(
            {"stage": "build", "return_code": 1},
            "#15 8.6 ModuleNotFoundError: No module named 'salem'\n------",
        )
        second = log_for(
            {"stage": "build", "return_code": 1},
            "#15 9.1 error: subprocess-exited-with-error while building pyproj\n------",
        )
        assert progress_key(graded, first) != progress_key(graded, second)
        assert progress_key(graded, first) == progress_key(graded, first), "must stay deterministic"

    def test_the_synthesizer_no_longer_hands_the_loop_a_bare_json_dump(self) -> None:
        """The call site is where the shape is decided, so assert on it.

        A behavioural test would need a real container build. This checks the
        one line that caused the defect, and the accompanying behaviour tests
        above cover what the shape has to achieve.
        """
        import inspect

        from datasmith.agents.synthesizer import Synthesizer

        source = inspect.getsource(Synthesizer._run_produce_verify)
        assert "json.dumps(result.failure_json or {})[:200000]" not in source
        assert "result.agent_output" in source, "the real build log must reach the loop"


class TestEveryRoundLeavesATrace:
    """A three-round failure has to be readable without re-running the task.

    OGGM/oggm#1830 failed three rounds twice, and the only record was the
    progress key on the no-progress line. The build log lived in a
    TemporaryDirectory the run deleted, and the reflexive rounds write no
    `error_logs` row -- only TRY_DEFAULT does. Diagnosing it cost a full
    re-run each time, which at 100-container scale is not a cost anyone pays.
    """

    @staticmethod
    def _graded(hard: bool):
        from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Verdict
        from datasmith.agents.reflexive.severity import grade

        checks = (
            [CheckResult(id="asv_exec_failed", verdict="fail", cause=Cause.OTHER, evidence="e", remedy="")]
            if hard
            else []
        )
        return grade(RejectionReport(verdict=Verdict.REJECT, mode="build_failed", checks=checks))

    def test_a_failing_round_logs_the_build_log_and_the_reason(self, caplog) -> None:
        import logging
        from pathlib import Path

        from datasmith.agents.reflexive.loop import run_loop

        graded = self._graded(hard=True)
        # The sentinel must NOT be the line `_signature` picks, or the test
        # passes on the `key=` field alone and says nothing about the tail.
        # A named cause on the last line wins the signature, so the sentinel
        # above it can only reach the log through the build-log tail.
        logs = [
            "SENTINEL_ROUND_1_LOG_LINE\nModuleNotFoundError: No module named 'salem'",
            "SENTINEL_ROUND_2_LOG_LINE\nModuleNotFoundError: No module named 'pyproj'",
        ]
        seen = {"n": 0}

        def build(ctx):
            i = seen["n"]
            seen["n"] += 1
            return False, None, logs[min(i, len(logs) - 1)]

        with caplog.at_level(logging.INFO, logger="datasmith.agents.reflexive.loop"):
            outcome = run_loop(
                context=object(),
                build=build,
                verify=lambda image, log, mode: graded,
                revise=lambda ctx, g: (ctx, None),
                workdir=Path("."),
                max_rounds=2,
            )

        assert outcome.accepted is False
        text = caplog.text
        assert "salem" in text, "the signature must be logged"
        assert "SENTINEL_ROUND_1_LOG_LINE" in text, (
            "the build log TAIL must reach the operator, not just its one-line signature"
        )
        assert "asv_exec_failed" in text, "the hard failure must be named"
        assert "round 1/2" in text and "round 2/2" in text, "each round must be identifiable"


class TestTheFailureDetailIsSignableAsRealLines:
    """The second way the build log reached the loop unsignable.

    `json.dumps(x, indent=2)` indents the STRUCTURE but leaves newlines inside
    string VALUES escaped, so a whole build stdout stays on one line. When
    `agent_output` is empty -- which it is on the docker-build failure path --
    that line is the entire log, and `_signature` falls back to the generic
    wrapper for every failure alike.

    mars#3329: round 1 failed on a missing `pkg_resources`, round 2 on a PEP
    660 `build_editable` hook. Different failures, identical signatures, loop
    stopped for "no progress" at round 2.
    """

    @staticmethod
    def _failure(stdout: str) -> dict:
        return {
            "stage": "pkg",
            "return_code": 1,
            "error_message": "Build failed at stage 'pkg': Docker build failed (rc=1)",
            "stdout": TestTheFailureDetailIsSignableAsRealLines._PREAMBLE + stdout,
        }

    # A real build log, whose preamble is identical across rounds. The first
    # version of this fixture had no preamble and no embedded newlines, so
    # nothing collapsed and the test below failed -- correctly. The collapse
    # needs BOTH properties: escaped newlines put the log on one line, and a
    # shared preamble longer than `_signature`'s [:90] truncation makes the
    # prefixes equal.
    _PREAMBLE = (
        "#14 1.0 Collecting package metadata and building wheels for the project, "
        "resolving dependencies from the pinned env_payload\n"
    ) * 2

    def test_json_dumps_collapses_two_different_failures(self) -> None:
        """The shape being replaced. If this stops holding, the fix is moot."""
        import json as _json

        from datasmith.agents.reflexive.loop import _signature

        a = _json.dumps(self._failure("#14 2.9 ModuleNotFoundError: No module named 'pkg_resources'"), indent=2)
        b = _json.dumps(self._failure("#14 10.8 ERROR: missing the 'build_editable' hook"), indent=2)
        assert _signature(a) == _signature(b), "two different failures must collapse under the OLD shape"
        assert _signature(a).startswith('"stdout"'), "the signature is a prefix of the escaped log line"

    def test_the_real_formatter_keeps_them_apart(self) -> None:
        from datasmith.agents.reflexive.loop import _signature
        from datasmith.agents.synthesizer import _default_failure_message

        a = _default_failure_message(self._failure("#14 2.9 ModuleNotFoundError: No module named 'pkg_resources'"))
        b = _default_failure_message(self._failure("#14 10.8 ERROR: missing the 'build_editable' hook"))
        assert _signature(a) != _signature(b), "two different build failures must not compare equal"
        assert "pkg_resources" in _signature(a)

    def test_the_synthesizer_uses_the_real_formatter(self) -> None:
        import inspect

        from datasmith.agents.synthesizer import Synthesizer

        source = inspect.getsource(Synthesizer._run_produce_verify)
        assert "_default_failure_message(result.failure_json)" in source
        assert "json.dumps(result.failure_json, indent=2)" not in source


class TestTheSignatureIgnoresBuildKitTiming:
    """The bug that made stopping rule 3 unable to fire.

    BuildKit stamps every log line with elapsed seconds -- `#14 6.799 `,
    `25.05 <x> `. `_signature` branch 2 has always stripped that; branch 1 did
    not, and branch 1 is the common case because most real failures name a
    cause. So the signature carried a timestamp, two IDENTICAL failures never
    compared equal, and the no-progress rule could not fire on exactly the
    failures it existed to catch.

    Measured 2026-08-25: TileDB-Py#869 and satpy#2998 each spent all 8 rounds
    re-failing one wheel build, signing as `25.05 <x> Failed to build...`,
    `26.68 <x> Failed to build...`, `103.8 <x> Failed to build...`. It also
    means a `budget` stop was never evidence of progress, which is how it was
    read at first.
    """

    WHEEL_A = "25.05 \u00d7 Failed to build installable wheels for some pyproject.toml based projects"
    WHEEL_B = "103.8 \u00d7 Failed to build installable wheels for some pyproject.toml based projects"
    MOD_A = "#14 6.799   ModuleNotFoundError: No module named 'pkg_resources'"
    MOD_B = "#14 9.060   ModuleNotFoundError: No module named 'pkg_resources'"

    def test_the_same_wheel_failure_signs_identically(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        assert _signature(self.WHEEL_A) == _signature(self.WHEEL_B)

    def test_the_same_missing_module_signs_identically(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        assert _signature(self.MOD_A) == _signature(self.MOD_B)

    def test_no_timing_survives_into_the_signature(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        for line in (self.WHEEL_A, self.WHEEL_B, self.MOD_A, self.MOD_B):
            sig = _signature(line)
            assert not sig[:1].isdigit(), f"signature still starts with a timing stamp: {sig!r}"
            assert "#" not in sig[:4]

    def test_different_failures_still_differ(self) -> None:
        """Stripping must not flatten genuinely distinct causes together."""
        from datasmith.agents.reflexive.loop import _signature

        other = "#14 6.024   ModuleNotFoundError: No module named 'skbuild'"
        assert _signature(self.MOD_A) != _signature(other)
        assert _signature(self.MOD_A) != _signature(self.WHEEL_A)

    def test_rule_three_can_now_fire_on_a_repeated_failure(self) -> None:
        """End to end over progress_key, which is what the rule compares."""
        from datasmith.agents.reflexive.loop import progress_key
        from datasmith.agents.reflexive.schema import RejectionReport, Verdict
        from datasmith.agents.reflexive.severity import grade

        graded = grade(RejectionReport(verdict=Verdict.REJECT, mode="build_failed", checks=[]))
        assert progress_key(graded, self.WHEEL_A) == progress_key(graded, self.WHEEL_B)

    def test_both_implementations_strip_the_same_way(self) -> None:
        """loop.py and scripts/prepass_trial.py must not drift on this."""
        import importlib.util
        import sys
        from pathlib import Path as _P

        from datasmith.agents.reflexive.loop import _strip_buildkit_prefix as loop_strip

        root = _P(__file__).parents[3]
        spec = importlib.util.spec_from_file_location("_pp_strip", root / "scripts" / "prepass_trial.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_pp_strip"] = mod
        spec.loader.exec_module(mod)
        for line in (self.WHEEL_A, self.MOD_A, "2.15 BackendUnavailable: Cannot import 'hatchling.build'", "plain"):
            assert loop_strip(line) == mod._strip_buildkit_prefix(line), f"drifted on {line!r}"


class TestTheStageOnlySummaryIsNotASignature:
    """`Build failed at stage 'pkg': Docker build failed (rc=1)` names a stage
    and nothing else.

    It is the last line local_ci.py prints, so a reverse scan lands on it for
    any failure without a named cause. Measured across the 2026-08-25 grind, it
    covered 10 rounds and its `'env'` sibling another 9 -- so two genuinely
    different failures at the same stage compared EQUAL and the no-progress
    rule stopped loops that were still making progress. The real build output
    is rendered into the log ahead of it from `failure.json`.
    """

    def test_two_different_failures_at_one_stage_sign_differently(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        a = "12.0 error: could not find a working compiler\nBuild failed at stage 'pkg': Docker build failed (rc=1)"
        b = "9.0 error: hdf5 headers are missing\nBuild failed at stage 'pkg': Docker build failed (rc=1)"
        assert _signature(a) != _signature(b), "the stall detector cannot tell these apart"

    def test_the_summary_itself_is_never_the_signature(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        log = "4.2 error: hdf5 headers are missing\nBuild failed at stage 'env': Docker build failed (rc=1)"
        assert "Build failed at stage" not in _signature(log)

    def test_the_same_failure_still_signs_the_same(self) -> None:
        """Rule 3 must still fire when nothing actually changed."""
        from datasmith.agents.reflexive.loop import _signature

        a = "12.0 error: could not find a working compiler\nBuild failed at stage 'pkg': Docker build failed (rc=1)"
        b = "88.7 error: could not find a working compiler\nBuild failed at stage 'pkg': Docker build failed (rc=1)"
        assert _signature(a) == _signature(b), "elapsed seconds must not make two identical failures differ"


class TestTheStagePrefixedSummaryIsAlsoNoise:
    """`_default_failure_message` renders the summary as `"pkg: Build failed at
    stage 'pkg': ..."`.

    `_NOISE` is a startswith test, so it catches the bare form on stdout and
    misses this one. A stage-prefixed summary naming no cause is exactly as
    useless a signature as the bare one, and it is what survives when the raw
    build log is empty.
    """

    def test_the_stage_prefixed_form_is_skipped(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        assert _signature("pkg: Build failed at stage 'pkg': Docker build failed (rc=1)") == "no signature"

    def test_two_stages_do_not_masquerade_as_different_failures(self) -> None:
        """Without this they differ only by the stage word, which invents
        progress where there was none."""
        from datasmith.agents.reflexive.loop import _signature

        a = _signature("pkg: Build failed at stage 'pkg': Docker build failed (rc=1)")
        b = _signature("env: Build failed at stage 'env': Docker build failed (rc=1)")
        assert a == b == "no signature"

    def test_a_real_cause_still_wins_over_the_summary(self) -> None:
        from datasmith.agents.reflexive.loop import _signature

        log = "9.2 error: no C compiler found\npkg: Build failed at stage 'pkg': Docker build failed (rc=1)"
        assert _signature(log) == "error: no C compiler found"
