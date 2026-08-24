"""Severity is decided HERE, in our code, never in the verifier's prompt.

This is the structural answer to the standing objection that a Turing-complete
agent always defeats a verifier. The agent does not get to decide the hard set.
It reports a fact and this module grades it.
"""

from __future__ import annotations

import pytest

from datasmith.agents.reflexive.schema import Cause, CheckResult, RejectionReport, Severity, Verdict
from datasmith.agents.reflexive.severity import classify, grade


def _check(cid: str, cause: Cause, evidence_override: str = "e", **kw) -> CheckResult:
    return CheckResult(id=cid, verdict="fail", cause=cause, evidence=evidence_override, remedy="r", **kw)


class TestClassify:
    def test_a_module_that_is_not_installed_is_hard(self) -> None:
        assert classify("pytest_collect", Cause.MODULE_NOT_FOUND) is Severity.HARD

    def test_an_installed_module_raising_on_a_host_facility_is_soft(self) -> None:
        """cupy installs cleanly and still raises with no CUDA driver."""
        assert classify("pytest_collect", Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.SOFT

    def test_a_pytest_version_incompatibility_is_hard(self) -> None:
        """aicsimageio and datashader. The remedy is to pin pytest."""
        assert classify("pytest_collect", Cause.PYTEST_VERSION_INCOMPAT) is Severity.HARD

    @pytest.mark.parametrize(
        "check_id",
        [
            "tamper_audit",
            "asv_discovers_zero_against_source",
            "measure_timed_out",
            "asv_exec_failed",
            "oracle_patch_failed",
            "numpy_moved_major_minor",
        ],
    )
    def test_the_always_hard_ids_ignore_the_cause(self, check_id: str) -> None:
        """No cause can soften these. Not even one we do not model."""
        assert classify(check_id, Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.HARD

    def test_the_pass_ratio_check_is_soft(self) -> None:
        """fluids at 554/559 is the motivating case."""
        assert classify("pytest_pass_ratio", Cause.OTHER) is Severity.SOFT

    def test_an_unknown_check_id_defaults_to_hard(self) -> None:
        """Fail closed. An unrecognised check must not be waivable."""
        assert classify("something_invented_by_the_agent", Cause.OTHER) is Severity.HARD

    def test_an_invented_id_stays_hard_even_with_a_softening_cause(self) -> None:
        """The seam an earlier draft left open.

        Letting the CAUSE soften any unrecognised id relocates the cheating
        concern from the producer to a persuaded verifier -- which is worse,
        because the verifier is the thing we were trusting.
        """
        assert classify("anything_the_agent_invented", Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.HARD

    def test_only_enumerated_ids_are_cause_discriminated(self) -> None:
        for check_id in ("pytest_collect", "import_sweep"):
            assert classify(check_id, Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.SOFT
            assert classify(check_id, Cause.MODULE_NOT_FOUND) is Severity.HARD

    def test_a_host_facility_claim_refuted_by_its_own_evidence_is_hard(self) -> None:
        """Nothing installed cannot have raised for want of a host facility."""
        assert (
            classify(
                "pytest_collect", Cause.IMPORT_RAISED_ON_HOST_FACILITY, "ModuleNotFoundError: No module named 'cupy'"
            )
            is Severity.HARD
        )

    def test_a_genuine_host_facility_claim_survives(self) -> None:
        """cupy installed, CUDA driver absent. The real waiver case."""
        assert (
            classify(
                "import_sweep",
                Cause.IMPORT_RAISED_ON_HOST_FACILITY,
                "ImportError: libcuda.so.1: cannot open shared object file",
            )
            is Severity.SOFT
        )


class TestGrade:
    def test_a_clean_report_is_accepted(self) -> None:
        report = RejectionReport(verdict=Verdict.ACCEPT, mode="container_built", checks=[])
        assert grade(report).accepted is True

    def test_one_hard_failure_rejects(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,  # the agent said accept...
            mode="container_built",
            checks=[_check("pytest_collect", Cause.PYTEST_VERSION_INCOMPAT)],
        )
        graded = grade(report)
        assert graded.accepted is False, "our grading overrides the agent's verdict"
        assert graded.hard_failures == ("pytest_collect",)

    def test_a_soft_failure_alone_is_accepted(self) -> None:
        report = RejectionReport(
            verdict=Verdict.REJECT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER, waiver_reason="5 numba failures, commit-invariant")],
        )
        graded = grade(report)
        assert graded.accepted is True
        assert graded.soft_failures == ("pytest_pass_ratio",)

    def test_a_soft_failure_with_no_waiver_reason_rejects(self) -> None:
        """A waiver must be argued, not merely asserted."""
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER)],
        )
        assert grade(report).accepted is False

    def test_a_self_contradicting_waiver_is_recorded_as_a_violation(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[
                CheckResult(
                    id="pytest_collect",
                    verdict="fail",
                    cause=Cause.IMPORT_RAISED_ON_HOST_FACILITY,
                    evidence="ModuleNotFoundError: No module named 'salem'",
                    remedy="r",
                    waiver_reason="no GPU on this host",
                )
            ],
        )
        graded = grade(report)
        assert graded.accepted is False
        assert graded.violations == ("pytest_collect",)

    def test_waiving_a_hard_check_is_ignored_and_logged(self) -> None:
        """The spec's core promise. An agent that waives a hard check fails."""
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[
                _check(
                    "tamper_audit",
                    Cause.IMPORT_RAISED_ON_HOST_FACILITY,
                    severity=Severity.SOFT,  # the agent claims soft
                    waiver_reason="looks fine to me",
                )
            ],
        )
        graded = grade(report)
        assert graded.accepted is False
        assert graded.hard_failures == ("tamper_audit",)
        assert graded.violations == ("tamper_audit",), "the attempted waiver must be recorded"

    def test_a_passing_check_does_not_count_as_a_failure(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[CheckResult(id="x", verdict="pass", cause=Cause.OTHER, evidence="e", remedy="")],
        )
        graded = grade(report)
        assert graded.accepted is True
        assert graded.hard_failures == ()


class TestSeamsFoundByAudit:
    """Three weaknesses in the plan's own code, found auditing Task 2.

    The plan is the authority for what to build, not for whether it is right.
    All three sit in the module whose entire job is to be unbypassable.
    """

    def test_a_whitespace_only_waiver_does_not_count_as_argued(self) -> None:
        """`grade` promised a waiver must be ARGUED, not merely asserted.

        It enforced that with truthiness, so "   " passed and the container
        was accepted on a waiver that says nothing.
        """
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER, waiver_reason="   ")],
        )
        assert grade(report).accepted is False

    def test_a_real_waiver_still_counts(self) -> None:
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[_check("pytest_pass_ratio", Cause.OTHER, waiver_reason="5 numba failures, commit-invariant")],
        )
        assert grade(report).accepted is True

    def test_a_smoke_test_failing_on_a_missing_module_is_hard(self) -> None:
        """`repo_smoke_test` was unconditionally SOFT.

        That is a broader waiver than pytest_collect or import_sweep get. A
        smoke test that fails because nothing is installed is a missing
        dependency, and the producer can fix it.
        """
        assert classify("repo_smoke_test", Cause.MODULE_NOT_FOUND) is Severity.HARD

    def test_a_smoke_test_failing_on_a_host_facility_stays_soft(self) -> None:
        assert classify("repo_smoke_test", Cause.IMPORT_RAISED_ON_HOST_FACILITY) is Severity.SOFT

    def test_a_violation_is_recorded_once_not_twice(self) -> None:
        """A check that both contradicts its evidence AND falsely claims SOFT
        appended its id twice, so a caller counting violations double-counted."""
        report = RejectionReport(
            verdict=Verdict.ACCEPT,
            mode="container_built",
            checks=[
                _check(
                    "tamper_audit",
                    Cause.IMPORT_RAISED_ON_HOST_FACILITY,
                    evidence_override="ModuleNotFoundError: No module named 'x'",
                    severity=Severity.SOFT,
                    waiver_reason="no GPU",
                )
            ],
        )
        graded = grade(report)
        assert graded.violations == ("tamper_audit",)
        assert graded.accepted is False
