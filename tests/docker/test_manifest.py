"""Tests for datasmith.docker.manifest — invariant evaluation."""

from datasmith.docker.manifest import evaluate_invariants

_LOCAL_CI_PATH = None  # set lazily by _local_ci_path() below


def _local_ci_path():
    global _LOCAL_CI_PATH
    if _LOCAL_CI_PATH is None:
        from pathlib import Path

        _LOCAL_CI_PATH = Path(__file__).parents[2] / "src" / "datasmith" / "agents" / "templates" / "local_ci.py"
    return _LOCAL_CI_PATH


def _load_local_ci_module():
    """Dynamically load local_ci.py by its file path rather than importing
    it as a package module (it isn't one -- it's copied standalone into the
    agent sandbox and has no ``datasmith`` package around it there either).

    Its one third-party dependency, ``python_on_whales``, must be installed
    in this venv for the load to succeed; it already is, since local_ci.py
    itself imports it unconditionally.
    """
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("_local_ci_under_test", _local_ci_path())
    module = importlib.util.module_from_spec(spec)
    # Register before exec: local_ci.py uses `from __future__ import
    # annotations`, and its @dataclass Task resolves string annotations via
    # sys.modules[cls.__module__] at class-definition time -- without this,
    # that lookup returns None and dataclass() crashes.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _good_manifest() -> dict:
    """A manifest where every invariant passes."""
    return {
        "schema_version": 1,
        "build": {
            "owner": "pvlib",
            "repo": "pvlib-python",
            "issue_number": 369,
            "declared_commit": "c8b8086",
            "head_at_seal": "c8b8086",
            "image_digest": "sha256:abc",
            "lsv_sha": "fc16ba4",
            "reward_formula_id": "case3-unclamped-v1",
            "benchmark_dest": "benchmarks/benchmark_clearsky.py",
            "benchmark_dir": "/workspace/repo/benchmarks",
            "benchmark_dir_init_present": True,
            "benchmark_dest_present_post_clean": True,
            "discovered_n": 3,
            "expected_n": None,
            "discovery_fallback_used": False,
            "pins_requested": ["scipy<=1.10"],
            "pins_resolved": ["scipy==1.10.1"],
            "cpu_cap": 4,
            "nproc": 128,
            "rounds": 5,
            "secrets_scan_clean": True,
        },
        "verify": {
            "test_duration_s": 412.6,
            "test_timed_out": False,
            "timeout_s": 3600,
            "pytest_collect_ok": True,
            "pytest_failed_at_base": 0,
            "measure_ran": True,
            "measure_timed_out": False,
            "measure_timeout_s": 3600,
            "measure_duration_s": 812.4,
            "measure_error": None,
            "base_sha_measured": "c8b8086",
            "patch_present": True,
            "patch_applied": True,
            "patch_files_changed": 7,
            "patch_paths_excluded": 0,
            "benchmarks_impactable_n": 140,
            "benchmarks_measured_n": 12,
            "benchmarks_degenerate_n": 0,
            "geomean_speedup": 1.34,
            "max_speedup": 2.10,
        },
    }


class TestEvaluateInvariants:
    def test_clean_manifest_passes(self):
        report = evaluate_invariants(_good_manifest())
        assert report.ok is True
        assert report.fatal == []

    def test_timeout_is_fatal(self):
        m = _good_manifest()
        m["verify"]["test_timed_out"] = True
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "test_timed_out" in report.fatal

    def test_zero_benchmarks_is_fatal(self):
        m = _good_manifest()
        m["build"]["discovered_n"] = 0
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "discovered_n_zero" in report.fatal

    def test_head_drift_is_fatal(self):
        m = _good_manifest()
        m["build"]["head_at_seal"] = "deadbee"
        report = evaluate_invariants(m)
        assert "head_commit_drift" in report.fatal

    def test_fallback_is_warning_not_fatal(self):
        m = _good_manifest()
        m["build"]["discovery_fallback_used"] = True
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "discovery_fallback_used" in report.warnings

    def test_cpu_cap_unset_warns(self):
        m = _good_manifest()
        m["build"]["cpu_cap"] = None
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "cpu_cap_unset" in report.warnings

    def test_expected_n_absent_skips_dilution_check(self):
        """#12 is deferred: no expected_n source exists in this tree."""
        m = _good_manifest()
        m["build"]["expected_n"] = None
        m["build"]["discovered_n"] = 500
        report = evaluate_invariants(m)
        assert "dilution_ratio" in report.skipped
        assert "dilution_ratio" not in report.warnings

    def test_dilution_warns_when_expected_n_present(self):
        m = _good_manifest()
        m["build"]["expected_n"] = 3
        m["build"]["discovered_n"] = 140
        report = evaluate_invariants(m)
        assert "dilution_ratio" in report.warnings

    def test_missing_manifest_is_not_assessable(self):
        report = evaluate_invariants(None)
        assert report.ok is None
        assert report.fatal == []
        assert "test_timed_out" in report.skipped

    def test_missing_verify_block_skips_runtime_invariants(self):
        """A pulled image that has never been run has build facts only."""
        m = _good_manifest()
        del m["verify"]
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "test_timed_out" in report.skipped
        assert "discovered_n_zero" not in report.skipped

    # ── Additional coverage for untested fatal invariants ──

    def test_benchmark_dest_missing_is_fatal(self):
        m = _good_manifest()
        m["build"]["benchmark_dest_present_post_clean"] = False
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "benchmark_dest_missing" in report.fatal

    def test_benchmark_init_missing_is_warning(self):
        """warn, not fatal: see the comment on this Invariant in manifest.py.
        Build-time this check is either a false-positive gate (repos that
        legitimately lack __init__.py) or tautological (if we create it
        unconditionally); the meaningful comparison is trial-time against
        the sealed value, which lands in a later plan."""
        m = _good_manifest()
        m["build"]["benchmark_dir_init_present"] = False
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "benchmark_init_missing" in report.warnings
        assert "benchmark_init_missing" not in report.fatal

    def test_secrets_present_is_fatal(self):
        m = _good_manifest()
        m["build"]["secrets_scan_clean"] = False
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "secrets_present" in report.fatal

    def test_pytest_collect_failed_is_warning(self):
        """Downgraded from fatal: pytest_runner.py's summary["error"] mixes
        genuine collection failures with ordinary per-test setup/teardown
        errors, and only the flat summary reaches local_ci.py -- see the
        comment on this Invariant in manifest.py. Returns to fatal once
        len(results["errors"]) is read from /logs/test_results.json."""
        m = _good_manifest()
        m["verify"]["pytest_collect_ok"] = False
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "pytest_collect_failed" in report.warnings
        assert "pytest_collect_failed" not in report.fatal

    # ── Additional coverage for untested warning invariants ──

    def test_pins_drift_warns_when_missing_from_resolved(self):
        """Requested package not in resolved set is a warning."""
        m = _good_manifest()
        m["build"]["pins_requested"] = ["scipy<=1.10"]
        m["build"]["pins_resolved"] = ["numpy==1.24.0"]
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "pins_drift" in report.warnings

    def test_pins_passes_when_in_resolved(self):
        """All requested packages appear in resolved set."""
        m = _good_manifest()
        m["build"]["pins_requested"] = ["scipy<=1.10"]
        m["build"]["pins_resolved"] = ["scipy==1.10.1"]
        report = evaluate_invariants(m)
        assert "pins_drift" not in report.warnings
        assert "pins_drift" not in report.fatal

    def test_base_tests_failing_is_warning(self):
        m = _good_manifest()
        m["verify"]["pytest_failed_at_base"] = 1
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "base_tests_failing" in report.warnings

    # ── Tests for deferred invariants (now skipped, not warned) ──

    def test_image_identity_both_absent_skips(self):
        """When both image_digest and lsv_sha are absent, skip."""
        m = _good_manifest()
        del m["build"]["image_digest"]
        del m["build"]["lsv_sha"]
        report = evaluate_invariants(m)
        assert "image_identity_missing" in report.skipped
        assert "image_identity_missing" not in report.warnings

    def test_image_identity_one_present_warns(self):
        """When only one of image_digest or lsv_sha is present, warn."""
        m = _good_manifest()
        del m["build"]["lsv_sha"]
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "image_identity_missing" in report.warnings

    def test_reward_formula_absent_skips(self):
        """When reward_formula_id is absent, skip (deferred check)."""
        m = _good_manifest()
        del m["build"]["reward_formula_id"]
        report = evaluate_invariants(m)
        assert "reward_formula_unknown" in report.skipped
        assert "reward_formula_unknown" not in report.warnings

    def test_reward_formula_present_skips(self):
        """When reward_formula_id is present, still skip (deferred comparison)."""
        m = _good_manifest()
        m["build"]["reward_formula_id"] = "case3-unclamped-v1"
        report = evaluate_invariants(m)
        assert "reward_formula_unknown" in report.skipped
        assert "reward_formula_unknown" not in report.warnings

    # ── manifest_empty: distinguishes "breadcrumbs never reached the sealer"
    # from "invariants legitimately skipped" ──

    def test_manifest_empty_warns_when_build_block_is_all_null(self):
        """Every fc_note call site fell back to a no-op (pre-manifest base
        image): the build block comes out all-null and manifest_empty must
        fire so it isn't mistaken for a healthy build with nothing to check."""
        m = {
            "build": {
                "owner": None,
                "repo": None,
                "declared_commit": None,
                "head_at_seal": None,
                "nproc": None,
                "discovered_n": None,
                "benchmark_dest_present_post_clean": None,
                "secrets_scan_clean": None,
                "pins_requested": None,
            },
            "verify": {},
        }
        report = evaluate_invariants(m)
        assert report.ok is True  # warn, not fatal
        assert "manifest_empty" in report.warnings

    def test_manifest_empty_does_not_fire_with_one_breadcrumb_value(self):
        """A single breadcrumb-sourced value is enough to prove fc_note ran."""
        m = {
            "build": {
                "declared_commit": None,
                "head_at_seal": None,
                "nproc": None,
                "discovered_n": 3,
            },
            "verify": {},
        }
        report = evaluate_invariants(m)
        assert "manifest_empty" not in report.warnings
        assert "manifest_empty" not in report.fatal

    def test_manifest_empty_not_fooled_by_falsy_but_present_zero(self):
        """discovered_n=0 and secrets_scan_clean=False are meaningful, present
        values -- a future `any(b.values())` refactor would treat them as
        absent and warn spuriously. Pin the `is not None` semantics."""
        m = {
            "build": {
                "declared_commit": None,
                "head_at_seal": None,
                "nproc": None,
                "discovered_n": 0,
            },
            "verify": {},
        }
        report = evaluate_invariants(m)
        assert "manifest_empty" not in report.warnings

    def test_manifest_empty_fires_when_only_introspected_fields_present(self):
        """The exact broken state: declared_commit (written directly by
        Dockerfile.pr) and head_at_seal/nproc (introspected by
        emit_manifest.py) are populated even when fc_note never fires, so
        they must not count as evidence breadcrumbs reached the sealer."""
        m = {
            "build": {
                "declared_commit": "c8b8086",
                "head_at_seal": "c8b8086",
                "nproc": 8,
            },
            "verify": {},
        }
        report = evaluate_invariants(m)
        assert report.ok is True
        assert "manifest_empty" in report.warnings

    def test_manifest_empty_skips_when_no_build_block_at_all(self):
        m = {"verify": {"test_timed_out": False}}
        report = evaluate_invariants(m)
        assert "manifest_empty" in report.skipped
        assert "manifest_empty" not in report.warnings

    def test_manifest_empty_clean_on_good_manifest(self):
        report = evaluate_invariants(_good_manifest())
        assert "manifest_empty" not in report.warnings


class TestLocalCiSync:
    """local_ci.py runs in the sandbox without datasmith installed, so it
    carries its own copy of the evaluator.  These guard the duplication."""

    def _local_ci_source(self) -> str:
        from pathlib import Path

        return (Path(__file__).parents[2] / "src" / "datasmith" / "agents" / "templates" / "local_ci.py").read_text()

    def test_timeout_is_no_longer_treated_as_success(self):
        src = self._local_ci_source()
        assert "treated as success" not in src

    def test_timeout_default_is_configurable(self):
        src = self._local_ci_source()
        assert "DATASMITH_VERIFY_TEST_TIMEOUT_S" in src
        assert '"3600"' in src

    def test_measure_timeout_default_is_configurable(self):
        src = self._local_ci_source()
        assert "DATASMITH_VERIFY_MEASURE_TIMEOUT_S" in src
        assert '"3600"' in src

    def test_measure_step_runs_after_tests_pass(self):
        """Ordering guard: measure must not run on a container whose pytest
        already failed -- that would burn ~14 minutes per doomed attempt.

        Scoped to verify()'s body, not the whole module: `def run_measure(`
        is defined above verify(), so a whole-file index comparison would
        pass vacuously regardless of call order.
        """
        src = self._local_ci_source()
        body = src.split("def verify(task_dir: Path) -> bool:", 1)[1]
        assert "run_measure(" in body, "verify() never calls run_measure"
        assert body.index("run_tests(") < body.index("run_measure(")
        # and the tests-failed early return must come before the measure call
        assert body.index('_write_failure(task_dir, "tests"') < body.index("run_measure(")

    def test_every_fatal_invariant_id_is_known_to_local_ci(self):
        from datasmith.docker.manifest import INVARIANTS

        src = self._local_ci_source()
        for inv in INVARIANTS:
            if inv.severity == "fatal":
                assert inv.id in src, f"local_ci.py is missing fatal invariant {inv.id}"

    def test_fatal_checks_agree_with_manifest_module(self):
        """Behavioral parity, not just id coverage.

        The duplication between manifest.py and local_ci.py is deliberate
        (local_ci runs in a sandbox without datasmith installed). This test
        is what makes that duplication safe: it fails if either copy's
        SEMANTICS drift -- e.g. one comparison operator flipped, or one
        check missing a None-when-absent branch the other has -- which an
        id-substring check (test_every_fatal_invariant_id_is_known_to_local_ci
        above) cannot detect, because the id string stays present either way.

        Manually verified this has teeth: temporarily changed local_ci.py's
        _c_discovered_n from ``b["discovered_n"] > 0`` to
        ``b["discovered_n"] >= 0`` and reran this test alone -- it failed
        (drift on the "discovered_n_zero"-violated case, where manifest.py
        says fatal but the mutated local_ci.py says clean). Reverted and
        reran -- passed again. See task-5-report.md for the exact commands
        and output.
        """
        import copy

        from datasmith.docker.manifest import evaluate_invariants

        local_ci = _load_local_ci_module()
        good = _good_manifest()

        # (case, invariant id the case is meant to trip -- None for the
        # three baseline cases, which must stay clean). Pairing the id with
        # its case (rather than parallel lists) keeps them from silently
        # drifting apart under reordering or edits.
        cases: list[tuple[dict, str | None]] = [
            ({}, None),  # no manifest at all
            ({"build": {}, "verify": {}}, None),  # manifest present, everything absent
            (good, None),  # everything passing
        ]

        # One case per fatal invariant (currently five -- see
        # datasmith.docker.manifest.INVARIANTS), each violated in isolation.
        for build_key, build_value, inv_id in (
            ("discovered_n", 0, "discovered_n_zero"),
            ("benchmark_dest_present_post_clean", False, "benchmark_dest_missing"),
            ("head_at_seal", "deadbeef0000", "head_commit_drift"),
            ("secrets_scan_clean", False, "secrets_present"),
        ):
            m = copy.deepcopy(good)
            m["build"][build_key] = build_value
            cases.append((m, inv_id))

        for verify_key, verify_value, inv_id in (
            ("test_timed_out", True, "test_timed_out"),
            ("measure_timed_out", True, "measure_timed_out"),
            ("benchmarks_measured_n", 0, "asv_exec_failed"),
            ("patch_applied", False, "oracle_patch_failed"),
        ):
            m = copy.deepcopy(good)
            m["verify"][verify_key] = verify_value
            cases.append((m, inv_id))

        # benchmark_init_missing is warn-severity, not fatal (see the
        # comment on that Invariant in manifest.py) -- this case proves
        # BOTH implementations agree it produces zero fatal violations, so
        # a future accidental re-promotion to fatal on only one side is
        # still caught by the parity assertion below.
        m = copy.deepcopy(good)
        m["build"]["benchmark_dir_init_present"] = False
        cases.append((m, None))

        # pytest_collect_failed is warn-severity, not fatal (downgraded --
        # see the comment on that Invariant in manifest.py and on _c_collect
        # in local_ci.py) -- same proof-of-agreement pattern as
        # benchmark_init_missing above.
        m = copy.deepcopy(good)
        m["verify"]["pytest_collect_ok"] = False
        cases.append((m, None))

        # Every fatal invariant must have a case above. Without this, adding
        # a fatal gate with no parity case leaves its semantics unguarded and
        # the whole test passes anyway -- which is exactly what happened when
        # the three measurability gates first landed.
        from datasmith.docker.manifest import INVARIANTS

        covered = {want_id for _case, want_id in cases if want_id is not None}
        all_fatal = {inv.id for inv in INVARIANTS if inv.severity == "fatal"}
        assert all_fatal <= covered, (
            f"fatal invariants with no parity case: {sorted(all_fatal - covered)}. "
            "Add a case that violates each in isolation, or drift in it goes undetected."
        )

        for case, want_id in cases:
            manifest_fatal = evaluate_invariants(case or None).fatal

            # Self-check: a single-flip case that fails to actually trip its
            # intended invariant would make the parity assertion below pass
            # vacuously (both implementations agreeing on "nothing violated"
            # proves nothing about the flipped field). A baseline case
            # (want_id is None) must stay clean instead.
            if want_id is None:
                assert manifest_fatal == [], f"expected a clean baseline case, manifest.py found {manifest_fatal}"
            else:
                assert want_id in manifest_fatal, (
                    f"case meant to trip {want_id!r} didn't -- manifest.py reports {manifest_fatal}; "
                    "fix the case, this assertion isn't testing what it claims to"
                )

            expected = set(manifest_fatal)
            actual = set(local_ci.check_fatal_invariants(case or None))
            assert actual == expected, f"drift on {case!r}: local_ci={actual} manifest.py={expected}"


class TestLocalCiPytestSummaryParsing:
    """local_ci.py's verify() derives verify.pytest_collect_ok /
    verify.pytest_failed_at_base by parsing the FORMULACODE_TESTS_START /
    FORMULACODE_TESTS_END block out of run_tests()'s captured stdout. These
    guard that parser against the actual shapes run-tests.sh /
    pytest_runner.py can produce, and against nothing-there / garbage input.
    """

    def _module(self):
        return _load_local_ci_module()

    def _wrap(self, body: str) -> str:
        return f"noise before\nFORMULACODE_TESTS_START\n{body}\nFORMULACODE_TESTS_END\nnoise after\n"

    # ── the normal pytest-run shape (pytest_runner.py's flat summary) ──

    def test_normal_summary_with_no_errors_or_failures(self):
        local_ci = self._module()
        stdout = self._wrap(
            '{"error": 0, "failed": 0, "passed": 12, "rerun": 0, '
            '"skipped": 1, "total": 13, "warnings": 0, "xfailed": 0, "xpassed": 0}'
        )
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {
            "pytest_collect_ok": True,
            "pytest_failed_at_base": 0,
            "pytest_total_at_base": 13,
            "pytest_passed_at_base": 12,
            "pytest_pass_ratio": 0.923077,
        }

    def test_normal_summary_with_failures_but_no_collection_errors(self):
        """Failed tests alone (joblib's preexisting pytest-8 failures, per
        the design doc) must not flip pytest_collect_ok -- that's a
        separate, warn-severity invariant."""
        local_ci = self._module()
        stdout = self._wrap(
            '{"error": 0, "failed": 3, "passed": 9, "rerun": 0, '
            '"skipped": 1, "total": 13, "warnings": 0, "xfailed": 0, "xpassed": 0}'
        )
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {
            "pytest_collect_ok": True,
            "pytest_failed_at_base": 3,
            "pytest_total_at_base": 13,
            "pytest_passed_at_base": 9,
            "pytest_pass_ratio": 0.692308,
        }

    def test_collection_error_flips_collect_ok_false(self):
        local_ci = self._module()
        stdout = self._wrap(
            '{"error": 1, "failed": 0, "passed": 0, "rerun": 0, '
            '"skipped": 0, "total": 0, "warnings": 0, "xfailed": 0, "xpassed": 0}'
        )
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {
            "pytest_collect_ok": False,
            "pytest_failed_at_base": 0,
            "pytest_total_at_base": 0,
            "pytest_passed_at_base": 0,
        }

    # ── the no-ASV-benchmarks early-exit shape (hardcoded in run-tests.sh) ──

    def test_no_benchmarks_early_exit_shape(self):
        local_ci = self._module()
        stdout = self._wrap('{"total": 0, "passed": 0, "failed": 0, "error": 0, "skipped": 0}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {
            "pytest_collect_ok": True,
            "pytest_failed_at_base": 0,
            "pytest_total_at_base": 0,
            "pytest_passed_at_base": 0,
        }
        assert "pytest_pass_ratio" not in fields, "0/0 has no ratio; it must be absent, not 0.0"

    # ── the run_pytest=False template shape: no failed/error keys at all ──

    def test_results_wrapper_shape_yields_no_fields(self):
        local_ci = self._module()
        stdout = self._wrap('{"results": {"exit_code": 0, "details": "Tests skipped as per configuration."}}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {}

    # ── defensiveness: absent / malformed input must never raise or guess ──

    def test_missing_block_yields_no_fields(self):
        local_ci = self._module()
        fields = local_ci._pytest_verify_fields("some unrelated container output\nno markers here\n")
        assert fields == {}

    def test_malformed_json_yields_no_fields(self):
        local_ci = self._module()
        stdout = self._wrap("{not valid json,,,")
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {}

    def test_non_object_json_yields_no_fields(self):
        local_ci = self._module()
        stdout = self._wrap("[1, 2, 3]")
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {}

    def test_non_numeric_error_and_failed_are_omitted_not_raised(self):
        local_ci = self._module()
        stdout = self._wrap('{"error": "oops", "failed": null}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {}

    def test_empty_stdout_yields_no_fields(self):
        local_ci = self._module()
        assert local_ci._pytest_verify_fields("") == {}


class TestMeasurabilityInvariants:
    """Each invariant gets three cases: fires, skips, holds.

    'Skips' is the one that matters most — the previous run shipped three
    gates that compared against values nothing emitted, and a skip that is
    indistinguishable from a pass is exactly that defect.
    """

    def _report(self, verify: dict, build: dict | None = None):
        from datasmith.docker.manifest import evaluate_invariants

        return evaluate_invariants({
            "schema_version": 1,
            "build": build if build is not None else {"discovered_n": 3},
            "verify": verify,
        })

    # ── measure_timed_out (fatal) ──
    def test_measure_timeout_fires(self):
        assert "measure_timed_out" in self._report({"measure_timed_out": True}).fatal

    def test_measure_timeout_holds(self):
        assert "measure_timed_out" not in self._report({"measure_timed_out": False}).fatal

    def test_measure_timeout_skips_when_absent(self):
        r = self._report({})
        assert "measure_timed_out" in r.skipped
        assert "measure_timed_out" not in r.fatal

    # ── asv_exec_failed (fatal) — the core gate ──
    def test_asv_exec_fires_on_zero_measured(self):
        assert "asv_exec_failed" in self._report({"benchmarks_measured_n": 0}).fatal

    def test_asv_exec_holds_on_one_measured(self):
        assert "asv_exec_failed" not in self._report({"benchmarks_measured_n": 1}).fatal

    def test_asv_exec_skips_when_absent(self):
        r = self._report({})
        assert "asv_exec_failed" in r.skipped
        assert "asv_exec_failed" not in r.fatal

    # ── oracle_patch_failed (fatal) ──
    def test_patch_failure_fires_when_present_but_unapplied(self):
        r = self._report({"patch_present": True, "patch_applied": False})
        assert "oracle_patch_failed" in r.fatal

    def test_patch_failure_holds_when_applied(self):
        r = self._report({"patch_present": True, "patch_applied": True})
        assert "oracle_patch_failed" not in r.fatal

    def test_patch_failure_skips_when_no_patch_exists(self):
        """6 of 12941 perf PRs have an empty patch. Absence is a skip, not a
        failure — flipping a gate to fatal against a sometimes-absent input
        is how the 720s timeout would have become a 34% hard-fail."""
        r = self._report({"patch_present": False, "patch_applied": False})
        assert "oracle_patch_failed" in r.skipped
        assert "oracle_patch_failed" not in r.fatal

    def test_patch_failure_skips_when_key_absent(self):
        assert "oracle_patch_failed" in self._report({}).skipped

    # ── speedup_direction (warn) ──
    def test_direction_warns_on_slowdown(self):
        assert "speedup_direction" in self._report({"geomean_speedup": 0.63}).warnings

    def test_direction_holds_at_parity(self):
        assert "speedup_direction" not in self._report({"geomean_speedup": 1.0}).warnings

    def test_direction_skips_when_unmeasured(self):
        assert "speedup_direction" in self._report({"geomean_speedup": None}).skipped

    def test_direction_threshold_is_env_overridable(self, monkeypatch):
        import importlib

        monkeypatch.setenv("DATASMITH_VERIFY_MEASURE_GEOMEAN_MIN", "1.5")
        import datasmith.docker.manifest as mod

        importlib.reload(mod)
        try:
            r = mod.evaluate_invariants({
                "schema_version": 1,
                "build": {},
                "verify": {"geomean_speedup": 1.2},
            })
            assert "speedup_direction" in r.warnings
        finally:
            monkeypatch.delenv("DATASMITH_VERIFY_MEASURE_GEOMEAN_MIN")
            importlib.reload(mod)

    # ── oracle_patch_touches_benchmarks (warn) ──
    def test_excluded_paths_warn(self):
        assert "oracle_patch_touches_benchmarks" in self._report({"patch_paths_excluded": 2}).warnings

    def test_no_excluded_paths_holds(self):
        assert "oracle_patch_touches_benchmarks" not in self._report({"patch_paths_excluded": 0}).warnings

    def test_excluded_paths_skips_when_absent(self):
        assert "oracle_patch_touches_benchmarks" in self._report({}).skipped

    # ── measure_partial (warn) ──
    def test_partial_warns_when_error_but_some_measured(self):
        r = self._report({"measure_error": "LSV selected 5 but measured 2", "benchmarks_measured_n": 2})
        assert "measure_partial" in r.warnings

    def test_partial_holds_when_no_error(self):
        r = self._report({"measure_error": None, "benchmarks_measured_n": 2})
        assert "measure_partial" not in r.warnings

    def test_partial_skips_when_nothing_measured(self):
        """Zero measured is asv_exec_failed's job — not a partial-measure warn."""
        r = self._report({"measure_error": "boom", "benchmarks_measured_n": 0})
        assert "measure_partial" in r.skipped


class TestMeasurabilityProducerCoverage:
    """Prove every new gate has a producer that actually emits its input.

    Three gates shipped structurally inert last time because nothing emitted
    the value they read. This test is driven off the invariant registry, so
    a new invariant with no producer fails immediately, and deleting a key
    from emit_measure.py fails immediately too.
    """

    _MEASURE_KEYS = {
        "measure_timed_out": "local_ci",  # produced host-side by run_measure
        "asv_exec_failed": "benchmarks_measured_n",
        "oracle_patch_failed": "patch_applied",
        "speedup_direction": "geomean_speedup",
        "oracle_patch_touches_benchmarks": "patch_paths_excluded",
        "measure_partial": "measure_error",
    }

    def test_every_new_invariant_is_registered(self):
        from datasmith.docker.manifest import INVARIANTS

        ids = {inv.id for inv in INVARIANTS}
        for inv_id in self._MEASURE_KEYS:
            assert inv_id in ids, f"{inv_id} is not registered in INVARIANTS"

    def test_every_emitter_key_is_emitted_by_emit_measure(self):
        import importlib.util
        from pathlib import Path

        root = Path(__file__).parents[2]
        emitter = root / "src" / "datasmith" / "docker" / "templates" / "emit_measure.py"
        parser = root / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py"
        spec = importlib.util.spec_from_file_location("emit_measure", emitter)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        fns = mod.load_parser_fns(str(parser))
        block = mod.measure_block(
            {
                "init": {"benchmarks_impactable": ["b"]},
                "measure": {"benchmarks": {"m.C.time_a": {"baseline": 2.0, "current": 1.0}}, "error": None},
            },
            {"present": True, "applied": True, "files_changed": 1, "paths_excluded": 0},
            "c8b8086",
            2,
            *fns,
        )
        for inv_id, key in self._MEASURE_KEYS.items():
            if key == "local_ci":
                continue  # produced by local_ci.py, covered by TestLocalCiSync
            assert key in block, f"emit_measure.py stopped emitting {key}, needed by {inv_id}"


class TestPytestPassRatioIsRecordedNotGated:
    """The pass ratio must be observable on every run, including rejected ones.

    Today any failing test fails the build: run-tests.sh exits with pytest's
    code and run_tests treats rc != 0 as a failure. CalebBell/fluids#38 is
    rejected at 554/559 -- 99.1% -- for five numba TypingErrors, with zero
    collection errors.

    Whether that is right needs a distribution, and the one available is
    survivorship-biased: all 68 harbor_runs ratios come from containers that
    already passed this gate, 60 of them at exactly 1.0. It cannot show what is
    being rejected just below 1.0. So record on every run and set a threshold
    from that, not from one repository.
    """

    def _module(self):
        return _load_local_ci_module()

    def _wrap(self, body: str) -> str:
        return f"FORMULACODE_TESTS_START\n{body}\nFORMULACODE_TESTS_END\n"

    def test_the_fluids_numbers(self):
        """554/559, the run that is currently rejected."""
        local_ci = self._module()
        stdout = self._wrap('{"error": 0, "failed": 5, "passed": 554, "skipped": 0, "total": 559}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields["pytest_pass_ratio"] == 0.991055
        assert fields["pytest_total_at_base"] == 559
        assert fields["pytest_passed_at_base"] == 554

    def test_a_perfect_run_is_exactly_one(self):
        local_ci = self._module()
        stdout = self._wrap('{"error": 0, "failed": 0, "passed": 6756, "skipped": 0, "total": 6756}')
        assert local_ci._pytest_verify_fields(stdout)["pytest_pass_ratio"] == 1.0

    def test_a_missing_total_yields_no_ratio(self):
        """Absent input must skip, never read as a ratio of zero."""
        local_ci = self._module()
        stdout = self._wrap('{"error": 0, "failed": 0}')
        assert "pytest_pass_ratio" not in local_ci._pytest_verify_fields(stdout)

    def test_a_non_numeric_total_yields_no_ratio(self):
        local_ci = self._module()
        stdout = self._wrap('{"error": 0, "failed": 0, "passed": 1, "total": "many"}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert "pytest_pass_ratio" not in fields
        assert "pytest_total_at_base" not in fields
