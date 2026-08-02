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

    def test_benchmark_init_missing_is_fatal(self):
        m = _good_manifest()
        m["build"]["benchmark_dir_init_present"] = False
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "benchmark_init_missing" in report.fatal

    def test_secrets_present_is_fatal(self):
        m = _good_manifest()
        m["build"]["secrets_scan_clean"] = False
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "secrets_present" in report.fatal

    def test_pytest_collect_failed_is_fatal(self):
        m = _good_manifest()
        m["verify"]["pytest_collect_ok"] = False
        report = evaluate_invariants(m)
        assert report.ok is False
        assert "pytest_collect_failed" in report.fatal

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

        for build_key, build_value, inv_id in (
            ("discovered_n", 0, "discovered_n_zero"),
            ("benchmark_dest_present_post_clean", False, "benchmark_dest_missing"),
            ("benchmark_dir_init_present", False, "benchmark_init_missing"),
            ("head_at_seal", "deadbeef0000", "head_commit_drift"),
            ("secrets_scan_clean", False, "secrets_present"),
        ):
            m = copy.deepcopy(good)
            m["build"][build_key] = build_value
            cases.append((m, inv_id))

        for verify_key, verify_value, inv_id in (
            ("test_timed_out", True, "test_timed_out"),
            ("pytest_collect_ok", False, "pytest_collect_failed"),
        ):
            m = copy.deepcopy(good)
            m["verify"][verify_key] = verify_value
            cases.append((m, inv_id))

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
        assert fields == {"pytest_collect_ok": True, "pytest_failed_at_base": 0}

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
        assert fields == {"pytest_collect_ok": True, "pytest_failed_at_base": 3}

    def test_collection_error_flips_collect_ok_false(self):
        local_ci = self._module()
        stdout = self._wrap(
            '{"error": 1, "failed": 0, "passed": 0, "rerun": 0, '
            '"skipped": 0, "total": 0, "warnings": 0, "xfailed": 0, "xpassed": 0}'
        )
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {"pytest_collect_ok": False, "pytest_failed_at_base": 0}

    # ── the no-ASV-benchmarks early-exit shape (hardcoded in run-tests.sh) ──

    def test_no_benchmarks_early_exit_shape(self):
        local_ci = self._module()
        stdout = self._wrap('{"total": 0, "passed": 0, "failed": 0, "error": 0, "skipped": 0}')
        fields = local_ci._pytest_verify_fields(stdout)
        assert fields == {"pytest_collect_ok": True, "pytest_failed_at_base": 0}

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
