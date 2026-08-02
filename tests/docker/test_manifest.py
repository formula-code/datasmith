"""Tests for datasmith.docker.manifest — invariant evaluation."""

from datasmith.docker.manifest import evaluate_invariants


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
