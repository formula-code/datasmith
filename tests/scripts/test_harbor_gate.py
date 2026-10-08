"""The Harbor half of the honesty gate.

A container can be sound and untampered and still be useless: momepy#237 built,
collected tests, passed them, and measured zero benchmarks. These fixtures are
real shapes from the 94 stored harbor_runs rows.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).parents[2]
_spec = importlib.util.spec_from_file_location("harbor_honesty", _ROOT / "scripts" / "harbor_honesty.py")
hh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hh)


def _row(**payload_overrides):
    payload = {
        "setup": {"exit_code": 0, "succeeded": True, "failed_phase": "complete"},
        "pytest": {"total": 77, "passed": 77, "failed": 0, "error": 0},
        "tests_passed": True,
        "lsv_error": None,
        "num_valid_benchmarks": 140,
        "per_benchmark_speedups": {"a.time_x": 1.6, "a.time_y": 0.98},
    }
    payload.update(payload_overrides)
    return {"status": "success", "n_benchmarks": 140, "reward_payload": payload}


class TestSpecParsing:
    def test_hash_form(self):
        assert hh.parse_spec("networkx/networkx#8148") == ("networkx", "networkx", 8148)

    def test_slash_form(self):
        assert hh.parse_spec("apache/arrow/44236") == ("apache", "arrow", 44236)


class TestPolicy:
    def test_a_healthy_trial_passes(self):
        assert hh.evaluate(_row())["harbor_ok"] is True

    def test_zero_benchmarks_fails(self):
        """momepy#237. Built, tested, passed, measured nothing."""
        v = hh.evaluate(_row(num_valid_benchmarks=0))
        assert v["harbor_ok"] is False
        assert "benchmarks_measured" in v["failed"]

    def test_an_lsv_error_fails(self):
        v = hh.evaluate(_row(lsv_error="dependency database missing"))
        assert v["harbor_ok"] is False
        assert "no_lsv_error" in v["failed"]

    def test_a_failed_setup_fails(self):
        v = hh.evaluate(_row(setup={"exit_code": 1, "succeeded": False, "failed_phase": "lsv_init"}))
        assert v["harbor_ok"] is False
        assert "setup_succeeded" in v["failed"]

    def test_all_speedups_exactly_one_fails(self):
        """pysindy#139's fabrication signature: baseline set equal to current."""
        v = hh.evaluate(_row(per_benchmark_speedups={"m.C.time_a": 1.0, "m.C.peakmem_b": 1.0}))
        assert v["harbor_ok"] is False
        assert "speedups_not_degenerate" in v["failed"]

    def test_a_genuine_no_change_result_is_not_flagged(self):
        """Not every 1.0 is fabricated. One real 1.0 among varied values is fine."""
        v = hh.evaluate(_row(per_benchmark_speedups={"a": 1.0, "b": 1.07, "c": 0.94}))
        assert "speedups_not_degenerate" in v["passed"]

    def test_no_pytest_run_fails(self):
        v = hh.evaluate(_row(pytest={"total": 0, "passed": 0}))
        assert v["harbor_ok"] is False
        assert "pytest_ran" in v["failed"]

    def test_failing_tests_warn_rather_than_fail(self):
        v = hh.evaluate(_row(tests_passed=False))
        assert "tests_passed" in v["warned"]


class TestThreeValued:
    def test_an_absent_payload_skips_everything_it_can(self):
        v = hh.evaluate({"status": "success", "reward_payload": {}})
        assert "setup_succeeded" in v["skipped"]
        assert "no_lsv_error" in v["skipped"]
        assert "pytest_ran" in v["skipped"]

    def test_an_absent_status_skips_rather_than_passing(self):
        v = hh.evaluate({"reward_payload": {}})
        assert "harbor_status_success" in v["skipped"]

    def test_a_non_dict_payload_does_not_crash(self):
        v = hh.evaluate({"status": "success", "reward_payload": "not a dict"})
        assert isinstance(v["harbor_ok"], bool)
