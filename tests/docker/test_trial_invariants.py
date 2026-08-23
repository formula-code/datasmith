"""Tests for the trial-time invariants in harbor_adapter/template/parser.py.

These are evaluated inside the trial container after the agent has run, and
echoed into reward.json (and thence harbor_runs.reward_payload). They judge
whether a trial's REWARD is trustworthy -- unlike the build-time invariants,
which judge whether an image is usable.

Every invariant gets three cases: fires, skips, holds. 'Skips' carries the
weight: the previous run shipped three gates that compared against values
nothing emitted, and a skip indistinguishable from a pass is that same defect.
"""

import importlib.util
from pathlib import Path

import pytest

_PARSER = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py"


def _load():
    spec = importlib.util.spec_from_file_location("fc_parser_test", _PARSER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _ctx(**kw):
    """A trial context where every invariant holds, overridable per test."""
    base = {
        "base_commit": "c8b8086abc",
        "baseline_sha": "c8b8086abc",
        "speedups": {"m.C.time_a": 2.0, "m.C.time_b": 1.5},
        "benchmarks": {
            "m.C.time_a": {"baseline": 2.0, "current": 1.0},
            "m.C.time_b": {"baseline": 1.5, "current": 1.0},
        },
        "impacted_n": 2,
        "expected_n": 2,
        "snapshot_factor": 1.0,
        "baseline_from_cache": False,
    }
    base.update(kw)
    return base


class TestBaselineProvenance:
    """#15 -- FATAL. The shapely failure: baselines measured AFTER the patch."""

    def test_fires_on_mismatch(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(baseline_sha="deadbeef99"))
        assert "baseline_sha_mismatch" in r["fatal"]

    def test_holds_when_equal(self):
        m = _load()
        assert "baseline_sha_mismatch" not in m.evaluate_trial_invariants(_ctx())["fatal"]

    def test_compares_on_the_shorter_sha(self):
        """Images record short shas; a prefix match is a match."""
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(base_commit="c8b8086abc123", baseline_sha="c8b8086"))
        assert "baseline_sha_mismatch" not in r["fatal"]

    def test_skips_when_baseline_sha_absent(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(baseline_sha=None))
        assert "baseline_sha_mismatch" in r["skipped"]
        assert "baseline_sha_mismatch" not in r["fatal"]

    def test_skips_when_base_commit_absent(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(base_commit=None))
        assert "baseline_sha_mismatch" in r["skipped"]


class TestDegenerateBaseline:
    """#16 -- FATAL. A zero or missing baseline makes the reward garbage."""

    def test_fires_when_every_baseline_is_degenerate(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(benchmarks={"m.C.time_a": {"baseline": 0.0, "current": 1.0}}, speedups={}))
        assert "degenerate_baseline" in r["fatal"]

    def test_holds_when_a_finite_baseline_exists(self):
        m = _load()
        assert "degenerate_baseline" not in m.evaluate_trial_invariants(_ctx())["fatal"]

    def test_skips_when_no_benchmarks_were_measured_at_all(self):
        """Nothing measured is a different failure (lsv_error / setup), and
        reporting it here too would double-count one problem."""
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(benchmarks={}, speedups={}))
        assert "degenerate_baseline" in r["skipped"]


class TestSpeedupDirection:
    """#17 -- warn. h11's apparent 0.633 slowdown."""

    def test_warns_on_geomean_below_one(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(speedups={"a": 0.5, "b": 0.8}))
        assert "oracle_speedup_direction" in r["warnings"]

    def test_holds_at_parity(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(speedups={"a": 1.0}))
        assert "oracle_speedup_direction" not in r["warnings"]

    def test_skips_when_nothing_measured(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(speedups={}, benchmarks={}))
        assert "oracle_speedup_direction" in r["skipped"]


class TestDilution:
    """#18 -- warn. networkx measuring 140 when 10 were expected."""

    def test_warns_when_impacted_far_exceeds_expected(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(impacted_n=140, expected_n=10))
        assert "dilution_ratio" in r["warnings"]

    def test_holds_at_parity(self):
        m = _load()
        assert "dilution_ratio" not in m.evaluate_trial_invariants(_ctx(impacted_n=10, expected_n=10))["warnings"]

    def test_skips_when_expected_n_is_null(self):
        """expected_n is hand-declared and usually NULL. It must SKIP, not
        fire -- this is the invariant the 2026-07-31 spec would have shipped
        permanently inert."""
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(expected_n=None))
        assert "dilution_ratio" in r["skipped"]
        assert "dilution_ratio" not in r["warnings"]

    def test_skips_when_expected_n_is_zero(self):
        m = _load()
        assert "dilution_ratio" in m.evaluate_trial_invariants(_ctx(expected_n=0))["skipped"]

    def test_threshold_is_env_overridable(self, monkeypatch):
        monkeypatch.setenv("DATASMITH_VERIFY_DILUTION_RATIO_MAX", "1.5")
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(impacted_n=4, expected_n=2))
        assert "dilution_ratio" in r["warnings"]


class TestSnapshotFactor:
    """#19 -- warn. h11's ~0.5x constant bias between snapshot and ASV."""

    def test_warns_below_the_floor(self):
        m = _load()
        assert "snapshot_asv_factor" in m.evaluate_trial_invariants(_ctx(snapshot_factor=0.2))["warnings"]

    def test_warns_above_the_ceiling(self):
        m = _load()
        assert "snapshot_asv_factor" in m.evaluate_trial_invariants(_ctx(snapshot_factor=5.0))["warnings"]

    def test_holds_inside_the_band(self):
        m = _load()
        assert "snapshot_asv_factor" not in m.evaluate_trial_invariants(_ctx(snapshot_factor=1.0))["warnings"]

    def test_skips_when_absent(self):
        m = _load()
        assert "snapshot_asv_factor" in m.evaluate_trial_invariants(_ctx(snapshot_factor=None))["skipped"]


class TestBaselineProvenanceSource:
    """#20 -- warn. optuna's cold-oracle-vs-warm-agent free ~1.5x."""

    def test_warns_when_the_baseline_came_from_cache(self):
        m = _load()
        assert "baseline_from_cache" in m.evaluate_trial_invariants(_ctx(baseline_from_cache=True))["warnings"]

    def test_holds_on_a_fresh_measure(self):
        m = _load()
        assert "baseline_from_cache" not in m.evaluate_trial_invariants(_ctx())["warnings"]

    def test_skips_when_unknown(self):
        m = _load()
        assert "baseline_from_cache" in m.evaluate_trial_invariants(_ctx(baseline_from_cache=None))["skipped"]


class TestReportShape:
    def test_clean_trial_is_ok(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx())
        assert r["ok"] is True
        assert r["fatal"] == []

    def test_a_fatal_sets_ok_false(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(baseline_sha="deadbeef99"))
        assert r["ok"] is False

    def test_warnings_do_not_set_ok_false(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx(baseline_from_cache=True))
        assert r["ok"] is True
        assert "baseline_from_cache" in r["warnings"]

    def test_empty_context_skips_everything_and_does_not_raise(self):
        """A partially-run trial must still produce a well-formed block."""
        m = _load()
        r = m.evaluate_trial_invariants({})
        assert r["ok"] is True
        assert r["fatal"] == []
        assert len(r["skipped"]) == 6

    def test_every_invariant_is_accounted_for_exactly_once(self):
        m = _load()
        r = m.evaluate_trial_invariants(_ctx())
        seen = r["fatal"] + r["warnings"] + r["skipped"] + r["passed"]
        assert len(seen) == len(set(seen)) == 6


class TestProducerCoverage:
    """Prove each gate's input is actually produced somewhere.

    Three gates shipped structurally inert last time because nothing emitted
    the value they read. These assertions fail if a producer is removed.
    """

    def test_parser_accepts_base_commit(self):
        """#15's reference value. parser.py's argparse had no --base-commit,
        so the invariant would have been inert on arrival."""
        src = _PARSER.read_text()
        assert "--base-commit" in src

    def test_test_sh_passes_base_commit_to_parser(self):
        """The other half: the arg must actually be supplied at the call site."""
        test_sh = (
            Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "test.sh"
        ).read_text()
        parser_call = [ln for ln in test_sh.splitlines() if "parser.py" in ln]
        assert parser_call, "test.sh never invokes parser.py"
        assert any("--base-commit" in ln for ln in parser_call), (
            "test.sh calls parser.py without --base-commit, so invariant #15 would skip forever"
        )

    def test_lsv_init_records_the_baseline_sha(self):
        """#15's measured value. lsv_init.py recorded no sha at all, so
        NEITHER side of the comparison existed."""
        src = (
            Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "lsv_init.py"
        ).read_text()
        assert '"baseline_sha"' in src
        assert "rev-parse" in src

    @pytest.mark.parametrize(
        "inv_id,key",
        [
            ("baseline_sha_mismatch", "baseline_sha"),
            ("degenerate_baseline", "benchmarks"),
            ("oracle_speedup_direction", "speedups"),
            ("dilution_ratio", "expected_n"),
            ("snapshot_asv_factor", "snapshot_factor"),
            ("baseline_from_cache", "baseline_from_cache"),
        ],
    )
    def test_each_invariant_reads_a_context_key_the_builder_supplies(self, inv_id, key):
        """The context builder must supply every key an invariant reads, or
        that invariant skips forever regardless of what the trial did."""
        m = _load()
        keys = m.trial_context_keys()
        assert key in keys, f"{inv_id} reads {key!r}, which the trial context never supplies"
