"""Tests for the in-container measure-block emitter."""

import importlib.util
import json
from pathlib import Path

_ROOT = Path(__file__).parents[2]
_EMITTER = _ROOT / "src" / "datasmith" / "docker" / "templates" / "emit_measure.py"
_PARSER = _ROOT / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py"


def _load():
    """Import the emitter by path — it is a template, not a package module."""
    spec = importlib.util.spec_from_file_location("emit_measure", _EMITTER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _lsv(benchmarks: dict, impactable: int = 3, error=None) -> dict:
    return {
        "init": {"benchmarks_impactable": [f"b{i}" for i in range(impactable)]},
        "measure": {"benchmarks": benchmarks, "selected_count": len(benchmarks), "error": error},
    }


def _patch_info(**kw) -> dict:
    base = {"present": True, "applied": True, "files_changed": 7, "paths_excluded": 0}
    base.update(kw)
    return base


def _block(m, lsv, patch_info=None, base_sha="c8b8086", rounds=2) -> dict:
    fns = m.load_parser_fns(str(_PARSER))
    return m.measure_block(lsv, patch_info or _patch_info(), base_sha, rounds, *fns)


class TestCounting:
    def test_counts_benchmarks_measured_on_both_sides(self):
        m = _load()
        b = _block(m, _lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.2}}))
        assert b["benchmarks_measured_n"] == 1
        assert b["benchmarks_degenerate_n"] == 0

    def test_none_baseline_is_degenerate_not_measured(self):
        m = _load()
        b = _block(m, _lsv({"mod.C.time_a": {"baseline": None, "current": 0.2}}))
        assert b["benchmarks_measured_n"] == 0
        assert b["benchmarks_degenerate_n"] == 1

    def test_zero_current_is_degenerate_not_measured(self):
        """A zero 'current' would make speedup infinite — the degenerate-baseline
        reward garbage the spec's invariant #16 exists to catch."""
        m = _load()
        b = _block(m, _lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.0}}))
        assert b["benchmarks_measured_n"] == 0
        assert b["benchmarks_degenerate_n"] == 1

    def test_mixed_set_counts_both_buckets(self):
        m = _load()
        b = _block(
            m,
            _lsv({
                "mod.C.time_a": {"baseline": 0.4, "current": 0.2},
                "mod.C.time_b": {"baseline": 0.4, "current": None},
            }),
        )
        assert b["benchmarks_measured_n"] == 1
        assert b["benchmarks_degenerate_n"] == 1

    def test_impactable_count_read_from_init_block(self):
        m = _load()
        b = _block(m, _lsv({}, impactable=140))
        assert b["benchmarks_impactable_n"] == 140


class TestSpeedups:
    def test_geomean_matches_parser_definition(self):
        m = _load()
        b = _block(
            m,
            _lsv({
                "mod.C.time_a": {"baseline": 4.0, "current": 1.0},  # 4.0
                "mod.C.time_b": {"baseline": 1.0, "current": 1.0},  # 1.0
            }),
        )
        assert b["geomean_speedup"] == 2.0  # sqrt(4 * 1)
        assert b["max_speedup"] == 4.0

    def test_no_measured_benchmarks_yields_none_not_zero(self):
        """None means 'not measurable'. Zero would read as a real, terrible
        speedup and silently satisfy any '> 0' style check."""
        m = _load()
        b = _block(m, _lsv({}))
        assert b["geomean_speedup"] is None
        assert b["max_speedup"] is None


class TestPatchAndProvenance:
    def test_patch_info_is_passed_through(self):
        m = _load()
        b = _block(m, _lsv({}), _patch_info(applied=False, files_changed=0, paths_excluded=2))
        assert b["patch_present"] is True
        assert b["patch_applied"] is False
        assert b["patch_files_changed"] == 0
        assert b["patch_paths_excluded"] == 2

    def test_absent_patch_reports_present_false(self):
        m = _load()
        b = _block(m, _lsv({}), _patch_info(present=False, applied=False, files_changed=0))
        assert b["patch_present"] is False

    def test_base_sha_and_rounds_recorded(self):
        m = _load()
        b = _block(m, _lsv({}), base_sha="deadbeef", rounds=5)
        assert b["base_sha_measured"] == "deadbeef"
        assert b["measure_rounds"] == 5


class TestDegradation:
    def test_lsv_error_is_surfaced(self):
        m = _load()
        b = _block(m, _lsv({}, error="LSV measure_impacted raised: boom"))
        assert "boom" in b["measure_error"]

    def test_empty_lsv_results_do_not_raise(self):
        m = _load()
        b = _block(m, {})
        assert b["benchmarks_measured_n"] == 0
        assert b["measure_ran"] is True

    def test_malformed_benchmark_entry_is_skipped_not_fatal(self):
        """parser.py's compute_per_benchmark_speedups calls .get() on every
        value and raises AttributeError on a non-dict entry — verified
        directly against the real function. The emitter must filter first."""
        m = _load()
        b = _block(m, _lsv({"mod.C.time_a": "not-a-dict"}))
        assert b["benchmarks_measured_n"] == 0


class TestOutputContract:
    def test_block_is_wrapped_in_sentinels_and_parses(self, capsys, tmp_path):
        m = _load()
        (tmp_path / "lsv.json").write_text(json.dumps(_lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.2}})))
        (tmp_path / "patch.json").write_text(json.dumps(_patch_info()))
        rc = m.main([
            "--lsv-results",
            str(tmp_path / "lsv.json"),
            "--patch-info",
            str(tmp_path / "patch.json"),
            "--parser",
            str(_PARSER),
            "--base-sha",
            "c8b8086",
            "--rounds",
            "2",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "FORMULACODE_MEASURE_START" in out
        assert "FORMULACODE_MEASURE_END" in out
        payload = out.split("FORMULACODE_MEASURE_START")[1].split("FORMULACODE_MEASURE_END")[0]
        assert json.loads(payload.strip())["benchmarks_measured_n"] == 1

    def test_missing_input_files_still_emit_a_block(self, capsys, tmp_path):
        """A crashed lsv_measure must not cost us the block — the invariants
        need benchmarks_measured_n == 0 to fire, not a missing block."""
        m = _load()
        rc = m.main([
            "--lsv-results",
            str(tmp_path / "nope.json"),
            "--patch-info",
            str(tmp_path / "nope2.json"),
            "--parser",
            str(_PARSER),
        ])
        assert rc == 0
        assert "FORMULACODE_MEASURE_START" in capsys.readouterr().out


class TestLsvUnavailable:
    def test_lsv_error_is_reported_as_measure_error(self):
        """A failed LSV install must read as 'LSV unavailable', not as
        'this container cannot measure'. docker_build_final.sh installs it
        with `|| true`, so a network blip yields an image with no LSV."""
        m = _load()
        fns = m.load_parser_fns(str(_PARSER))
        b = m.measure_block(_lsv({}), _patch_info(), "sha", 2, *fns, "LSV unavailable: boom")
        assert "LSV unavailable" in b["measure_error"]
        assert b["benchmarks_measured_n"] == 0

    def test_real_lsv_error_wins_over_the_availability_probe(self):
        m = _load()
        fns = m.load_parser_fns(str(_PARSER))
        b = m.measure_block(_lsv({}, error="measure_impacted raised"), _patch_info(), "s", 2, *fns, "unavailable")
        assert b["measure_error"] == "measure_impacted raised"

    def test_no_lsv_error_leaves_measure_error_none(self):
        m = _load()
        fns = m.load_parser_fns(str(_PARSER))
        b = m.measure_block(_lsv({}), _patch_info(), "sha", 2, *fns, "")
        assert b["measure_error"] is None


class TestDroppedBenchmarks:
    """`benchmarks_dropped_n` must be three-valued.

    A benchmark that asv SELECTED but returned no result for (worker killed on
    timeout, or errored) is discarded by lightspeed's ``_extract_deltas`` with a
    bare ``continue``. The geomean then covers a smaller population, and a
    shrinking measured set looks exactly like a genuinely quieter one.

    The failure this guards is the reverse: an LSV predating
    ``MeasureResult.dropped`` cannot report drops at all, so the count must be
    ``None`` (invariant SKIPS) and never ``0`` (invariant reads "no drops" and
    PASSES). Observed live on networkx#8148, where
    ``time_betweenness_centrality-3`` vanished from 8 of 12 measure runs while a
    ``getattr(..., [])`` default reported ``dropped_count == 0`` throughout.
    """

    def test_absent_key_is_none_not_zero(self):
        m = _load()
        lsv = _lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.2}})
        lsv["measure"].pop("dropped_count", None)
        assert _block(m, lsv)["benchmarks_dropped_n"] is None

    def test_zero_is_preserved_as_zero(self):
        m = _load()
        lsv = _lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.2}})
        lsv["measure"]["dropped_count"] = 0
        assert _block(m, lsv)["benchmarks_dropped_n"] == 0

    def test_a_real_drop_is_reported(self):
        m = _load()
        lsv = _lsv({"mod.C.time_a": {"baseline": 0.4, "current": 0.2}})
        lsv["measure"]["dropped_count"] = 3
        assert _block(m, lsv)["benchmarks_dropped_n"] == 3

    def test_dropped_is_independent_of_degenerate(self):
        """A dropped benchmark never appears in `benchmarks`; a degenerate one does."""
        m = _load()
        lsv = _lsv({
            "mod.C.time_a": {"baseline": 0.4, "current": 0.2},
            "mod.C.time_b": {"baseline": None, "current": 0.2},
        })
        lsv["measure"]["dropped_count"] = 2
        b = _block(m, lsv)
        assert b["benchmarks_measured_n"] == 1
        assert b["benchmarks_degenerate_n"] == 1
        assert b["benchmarks_dropped_n"] == 2
