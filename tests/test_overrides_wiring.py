"""The two producer gaps that left invariants inert, and their wiring.

Both were flagged as open in the 2026-08-13 ledger:

  #18 dilution_ratio    reads expected_n, which reaches the trial container via
                        FORMULACODE_EXPECTED_N -- nothing injected it.
  benchmark_dest_missing (FATAL) reads $BENCHMARK_DEST at image build time --
                        nothing set it, so it had been inert since it shipped.

Both inputs live on formulacode_task_overrides, which is RLS-locked with no
anon grant, so neither the trial container nor the build can read it directly.
The value must be fetched host-side and injected. These tests assert the
injection actually happens; each fails if its producer is removed.
"""

import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]


def _load_overrides():
    from datasmith.utils import overrides

    return overrides


@pytest.fixture(autouse=True)
def _clear_override_cache():
    """all_overrides() caches for the process. Without this, one test's
    fixture data leaks into the next and the DB stub is never consulted."""
    from datasmith.utils import overrides

    overrides._ALL_CACHE = None
    yield
    overrides._ALL_CACHE = None


class TestOverrideLookup:
    def test_returns_empty_mapping_for_no_tasks(self):
        m = _load_overrides()
        assert m.fetch_overrides([]) == {}

    def test_missing_table_or_db_is_not_fatal(self, monkeypatch):
        """Stage 6 and 7 must run on a machine where the table does not exist
        (a fresh checkout, or before the migration is applied). A lookup
        failure means 'no overrides', never a crash."""
        m = _load_overrides()

        def _boom(*a, **k):
            raise RuntimeError("relation does not exist")

        monkeypatch.setattr(m, "fetch_all", _boom)
        assert m.fetch_overrides([("pvlib", "pvlib-python", 369)]) == {}

    def test_keys_are_the_canonical_task_triple(self, monkeypatch):
        m = _load_overrides()
        monkeypatch.setattr(
            m,
            "fetch_all",
            lambda *a, **k: [
                {
                    "owner": "pvlib",
                    "repo": "pvlib-python",
                    "issue_number": 369,
                    "benchmark_dest": "benchmarks/b.py",
                    "expected_n": 3,
                },
            ],
        )
        got = m.fetch_overrides([("pvlib", "pvlib-python", 369)])
        assert got[("pvlib", "pvlib-python", 369)]["benchmark_dest"] == "benchmarks/b.py"
        assert got[("pvlib", "pvlib-python", 369)]["expected_n"] == 3


class TestLookupIsCached:
    def test_the_table_is_read_once_per_process(self, monkeypatch):
        """Consumers sit in per-item loops (stage 6 enqueues neighbours
        mid-flight), so an uncached lookup would be one round-trip per task."""
        m = _load_overrides()
        calls = []
        monkeypatch.setattr(m, "fetch_all", lambda *a, **k: calls.append(1) or [])
        m.fetch_overrides([("o", "r", 1)])
        m.fetch_overrides([("o", "r", 2)])
        m.fetch_overrides([("o", "r", 3)])
        assert len(calls) == 1, f"table read {len(calls)} times, expected 1"

    def test_a_failed_read_is_cached_too(self, monkeypatch):
        """If the table is absent it stays absent; retrying per item is a
        slow way to reach the same answer."""
        m = _load_overrides()
        calls = []

        def _boom(*a, **k):
            calls.append(1)
            raise RuntimeError("relation does not exist")

        monkeypatch.setattr(m, "fetch_all", _boom)
        assert m.fetch_overrides([("o", "r", 1)]) == {}
        assert m.fetch_overrides([("o", "r", 2)]) == {}
        assert len(calls) == 1

    def test_refresh_forces_a_reread(self, monkeypatch):
        m = _load_overrides()
        calls = []
        monkeypatch.setattr(m, "fetch_all", lambda *a, **k: calls.append(1) or [])
        m.all_overrides()
        m.all_overrides(refresh=True)
        assert len(calls) == 2


class TestExpectedNReachesTheTrial:
    """#18's producer: FORMULACODE_EXPECTED_N in the task's [verifier.env]."""

    def test_task_toml_carries_expected_n_when_declared(self):
        from datasmith.harbor_adapter.utils import render_task_toml

        toml = render_task_toml(verifier_env={"FORMULACODE_EXPECTED_N": "10"})
        assert "FORMULACODE_EXPECTED_N" in toml
        assert '"10"' in toml

    def test_generate_task_injects_expected_n(self, tmp_path):
        """The end of the chain: an override with expected_n set must land in
        the rendered task.toml, or #18 skips forever."""
        from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, FormulaCodeRecord

        rec = FormulaCodeRecord(
            container_name="img:tag",
            patch="",
            owner="pvlib",
            repo="pvlib-python",
            issue_number=369,
            gt_hash="a" * 40,
            base_commit="b" * 40,
            instructions="do the thing",
            repo_name="pvlib/pvlib-python",
        )
        adapter = FormulaCodeAdapter(harbor_tasks_root=tmp_path, force=True)
        out = adapter.generate_task(rec, rounds=1, expected_n=10)
        toml = (out / "task.toml").read_text()
        assert "FORMULACODE_EXPECTED_N" in toml
        assert '"10"' in toml

    def test_absent_expected_n_injects_nothing(self, tmp_path):
        """NULL expected_n is the common case. It must NOT emit an empty or
        zero value -- parser.py would parse '' to None anyway, but emitting a
        key that is always empty makes the wiring look live when it is not."""
        from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, FormulaCodeRecord

        rec = FormulaCodeRecord(
            container_name="img:tag",
            patch="",
            owner="o",
            repo="r",
            issue_number=1,
            gt_hash="a" * 40,
            base_commit="b" * 40,
            instructions="x",
            repo_name="o/r",
        )
        adapter = FormulaCodeAdapter(harbor_tasks_root=tmp_path, force=True)
        out = adapter.generate_task(rec, rounds=1, expected_n=None)
        assert "FORMULACODE_EXPECTED_N" not in (out / "task.toml").read_text()

    def test_parser_reads_the_env_var_the_adapter_writes(self):
        """Both halves must agree on the NAME. A rename on one side alone
        silently un-wires the invariant."""
        parser_src = (_ROOT / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py").read_text()
        assert "FORMULACODE_EXPECTED_N" in parser_src

        adapter_src = (_ROOT / "src" / "datasmith" / "harbor_adapter" / "adapter.py").read_text()
        assert "FORMULACODE_EXPECTED_N" in adapter_src

    def test_dilution_fires_end_to_end_on_the_injected_value(self, monkeypatch):
        """Prove the whole chain: env var -> context -> invariant fires."""
        spec = importlib.util.spec_from_file_location(
            "fc_parser_wiring",
            _ROOT / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        monkeypatch.setenv("FORMULACODE_EXPECTED_N", "10")
        ctx = mod.build_trial_context(
            base_commit="abc",
            lsv_results={"measure": {"selected_count": 140}},
            speedups={"a": 1.2},
            benchmarks={"a": {"baseline": 2.0, "current": 1.0}},
            snapshot_block={},
        )
        assert ctx["expected_n"] == 10
        report = mod.evaluate_trial_invariants(ctx)
        assert "dilution_ratio" in report["warnings"], (
            "140 impacted vs 10 expected must warn -- this is the networkx case"
        )

    def test_dilution_skips_when_the_env_var_is_absent(self, monkeypatch):
        spec = importlib.util.spec_from_file_location(
            "fc_parser_wiring2",
            _ROOT / "src" / "datasmith" / "harbor_adapter" / "template" / "parser.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        monkeypatch.delenv("FORMULACODE_EXPECTED_N", raising=False)
        ctx = mod.build_trial_context(
            base_commit="abc",
            lsv_results={"measure": {"selected_count": 140}},
            speedups={"a": 1.2},
            benchmarks={},
            snapshot_block={},
        )
        assert ctx["expected_n"] is None
        assert "dilution_ratio" in mod.evaluate_trial_invariants(ctx)["skipped"]


class TestBenchmarkDestReachesTheBuild:
    """benchmark_dest_missing's producer: the BENCHMARK_DEST build arg."""

    def test_dockerfile_declares_the_build_arg_in_the_run_stage(self):
        """docker_build_run.sh reads $BENCHMARK_DEST, and it runs in the `run`
        stage -- ARGs do not cross FROM boundaries, so the ARG must be declared
        there specifically, not merely somewhere in the file."""
        src = (_ROOT / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr").read_text()
        run_stage = src.split("FROM pkg AS run", 1)[1].split("FROM run AS final", 1)[0]
        assert "ARG BENCHMARK_DEST" in run_stage, (
            "BENCHMARK_DEST is not declared in the `run` stage, so docker_build_run.sh would never see it"
        )

    def test_run_stage_exports_it_to_the_script(self):
        src = (_ROOT / "src" / "datasmith" / "docker" / "templates" / "Dockerfile.pr").read_text()
        run_stage = src.split("FROM pkg AS run", 1)[1].split("FROM run AS final", 1)[0]
        assert "BENCHMARK_DEST=" in run_stage

    def test_image_manager_passes_the_build_arg(self):
        import inspect

        from datasmith.docker.images import ImageManager

        sig = inspect.signature(ImageManager.build_pr_image)
        assert "benchmark_dest" in sig.parameters
        src = inspect.getsource(ImageManager.build_pr_image)
        assert '"BENCHMARK_DEST"' in src

    def test_the_fatal_gate_is_now_live_end_to_end(self):
        """The point of the whole wiring: benchmark_dest_missing must PASS
        when the file survives, FIRE when it does not, and SKIP when no
        override declared one. Before this change it skipped in all three
        cases, because nothing ever set $BENCHMARK_DEST.

        The breadcrumb values below were produced by actually running
        docker_build_run.sh's emission block in a container under all three
        BENCHMARK_DEST conditions; this pins the evaluator's response to them.
        """
        from datasmith.docker.manifest import evaluate_invariants

        base = {
            "discovered_n": 3,
            "secrets_scan_clean": True,
            "declared_commit": "a" * 40,
            "head_at_seal": "a" * 40,
        }

        declared_present = evaluate_invariants({
            "schema_version": 1,
            "build": {**base, "benchmark_dest_present_post_clean": True, "benchmark_dest": "benchmarks/b.py"},
            "verify": {},
        })
        assert "benchmark_dest_missing" not in declared_present.fatal
        assert "benchmark_dest_missing" not in declared_present.skipped, (
            "the gate skipped even though the build declared a benchmark_dest -- it is still inert"
        )

        declared_missing = evaluate_invariants({
            "schema_version": 1,
            "build": {**base, "benchmark_dest_present_post_clean": False},
            "verify": {},
        })
        assert "benchmark_dest_missing" in declared_missing.fatal
        assert declared_missing.ok is False

        undeclared = evaluate_invariants({"schema_version": 1, "build": base, "verify": {}})
        assert "benchmark_dest_missing" in undeclared.skipped
        assert undeclared.ok is True

    def test_build_script_still_gates_on_a_non_empty_value(self):
        """The conditional emission is load-bearing: unconditionally emitting
        0 would turn a should-be-skipped FATAL invariant into a permanent
        hard-fail on every build without an override."""
        src = (_ROOT / "src" / "datasmith" / "docker" / "templates" / "docker_build_run.sh").read_text()
        assert '_BENCH_DEST="${BENCHMARK_DEST:-}"' in src
        assert 'if [ -n "$_BENCH_DEST" ]; then' in src
