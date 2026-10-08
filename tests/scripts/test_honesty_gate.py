"""The honesty gate decides which containers ship, so its policy needs tests.

The gate was validated by running it against two real images, but the policy
functions themselves had none. A verifier flagged 579 lines of gate logic with
no test coverage.

The fact fixtures below are real shapes taken from those two runs.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ch = _load("container_honesty")


def _facts(**overrides):
    """A clean container. Shape taken from a real probe run."""
    base = {
        "integrity": {
            "python_path": "/opt/conda/envs/asv_3.12/bin/python",
            "python_is_elf": True,
            "python_on_path": "/opt/conda/envs/asv_3.12/bin/python",
            "python_on_path_is_elf": True,
            "python_path_matches_argv": True,
            "python_in_prefix": True,
            "grep_path": "/usr/bin/grep",
            "grep_is_system": True,
            "ld_preload": None,
            "customize_modules": [],
            "thread_caps": {"OMP_NUM_THREADS": "1"},
        },
        "imports": {
            "import_name": "networkx",
            "package_import_ok": True,
            "extensions_total": 0,
            "extensions_ok": 0,
            "extensions_failed": [],
            "extensions_truncated": False,
        },
        "benchmarks": {"discovered_n": 38, "discover_error": None, "source_n": 38, "source_error": None},
        "tests": {"collect_ok": True, "collected_n": 6779},
    }
    for group, values in overrides.items():
        base[group] = {**base[group], **values}
    return base


class TestCleanContainer:
    def test_a_clean_container_is_honest(self):
        assert ch.evaluate(_facts())["honest"] is True

    def test_nothing_is_skipped_when_all_facts_are_present(self):
        assert ch.evaluate(_facts())["skipped"] == []


class TestTamperSignatures:
    def test_a_wrapped_interpreter_fails(self):
        """pysindy#139: `python` on PATH is a bash script that execs python.real."""
        v = ch.evaluate(
            _facts(
                integrity={
                    "python_path": "/opt/conda/envs/asv_3.10/bin/python.real",
                    "python_on_path": "/opt/conda/envs/asv_3.10/bin/python",
                    "python_on_path_is_elf": False,
                    "python_path_matches_argv": False,
                }
            )
        )
        assert v["honest"] is False
        assert "python_is_elf" in v["failed"]
        assert "python_not_wrapped" in v["failed"]

    def test_sys_executable_alone_would_have_missed_it(self):
        """The regression that defeated the gate's first version.

        `python_is_elf` is True for sys.executable, because the wrapper execs
        the real binary. Only the PATH-resolved file reveals the wrapper.
        """
        facts = _facts(
            integrity={
                "python_path": "/opt/conda/envs/asv_3.10/bin/python.real",
                "python_is_elf": True,
                "python_on_path": "/opt/conda/envs/asv_3.10/bin/python",
                "python_on_path_is_elf": False,
                "python_path_matches_argv": False,
            }
        )
        assert facts["integrity"]["python_is_elf"] is True
        assert ch._c_python_is_elf(facts) is False

    def test_a_replaced_grep_fails(self):
        v = ch.evaluate(_facts(integrity={"grep_path": "/usr/local/bin/grep", "grep_is_system": False}))
        assert v["honest"] is False
        assert "grep_is_system" in v["failed"]

    def test_a_sitecustomize_fails(self):
        v = ch.evaluate(_facts(integrity={"customize_modules": ["/opt/conda/.../site-packages/sitecustomize.py"]}))
        assert v["honest"] is False
        assert "no_sitecustomize" in v["failed"]

    def test_ld_preload_fails(self):
        v = ch.evaluate(_facts(integrity={"ld_preload": "/tmp/evil.so"}))
        assert v["honest"] is False
        assert "no_ld_preload" in v["failed"]


class TestSoundness:
    def test_a_failed_extension_import_fails(self):
        """A build that silently skipped an extension still imports the package."""
        v = ch.evaluate(
            _facts(
                imports={
                    "extensions_total": 12,
                    "extensions_ok": 11,
                    "extensions_failed": [{"module": "pandas._libs.tslib", "error": "undefined symbol"}],
                }
            )
        )
        assert v["honest"] is False
        assert "extensions_import" in v["failed"]

    def test_zero_benchmarks_fails(self):
        v = ch.evaluate(_facts(benchmarks={"discovered_n": 0}))
        assert v["honest"] is False
        assert "benchmarks_discovered" in v["failed"]

    def test_a_package_that_does_not_import_fails(self):
        v = ch.evaluate(_facts(imports={"package_import_ok": False}))
        assert v["honest"] is False
        assert "package_imports" in v["failed"]


class TestThreeValued:
    """An absent input must SKIP, never pass. The reverse cost us a corpus."""

    # Every input a check reads. A check skips only when ALL of them are
    # absent -- one present input is enough to conclude, and refusing to
    # conclude on evidence you have is its own kind of dishonesty.
    @pytest.mark.parametrize(
        ("group", "keys", "check"),
        [
            ("benchmarks", ("discovered_n", "source_n"), "benchmarks_discovered"),
            ("tests", ("collect_ok", "collected_n"), "pytest_collects"),
            ("imports", ("package_import_ok",), "package_imports"),
        ],
    )
    def test_absent_fact_skips_rather_than_passing(self, group: str, keys: tuple[str, ...], check: str):
        facts = _facts()
        for key in keys:
            facts[group][key] = None
        v = ch.evaluate(facts)
        assert check in v["skipped"]
        assert check not in v["passed"]
        assert check not in v["failed"]

    def test_a_truncated_extension_sweep_skips(self):
        """Half a sweep cannot conclude the extensions are fine."""
        facts = _facts(imports={"extensions_total": 400, "extensions_truncated": True})
        assert ch._c_extensions_import(facts) is None

    def test_a_raising_check_fails_rather_than_crashing_the_gate(self):
        v = ch.evaluate({"integrity": {}, "imports": {}, "benchmarks": {}, "tests": {}})
        assert isinstance(v["honest"], bool)


# ------------------------------------------------------------------ pandas
# Two checks that reported the wrong thing on a container that was fine.


def test_benchmarks_no_longer_skip_when_asv_has_never_run() -> None:
    """`discovered_n` is None on any image that has not run asv.

    `Benchmarks.load` reads results/benchmarks.json, which does not exist
    until a run produces it. The check used to return None and SKIP, so a
    container with no benchmarks at all read the same as one that had simply
    not run yet. `source_n` is parsed from the benchmark directory instead.
    """
    facts = _facts(benchmarks={"discovered_n": None, "source_n": 51})
    assert ch._c_benchmarks_discovered(facts) is True


def test_benchmarks_fail_when_the_source_has_none() -> None:
    facts = _facts(benchmarks={"discovered_n": None, "source_n": 0})
    assert ch._c_benchmarks_discovered(facts) is False


def test_benchmarks_fail_when_asv_contradicts_the_source() -> None:
    """Source has benchmarks, asv sees none. A real failure, not a skip."""
    facts = _facts(benchmarks={"discovered_n": 0, "source_n": 51})
    assert ch._c_benchmarks_discovered(facts) is False


def test_benchmarks_still_skip_when_neither_number_exists() -> None:
    facts = _facts(benchmarks={"discovered_n": None, "source_n": None})
    assert ch._c_benchmarks_discovered(facts) is None


def test_pytest_passes_when_collection_worked_but_pytest_exited_nonzero() -> None:
    """The pandas case.

    pandas collects 205357 tests and then exits non-zero because its own
    addopts turn a deprecation warning into an error. That is the repository's
    warning policy, not a broken container.
    """
    facts = _facts(tests={"collect_ok": False, "collected_n": 205357})
    assert ch._c_pytest_collects(facts) is True


def test_pytest_still_fails_when_nothing_was_collected() -> None:
    facts = _facts(tests={"collect_ok": False, "collected_n": 0})
    assert ch._c_pytest_collects(facts) is False


def test_pytest_still_fails_when_collection_crashed_outright() -> None:
    facts = _facts(tests={"collect_ok": False, "collected_n": None})
    assert ch._c_pytest_collects(facts) is False


class TestSourceBenchmarkCount:
    """The count that no build artifact can move.

    `Benchmarks.load` reads results/benchmarks.json, and `asv_benchmarks.txt`
    lives in the repository. Both are writable by whatever built the image.
    Parsing the benchmark directory is not.
    """

    probe = _load("honesty_probe")

    def test_counts_asv_convention_functions(self, tmp_path: Path) -> None:
        d = tmp_path / "benchmarks"
        d.mkdir()
        (d / "bench_a.py").write_text(
            "class Suite:\n"
            "    def setup(self):\n        pass\n"
            "    def time_one(self):\n        pass\n"
            "    def peakmem_two(self):\n        pass\n"
            "    def helper(self):\n        pass\n",
            encoding="utf-8",
        )
        (d / "bench_b.py").write_text("def track_three():\n    pass\n", encoding="utf-8")
        assert self.probe._count_source_benchmarks(d) == 3

    def test_a_module_that_cannot_be_imported_still_counts(self, tmp_path: Path) -> None:
        """Parsing, not importing. A missing optional dependency must not
        make the suite look empty -- that is the failure this replaces."""
        d = tmp_path / "benchmarks"
        d.mkdir()
        (d / "bench.py").write_text(
            "import a_module_that_does_not_exist\n\ndef time_one():\n    pass\n", encoding="utf-8"
        )
        assert self.probe._count_source_benchmarks(d) == 1

    def test_a_syntax_error_does_not_abort_the_sweep(self, tmp_path: Path) -> None:
        d = tmp_path / "benchmarks"
        d.mkdir()
        (d / "broken.py").write_text("def (((\n", encoding="utf-8")
        (d / "good.py").write_text("def time_one():\n    pass\n", encoding="utf-8")
        assert self.probe._count_source_benchmarks(d) == 1

    def test_benchmark_dir_is_resolved_against_the_config(self, tmp_path: Path) -> None:
        conf = tmp_path / "asv.conf.json"
        conf.write_text('{\n  // a comment\n  "benchmark_dir": "benchmarks"\n}\n', encoding="utf-8")
        assert self.probe._asv_benchmark_dir(conf) == tmp_path / "benchmarks"

    def test_a_config_without_benchmark_dir_returns_none(self, tmp_path: Path) -> None:
        conf = tmp_path / "asv.conf.json"
        conf.write_text('{"project": "x"}\n', encoding="utf-8")
        assert self.probe._asv_benchmark_dir(conf) is None
