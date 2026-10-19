"""Tests for the oracle-patch filter/applier."""

import importlib.util
import json
import subprocess
from pathlib import Path

_APPLIER = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "apply_oracle_patch.py"

_SRC_HUNK = """diff --git a/pkg/core.py b/pkg/core.py
index 1111111..2222222 100644
--- a/pkg/core.py
+++ b/pkg/core.py
@@ -1,2 +1,2 @@
 def f():
-    return slow()
+    return fast()
"""

_BENCH_HUNK = """diff --git a/benchmarks/bench_core.py b/benchmarks/bench_core.py
index 3333333..4444444 100644
--- a/benchmarks/bench_core.py
+++ b/benchmarks/bench_core.py
@@ -1,2 +1,2 @@
 class S:
-    def time_f(self): f()
+    def time_f(self): f(); f()
"""

_NEW_BENCH_HUNK = """diff --git a/benchmarks/bench_new.py b/benchmarks/bench_new.py
new file mode 100644
index 0000000..5555555
--- /dev/null
+++ b/benchmarks/bench_new.py
@@ -0,0 +1,2 @@
+class T:
+    def time_g(self): pass
"""

_CONF_HUNK = """diff --git a/asv.conf.json b/asv.conf.json
index 6666666..7777777 100644
--- a/asv.conf.json
+++ b/asv.conf.json
@@ -1,1 +1,1 @@
-{"version": 1}
+{"version": 2}
"""


def _load():
    spec = importlib.util.spec_from_file_location("apply_oracle_patch", _APPLIER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestFilterPatch:
    def test_source_section_is_kept(self):
        m = _load()
        out, dropped = m.filter_patch(_SRC_HUNK, ["benchmarks/"])
        assert "pkg/core.py" in out
        assert dropped == 0

    def test_benchmark_section_is_dropped(self):
        m = _load()
        out, dropped = m.filter_patch(_SRC_HUNK + _BENCH_HUNK, ["benchmarks/"])
        assert "pkg/core.py" in out
        assert "bench_core.py" not in out
        assert dropped == 1

    def test_patch_created_benchmark_file_is_dropped(self):
        """The case git-checkout-after-applying cannot undo."""
        m = _load()
        out, dropped = m.filter_patch(_SRC_HUNK + _NEW_BENCH_HUNK, ["benchmarks/"])
        assert "bench_new.py" not in out
        assert dropped == 1

    def test_asv_config_section_is_dropped(self):
        m = _load()
        out, dropped = m.filter_patch(_SRC_HUNK + _CONF_HUNK, ["benchmarks/"])
        assert "asv.conf.json" not in out
        assert dropped == 1

    def test_nested_asv_config_is_dropped(self):
        m = _load()
        nested = _CONF_HUNK.replace("asv.conf.json", "asv_benchmarks/asv.test.conf.json")
        out, dropped = m.filter_patch(_SRC_HUNK + nested, ["benchmarks/"])
        assert "asv.test.conf.json" not in out
        assert dropped == 1

    def test_similarly_named_source_dir_is_not_dropped(self):
        """'benchmarks_util/' must not match the 'benchmarks/' prefix."""
        m = _load()
        other = _SRC_HUNK.replace("pkg/core.py", "benchmarks_util/helpers.py")
        out, dropped = m.filter_patch(other, ["benchmarks/"])
        assert "benchmarks_util/helpers.py" in out
        assert dropped == 0

    def test_empty_patch_yields_empty_output(self):
        m = _load()
        out, dropped = m.filter_patch("", ["benchmarks/"])
        assert out == ""
        assert dropped == 0

    def test_text_without_any_diff_header_does_not_raise(self):
        """Content before the first `diff --git` header is preamble and must
        pass through untouched — dropping it would corrupt the patch."""
        m = _load()
        text = "this is not a diff\nat all\n"
        out, dropped = m.filter_patch(text, ["benchmarks/"])
        assert dropped == 0
        assert out == text


class TestMain:
    def _git_repo(self, tmp_path: Path) -> Path:
        repo = tmp_path / "repo"
        (repo / "pkg").mkdir(parents=True)
        (repo / "benchmarks").mkdir()
        (repo / "pkg" / "core.py").write_text("def f():\n    return slow()\n")
        (repo / "benchmarks" / "bench_core.py").write_text("class S:\n    def time_f(self): f()\n")
        for cmd in (
            ["git", "init", "-q"],
            ["git", "config", "user.email", "t@t.t"],
            ["git", "config", "user.name", "t"],
            ["git", "add", "-A"],
            ["git", "commit", "-qm", "base"],
        ):
            subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
        return repo

    def _run(self, m, repo: Path, patch_text: str, out: Path, bench_dir="benchmarks") -> dict:
        patch_file = repo.parent / "solution.patch"
        patch_file.write_text(patch_text)
        base = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
        ).stdout.strip()
        m.main([
            "--patch",
            str(patch_file),
            "--repo",
            str(repo),
            "--benchmark-dir",
            str(repo / bench_dir) if bench_dir else "",
            "--base-sha",
            base,
            "--out",
            str(out),
        ])
        return json.loads(out.read_text())

    def test_applies_source_change_and_reports_files_changed(self, tmp_path):
        m = _load()
        repo = self._git_repo(tmp_path)
        info = self._run(m, repo, _SRC_HUNK, tmp_path / "info.json")
        assert info["present"] is True
        assert info["applied"] is True
        assert info["files_changed"] == 1
        assert "fast()" in (repo / "pkg" / "core.py").read_text()

    def test_benchmark_file_is_left_at_base(self, tmp_path):
        m = _load()
        repo = self._git_repo(tmp_path)
        info = self._run(m, repo, _SRC_HUNK + _BENCH_HUNK, tmp_path / "info.json")
        assert info["paths_excluded"] == 1
        assert info["files_changed"] == 1
        assert "f(); f()" not in (repo / "benchmarks" / "bench_core.py").read_text()

    def test_missing_patch_file_reports_present_false_and_exits_zero(self, tmp_path):
        m = _load()
        repo = self._git_repo(tmp_path)
        out = tmp_path / "info.json"
        base = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
        ).stdout.strip()
        rc = m.main([
            "--patch",
            str(tmp_path / "absent.patch"),
            "--repo",
            str(repo),
            "--benchmark-dir",
            "",
            "--base-sha",
            base,
            "--out",
            str(out),
        ])
        assert rc == 0
        info = json.loads(out.read_text())
        assert info["present"] is False
        assert info["applied"] is False

    def test_unappliable_patch_reports_applied_false_not_crash(self, tmp_path):
        """A stored patch that will also fail stage 7's solve.sh."""
        m = _load()
        repo = self._git_repo(tmp_path)
        garbage = _SRC_HUNK.replace("return slow()", "return something_else_entirely()")
        info = self._run(m, repo, garbage, tmp_path / "info.json")
        assert info["applied"] is False
        assert info["files_changed"] == 0

    def test_applied_is_derived_from_git_diff_not_patch_exit_code(self, tmp_path):
        """A patch whose only section is filtered out changes nothing, so
        applied must be False even though `patch` was never unhappy."""
        m = _load()
        repo = self._git_repo(tmp_path)
        info = self._run(m, repo, _BENCH_HUNK, tmp_path / "info.json")
        assert info["paths_excluded"] == 1
        assert info["files_changed"] == 0
        assert info["applied"] is False


# The precheck fields reproduce datasmith.verify.in_image.coverage_check (tests/verify/test_precheck.py on verify/core).
_OVERLAP_PATCH = """diff --git a/pkg/m.py b/pkg/m.py
--- a/pkg/m.py
+++ b/pkg/m.py
@@ -10,7 +10,7 @@ def f():
 a
-b
+c
diff --git a/pkg/new.py b/pkg/new.py
--- /dev/null
+++ b/pkg/new.py
@@ -0,0 +1,2 @@
+x
"""


def _image_diff(start, n, path="pkg/m.py"):
    return f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -{start},{n} +{start},{n} @@\n-x\n+y\n"


class TestPrecheckFields:
    def test_edit_inside_or_next_to_a_hunk_overlaps(self):
        m = _load()
        assert m.overlapping(_image_diff(12, 1), _OVERLAP_PATCH) == ["pkg/m.py"]
        assert m.overlapping(_image_diff(16, 3), _OVERLAP_PATCH) == ["pkg/m.py"]
        assert m.overlapping(_image_diff(9, 0), _OVERLAP_PATCH) == ["pkg/m.py"]

    def test_edit_elsewhere_or_in_another_file_does_not_overlap(self):
        m = _load()
        assert m.overlapping(_image_diff(200, 4), _OVERLAP_PATCH) == []
        assert m.overlapping(_image_diff(3, 6), _OVERLAP_PATCH) == []
        assert m.overlapping(_image_diff(12, 1, "pkg/other.py"), _OVERLAP_PATCH) == []

    def _repo(self, tmp_path):
        repo = TestMain()._git_repo(tmp_path)
        (repo / "pkg" / "core.py").write_text(
            "def f():\n    return slow()\n" + "".join(f"x{i} = {i}\n" for i in range(30))
        )
        (repo / "pkg" / "other.py").write_text("y = 1\n")
        for cmd in (["git", "add", "-A"], ["git", "commit", "-qm", "more"]):
            subprocess.run(cmd, cwd=repo, check=True, capture_output=True)
        return repo

    def _run(self, m, repo, monkeypatch, tmp_path):
        seen = []
        monkeypatch.setattr(m, "deps_counts", lambda db, changed: seen.append(sorted(changed)) or (2, 1))
        (tmp_path / "deps.db").write_text("")
        patch = (
            _SRC_HUNK + "diff --git a/pkg/new.py b/pkg/new.py\n--- /dev/null\n+++ b/pkg/new.py\n@@ -0,0 +1 @@\n+z = 1\n"
        )
        (tmp_path / "p.patch").write_text(patch)
        base = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
        out = tmp_path / "info.json"
        args = ["--patch", str(tmp_path / "p.patch"), "--repo", str(repo), "--base-sha", base, "--out", str(out)]
        m.main([*args, "--deps-db", str(tmp_path / "deps.db")])
        return json.loads(out.read_text()), seen

    def test_image_edits_are_reported_and_left_out_of_the_patch_files(self, tmp_path, monkeypatch):
        m = _load()
        repo = self._repo(tmp_path)
        core = (repo / "pkg" / "core.py").read_text()
        (repo / "pkg" / "core.py").write_text(core.replace("x25 = 25", "x25 = 0"))
        (repo / "pkg" / "other.py").write_text("y = 2\n")
        info, seen = self._run(m, repo, monkeypatch, tmp_path)
        assert info["edited_before"] == ["pkg/core.py", "pkg/other.py"]
        assert info["edited_overlap"] == []
        assert seen == [[str(repo / "pkg" / "core.py")]]
        assert (info["by_deps"], info["always_affected"]) == (2, 1)

    def test_image_edit_inside_an_oracle_hunk_overlaps(self, tmp_path, monkeypatch):
        m = _load()
        repo = self._repo(tmp_path)
        (repo / "pkg" / "core.py").write_text(
            (repo / "pkg" / "core.py").read_text().replace("def f():", "def f(): # e")
        )
        info, _ = self._run(m, repo, monkeypatch, tmp_path)
        assert info["edited_before"] == ["pkg/core.py"]
        assert info["edited_overlap"] == ["pkg/core.py"]

    def test_without_a_deps_db_the_counts_are_none(self, tmp_path):
        m = _load()
        repo = self._repo(tmp_path)
        info = TestMain()._run(m, repo, _SRC_HUNK, tmp_path / "info.json")
        assert info["edited_before"] == [] and info["by_deps"] is None and info["always_affected"] is None

    def test_deps_counts_excludes_always_affected(self, monkeypatch):
        import sqlite3
        import sys
        import types

        m = _load()
        con = sqlite3.connect(":memory:")
        con.execute("create table benchmark_meta (benchmark_id text, always_affected int)")
        con.executemany("insert into benchmark_meta values (?, ?)", [("a", 1), ("b", 0), ("c", 0)])

        class DB:
            def __init__(self, path):
                self._con = con

            def get_stored_fshas(self):
                return {}

            def get_affected_benchmark_ids(self, changed):
                return ["a", "b"] if changed else []

        mods = {"asv": None, "asv.contrib": None, "asv.contrib.lightspeed": None}
        mods["asv.contrib.lightspeed.deps_db"] = types.SimpleNamespace(LightspeedDB=DB)
        mods["asv.contrib.lightspeed.fingerprint"] = types.SimpleNamespace(
            changed_files_with_fingerprints=lambda c, f: c
        )
        for name, mod in mods.items():
            monkeypatch.setitem(sys.modules, name, mod or types.ModuleType(name))
        assert m.deps_counts("db", ["/r/pkg/core.py"]) == (1, 1)
        assert m.deps_counts("db", []) == (0, 1)
