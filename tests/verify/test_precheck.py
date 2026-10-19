from datasmith.verify.in_image import coverage_check as cc
from datasmith.verify.precheck import decide

PATCH = """cat > /workspace/repo/solution_patch.diff << '__SOLUTION__'
diff --git a/pkg/m.py b/pkg/m.py
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
__SOLUTION__
"""


def image_diff(start, n, path="pkg/m.py"):
    return f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -{start},{n} +{start},{n} @@\n-x\n+y\n"


def test_edit_inside_or_next_to_a_hunk_overlaps():
    assert cc.overlapping(image_diff(12, 1), PATCH) == ["pkg/m.py"]
    assert cc.overlapping(image_diff(16, 3), PATCH) == ["pkg/m.py"]
    assert cc.overlapping(image_diff(9, 0), PATCH) == ["pkg/m.py"]


def test_edit_elsewhere_or_in_another_file_does_not_overlap():
    assert cc.overlapping(image_diff(200, 4), PATCH) == []
    assert cc.overlapping(image_diff(3, 6), PATCH) == []
    assert cc.overlapping(image_diff(12, 1, "pkg/other.py"), PATCH) == []


CHANGED = {"pkg/m.py", "tests/test_m.py"}


def result(**kw):
    return {
        "benchmarks": 10,
        "always_affected": 0,
        "by_deps": 3,
        "patch_failed": [],
        "edited_before": [],
        "edited_overlap": [],
        **kw,
    }


def test_decide_end_states():
    assert decide(result(), CHANGED) is None
    assert decide(result(edited_overlap=["pkg/m.py"]), CHANGED) == "base_edited"
    assert decide(result(edited_before=["pkg/m.py"]), CHANGED) is None
    assert decide(result(patch_failed=["pkg/m.py"]), CHANGED) == "patch_failed"
    assert decide(result(patch_failed=["tests/test_m.py"]), CHANGED) is None
    assert decide(result(by_deps=0), CHANGED) == "uncovered"
    assert decide({"error": "boom"}, CHANGED) is None


def test_decide_without_overlap_field_counts_any_edit_in_a_changed_file():
    r = result(edited_before=["pkg/m.py"])
    del r["edited_overlap"]
    assert decide(r, CHANGED) == "base_edited"


def test_uncovered_kinds_measured_on_request():
    assert decide(result(by_deps=0, always_affected=10), CHANGED, {"always_affected"}) is None
    assert decide(result(by_deps=0), {"pkg/_fast.pyx"}, {"compiled"}) is None
    assert decide(result(by_deps=0), CHANGED, {"compiled"}) == "uncovered"
