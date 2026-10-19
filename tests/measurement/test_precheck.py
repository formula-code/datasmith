from datasmith.measurement.precheck import decide

CHANGED = {"pkg/m.py", "tests/test_m.py"}


def block(**kw):
    return {"patch_applied": True, "benchmarks_impactable_n": 10, "always_affected": 0, "by_deps": 3, **kw}


def test_decide_end_states():
    assert decide(block(), CHANGED) is None
    assert decide(block(edited_overlap=["pkg/m.py"]), CHANGED) == "base_edited"
    assert decide(block(edited_before=["pkg/m.py"]), CHANGED) is None
    assert decide(block(patch_applied=False, edited_before=["pkg/m.py"]), CHANGED) == "base_edited"
    assert decide(block(patch_applied=False), CHANGED) == "patch_failed"
    assert decide(block(by_deps=0), CHANGED) == "uncovered"
    assert decide({"patch_applied": True}, CHANGED) is None


def test_uncovered_kinds_measured_on_request():
    assert decide(block(by_deps=0, always_affected=10), CHANGED, {"always_affected"}) is None
    assert decide(block(by_deps=0), {"pkg/_fast.pyx"}, {"compiled"}) is None
    assert decide(block(by_deps=0), CHANGED, {"compiled"}) == "uncovered"
