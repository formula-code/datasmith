"""The package root must not depend on dict iteration order."""

from datasmith.resolution.metadata_parser import select_primary_candidate
from datasmith.resolution.models import Candidate, CandidateMeta


def _cand(root: str, *, pyproject: bool = False) -> Candidate:
    c = Candidate(root_relpath=root)
    if pyproject:
        c.pyproject_path = object()  # truthy sentinel; only its presence is read
    return c


def test_same_candidates_in_any_order_give_the_same_root():
    roots = ["python", "binder", "scippy"]
    analyzed = {r: CandidateMeta() for r in roots}

    forward = {r: _cand(r, pyproject=True) for r in roots}
    reverse = {r: _cand(r, pyproject=True) for r in reversed(roots)}

    a = select_primary_candidate("scipp/scipp", forward, set(), analyzed)
    b = select_primary_candidate("scipp/scipp", reverse, set(), analyzed)
    assert a == b


def test_name_match_beats_position():
    # The declared name is compared against the repository suffix verbatim, so
    # arrow's real ``pyarrow`` would not match ``arrow`` and this would test the
    # sorted fallback instead. Without a name match the answer is "other".
    cands = {"python": _cand("python"), "other": _cand("other")}
    analyzed = {"python": CandidateMeta(), "other": CandidateMeta()}
    analyzed["python"].name = "arrow"
    assert select_primary_candidate("apache/arrow", cands, set(), analyzed) == "python"


def test_multiple_name_matches_resolve_deterministically():
    # ``analyzed`` is walked in insertion order, so b-then-a is what makes this
    # discriminating: unsorted, the answer would be "b".
    cands = {"b": _cand("b"), "a": _cand("a")}
    analyzed = {"b": CandidateMeta(), "a": CandidateMeta()}
    analyzed["a"].name = "thing"
    analyzed["b"].name = "thing"
    assert select_primary_candidate("x/thing", cands, set(), analyzed) == "a"


def test_shallowest_path_wins_as_the_final_tiebreak():
    cands = {"deep/nested/pkg": _cand("deep/nested/pkg"), "pkg": _cand("pkg")}
    analyzed = {k: CandidateMeta() for k in cands}
    assert select_primary_candidate("x/y", cands, set(), analyzed) == "pkg"


def test_install_command_order_does_not_decide_the_root():
    # A list stands in for the production set: a set of the same strings always
    # iterates the same way inside one process, so only an ordered input can
    # show that the order stopped deciding. The two roots differ in depth, so
    # the install commands genuinely decide here -- the sorted fallback would
    # answer "pkg", the install commands answer "deep/nested/pkg".
    cands = {"pkg": _cand("pkg"), "deep/nested/pkg": _cand("deep/nested/pkg")}
    analyzed = {k: CandidateMeta() for k in cands}
    cmds = ["pip install ./deep/nested/pkg", "pip install ./pkg"]

    forward = select_primary_candidate("x/y", cands, cmds, analyzed)
    reverse = select_primary_candidate("x/y", cands, list(reversed(cmds)), analyzed)
    assert forward == reverse == "deep/nested/pkg"


def test_install_commands_as_a_set_pick_the_same_root():
    # The production shape: a monorepo whose two asv configs each name their own
    # package root, aggregated into one set by the orchestrator.
    cands = {"pkg": _cand("pkg"), "deep/nested/pkg": _cand("deep/nested/pkg")}
    analyzed = {k: CandidateMeta() for k in cands}
    cmds = {"pip install ./deep/nested/pkg", "pip install ./pkg"}
    assert select_primary_candidate("x/y", cands, cmds, analyzed) == "deep/nested/pkg"
