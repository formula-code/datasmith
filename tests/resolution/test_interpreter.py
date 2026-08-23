"""The interpreter is a decision with a recorded reason, not a control-flow accident."""

import datetime as dt

import pytest

from datasmith.resolution.interpreter import (
    InterpreterChoice,
    select_interpreter,
    trove_versions_from_classifiers,
)

JAN_2026 = dt.datetime(2026, 1, 15, tzinfo=dt.UTC)
JUL_2020 = dt.datetime(2020, 7, 2, tzinfo=dt.UTC)


def test_rung_1_requires_python_wins():
    c = select_interpreter(requires_python=">=3.9,<3.12", trove_versions=[], asv_pythons=[], commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.11", source="requires-python")


def test_rung_2_trove_when_no_requires_python():
    c = select_interpreter(requires_python=None, trove_versions=["3.8", "3.9"], asv_pythons=[], commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.9", source="trove")


def test_rung_3_asv_when_neither():
    c = select_interpreter(requires_python=None, trove_versions=[], asv_pythons=["3.10"], commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.10", source="asv")


def test_rung_4_commit_date_when_nothing_is_declared():
    c = select_interpreter(requires_python=None, trove_versions=[], asv_pythons=[], commit_date=JUL_2020)
    assert c.source == "commit-date"
    # 3.9 was released 2020-10-05, after this commit.
    assert c.version == "3.8"


def test_never_picks_an_interpreter_that_did_not_exist_yet():
    c = select_interpreter(requires_python=">=3.8", trove_versions=[], asv_pythons=[], commit_date=JUL_2020)
    assert c.version == "3.8"


def test_unsatisfiable_declaration_falls_through_to_the_next_rung():
    # pymc declares ">=3.6,<3.7"; nothing in the supported range satisfies it.
    c = select_interpreter(requires_python=">=3.6,<3.7", trove_versions=["3.10"], asv_pythons=[], commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.10", source="trove")


def test_malformed_requires_python_does_not_raise():
    c = select_interpreter(
        requires_python="not a specifier", trove_versions=[], asv_pythons=["3.11"], commit_date=JAN_2026
    )
    assert c.source == "asv"


def test_selection_is_deterministic():
    kw = {
        "requires_python": ">=3.9",
        "trove_versions": ["3.9", "3.10"],
        "asv_pythons": ["3.11"],
        "commit_date": JAN_2026,
    }
    assert select_interpreter(**kw) == select_interpreter(**kw)


@pytest.mark.parametrize(
    "classifiers,expected",
    [
        (["Programming Language :: Python :: 3.11"], ["3.11"]),
        (["Programming Language :: Python :: 3 :: Only"], []),
        (["Programming Language :: Python :: 3.9", "License :: OSI Approved"], ["3.9"]),
        ([], []),
    ],
)
def test_trove_extraction(classifiers, expected):
    assert trove_versions_from_classifiers(classifiers) == expected


def test_asv_rung_reads_the_tuples_the_repo_actually_holds():
    # ASVCfgAggregate.pythons is set[tuple[int, ...]]. Read as str((3, 10)) it
    # says "(3, 10)", matches nothing, and the choice drops silently to the
    # commit-date default.
    c = select_interpreter(requires_python=None, trove_versions=[], asv_pythons={(3, 10)}, commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.10", source="asv")


def test_asv_rung_narrows_a_patch_version_to_its_minor():
    c = select_interpreter(requires_python=None, trove_versions=[], asv_pythons=["3.10.2"], commit_date=JAN_2026)
    assert c == InterpreterChoice(version="3.10", source="asv")
