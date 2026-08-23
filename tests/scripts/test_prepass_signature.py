"""A failure row must name its cause, not our own output markers.

The first version took line 0 of the build log, which is BuildKit preamble, and
reported `pely::env=asv_3.8` and `tions:`. The second took the last meaningful
line, which is our own protocol trailer, and reported
`FORMULACODE_SNAPSHOT_END` for six of 24 repositories.

Both named nothing, and a signature that names nothing sends the reader looking
in the wrong place. Cases below are verbatim tails from the 2026-08-23 trial.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="module")
def sig():
    spec = importlib.util.spec_from_file_location("_prepass", _ROOT / "scripts" / "prepass_trial.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_prepass"] = mod
    spec.loader.exec_module(mod)
    return mod._signature


_TRAILER = (
    "FORMULACODE_TESTS_START\n"
    '{"error": 0, "failed": 0, "passed": 0, "total": 0}\n'
    "FORMULACODE_TESTS_END\n"
    "FORMULACODE_SNAPSHOT_START\n"
    '{"benchmark_dir": "", "failed": 1, "total": 0}\n'
    "FORMULACODE_SNAPSHOT_END"
)

CASES = [
    (
        "backend",
        '2.153   File "/opt/.../_impl.py", line 402, in _call_hook\n'
        "2.153 pip._vendor.pyproject_hooks._impl.BackendUnavailable: Cannot import 'hatchling.build'\n"
        '------\nERROR: failed to build: failed to solve: process "/bin/sh -c ..."',
        "BackendUnavailable",
    ),
    (
        "collection",
        "ERROR docs/conf.py\n!!!!!!! Interrupted: 2 errors during collection !!!!!!!\n" + _TRAILER,
        "Interrupted: 2 errors during collection",
    ),
    (
        "missing module",
        "8.679     from salem.gis import Grid\n8.679 ModuleNotFoundError: No module named 'salem'\n"
        "------\nERROR: failed to build: failed to solve",
        "No module named 'salem'",
    ),
    (
        "vanished sha",
        "0.313 fatal: reference is not a tree: f77206d4757027e373365a9af1298a2b2a803361\n------",
        "reference is not a tree",
    ),
    (
        "unsatisfiable pin",
        "5.540       Because there is no version of warp-lang==1.13.0.dev20260302 and\n"
        "5.540       requirements are unsatisfiable.\n------",
        "unsatisfiable",
    ),
]


@pytest.mark.parametrize(("name", "message", "expected"), CASES, ids=[c[0] for c in CASES])
def test_signature_names_the_cause(sig, name: str, message: str, expected: str) -> None:
    assert expected in sig({"error_message": message})


def test_our_own_trailer_is_never_the_signature(sig) -> None:
    """Six of 24 rows reported FORMULACODE_SNAPSHOT_END, which names nothing."""
    result = sig({"error_message": "1522 passed, 6 failed\n" + _TRAILER})
    assert "FORMULACODE_" not in result
    assert not result.startswith("{")


def test_buildkit_preamble_is_never_the_signature(sig) -> None:
    """The first version reported line 0, which is BuildKit framing."""
    message = (
        "> [run 2/2] RUN --mount=type=cache,target=/opt/uvcache ...\n"
        "0.911 No `asv.conf` file found for valid extensions: ['.json', '.jsonc'].\n"
        "------\nERROR: failed to build: failed to solve"
    )
    assert "asv.conf" in sig({"error_message": message})


def test_an_empty_message_says_so(sig) -> None:
    assert sig({"error_message": ""}) == "no signature"


def test_a_pipe_cannot_break_the_markdown_table(sig) -> None:
    result = sig({"error_message": "ModuleNotFoundError: No module named 'a|b'"})
    assert "|" not in result
