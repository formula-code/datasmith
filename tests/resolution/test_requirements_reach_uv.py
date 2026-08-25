"""Everything uv would have accepted must still reach uv.

Routing the resolver's input through a PEP 508 parser is only safe if the parser
is given requirement syntax. The strings that arrive are requirements-*file*
lines, and two of their shapes are not PEP 508 at all:

* ``numpy>=1.25  # pinned for the ABI`` - the comment is file syntax,
* ``https://host/foo-1.0-py3-none-any.whl`` - a bare URL, which PEP 508 cannot
  express and ``Requirement`` therefore rejects.

uv reads both. Dropping either loses the package from ``env_payload`` with no
error anywhere, which is the failure these tests exist to prevent.
"""

from __future__ import annotations

import subprocess

import pytest

from datasmith.resolution import dependency_resolver as dr
from datasmith.resolution.requirements import (
    REASON_DIRECTIVE,
    REASON_LOCAL_PATH,
    REASON_PLACEHOLDER,
    REASON_UNPARSEABLE,
    parse_many,
    parse_one,
    render,
    strip_inline_comment,
    to_requirement_lines,
)

WHEEL_URL = "https://files.pythonhosted.org/packages/x/foo-1.0-py3-none-any.whl"
VCS_URL = "git+https://github.com/airspeed-velocity/asv"
CORRUPTED_MARKER = "pyuwsgi; sys.platf or m!='win32'"


# --- requirements-file syntax -------------------------------------------------


def test_inline_comment_is_file_syntax_not_part_of_the_requirement():
    req = parse_one("numpy>=1.25  # pinned for the ABI")
    assert req is not None
    assert str(req) == "numpy>=1.25"


def test_comment_only_line_is_not_reported_as_a_loss():
    reqs, dropped = parse_many(["# just a note", "numpy"])
    assert render(reqs) == ["numpy"]
    assert dropped == []


def test_url_fragment_is_not_a_comment():
    # No whitespace before the '#', so it belongs to the URL.
    line = "foo @ https://h/foo-1.0-py3-none-any.whl#sha256=ab"
    assert strip_inline_comment(line) == line


def test_marker_survives_comment_stripping():
    req = parse_one("numpy; platform_system=='Windows'   # windows only")
    assert req is not None
    assert "platform_system" in str(req)


def test_bare_wheel_url_survives_to_uv():
    lines, dropped = to_requirement_lines([WHEEL_URL, "numpy>=1.25  # pinned"])
    assert lines == [WHEEL_URL, "numpy>=1.25"]
    assert dropped == []


def test_vcs_url_survives_to_uv():
    lines, dropped = to_requirement_lines([VCS_URL])
    assert lines == [VCS_URL]
    assert dropped == []


def test_a_url_with_whitespace_is_not_passed_through():
    lines, dropped = to_requirement_lines(["https://h/foo 1.0.whl"])
    assert lines == []
    assert [d.reason for d in dropped] == [REASON_UNPARSEABLE]


def test_to_requirement_lines_accounts_for_every_input():
    raws = ["numpy>=1.25", WHEEL_URL, "-r reqs.txt", "./local", CORRUPTED_MARKER]
    lines, dropped = to_requirement_lines(raws)
    assert len(lines) + len(dropped) == len(raws)
    assert {d.raw for d in dropped} == {"-r reqs.txt", "./local", CORRUPTED_MARKER}


def test_to_requirement_lines_is_order_stable_and_deduplicates():
    a, _ = to_requirement_lines(["numpy", "https://h/x-1.0.tar.gz", "numpy"])
    b, _ = to_requirement_lines(["https://h/x-1.0.tar.gz", "numpy", "numpy"])
    assert a == b == ["https://h/x-1.0.tar.gz", "numpy"]


def test_drop_reasons_name_the_actual_cause():
    _reqs, dropped = parse_many(["-r reqs.txt", "{wheel_file}", "./local", CORRUPTED_MARKER])
    reasons = {d.raw: d.reason for d in dropped}
    assert reasons["-r reqs.txt"] == REASON_DIRECTIVE
    assert reasons["{wheel_file}"] == REASON_PLACEHOLDER
    assert reasons["./local"] == REASON_LOCAL_PATH
    assert reasons[CORRUPTED_MARKER] == REASON_UNPARSEABLE
    assert len(set(reasons.values())) == 4


# --- the text actually handed to uv -------------------------------------------


@pytest.fixture
def captured_uv(monkeypatch):
    """Capture the requirements text every uv call is given, and run nothing."""
    seen: list[str] = []

    def fake_run_uv(args, *, input_text=None, cwd=None, extra_env=None, check=False):
        seen.append(input_text or "")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(dr, "run_uv", fake_run_uv)
    return seen


CANDIDATES = [
    "numpy>=1.25  # pinned for the ABI",
    WHEEL_URL,
    "scipy>=1.10",
]


def test_compile_hands_uv_the_comment_line_and_the_url(captured_uv):
    dr.uv_compile(CANDIDATES, python_version=None, cutoff_rfc3339=None)
    assert len(captured_uv) == 1
    lines = captured_uv[0].splitlines()
    assert "numpy>=1.25" in lines
    assert WHEEL_URL in lines
    assert "scipy>=1.10" in lines


def test_dry_run_install_hands_uv_the_same_lines(captured_uv):
    dr.uv_dry_run_install(CANDIDATES, python_version=None)
    lines = captured_uv[0].splitlines()
    assert "numpy>=1.25" in lines
    assert WHEEL_URL in lines


# ``uv_install_real`` had a test here too. The host-side real install is deleted
# outright by the redesign (design doc §5): it installs against a different
# interpreter and a different base image from the container, so it proved nothing
# about the build while dominating the stage's runtime. The two uv entry points
# that survive are covered above.


def test_nothing_uv_would_reject_is_handed_to_it(captured_uv):
    dr.uv_compile(["numpy", "-r reqs.txt", "{wheel}", CORRUPTED_MARKER], python_version=None, cutoff_rfc3339=None)
    assert captured_uv[0].splitlines() == ["numpy"]


def test_a_dropped_line_is_reported_not_silent(captured_uv, caplog):
    with caplog.at_level("DEBUG", logger="datasmith.resolution.dependency_resolver"):
        dr.uv_compile(["numpy", CORRUPTED_MARKER], python_version=None, cutoff_rfc3339=None)
    text = caplog.text
    assert "dropped 1 requirement" in text
    assert CORRUPTED_MARKER in text
    assert REASON_UNPARSEABLE in text
