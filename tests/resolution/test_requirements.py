"""The parser must never corrupt a marker, and never let one bad string kill a batch."""

from datasmith.resolution.requirements import Dropped, parse_many, parse_one, render

# The exact string fix_marker_spacing produced for apache/arrow's `pyuwsgi`
# requirement: `or` inside `platform` was substituted unanchored, so
# `sys.platform` became `sys.platf or m`. uv rejected it with
# "Expected a quoted string or a valid marker name, found 'sys.platf'" and
# aborted the whole compile. The original string parses fine; only this does not.
CORRUPTED_MARKER = "pyuwsgi; sys.platf or m!='win32'"


def test_common_markers_survive_untouched():
    # These four are the most common markers in Python packaging, and are exactly
    # what the old fix_marker_spacing regex destroyed (audit B1).
    for raw in [
        "numpy; platform_system=='Windows'",
        "foo; platform_machine=='x86_64'",
        "bar; sys_platform=='linux'",
        "baz; python_version<'3.11' and platform_system!='Darwin'",
        "qux; extra=='standard'",
    ]:
        req = parse_one(raw)
        assert req is not None, raw
        rendered = str(req)
        assert "platf or m" not in rendered
        assert "st and ard" not in rendered


def test_unparseable_is_dropped_not_rewritten():
    assert parse_one(CORRUPTED_MARKER) is None


def test_legacy_dotted_marker_survives():
    # The requirement fix_marker_spacing corrupted was itself well formed:
    # `sys.platform` is a legacy PEP 508 marker alias that the real parser
    # accepts. Dropping it would keep apache/arrow out of the corpus for the
    # same reason the regex did.
    req = parse_one("pyuwsgi;sys.platform!='win32'")
    assert req is not None
    assert "sys_platform" in str(req)


def test_one_bad_string_does_not_kill_its_siblings():
    # apache/arrow died because a single malformed requirement aborted the whole
    # compile. Every other requirement must still come through.
    reqs, dropped = parse_many([
        "numpy>=1.25",
        CORRUPTED_MARKER,
        "cython>=3.1",
    ])
    assert render(reqs) == ["cython>=3.1", "numpy>=1.25"]
    assert [d.raw for d in dropped] == [CORRUPTED_MARKER]
    assert dropped[0].reason


def test_parse_many_is_order_stable_and_deduplicates():
    a, _ = parse_many(["numpy", "cython", "numpy"])
    b, _ = parse_many(["cython", "numpy", "numpy"])
    assert render(a) == render(b) == ["cython", "numpy"]


def test_parse_many_never_raises_on_garbage():
    reqs, dropped = parse_many(["", "   ", "-r reqs.txt", "{wheel_file}", "./local"])
    assert reqs == []
    assert len(dropped) == 3  # blank and whitespace-only are skipped, not recorded


def test_dropped_is_hashable_and_frozen():
    d = Dropped(raw="x", reason="y")
    assert {d}
