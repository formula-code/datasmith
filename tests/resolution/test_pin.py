"""The seed carries project dependencies. The base image owns tooling."""

import datetime as dt

import pytest

from datasmith.resolution.declare import Declared
from datasmith.resolution.pin import TOOLING_OWNED_BY_BASE_IMAGE, pin

JAN_2026 = dt.datetime(2026, 1, 15, tzinfo=dt.UTC)


@pytest.fixture
def fake_compile(monkeypatch):
    """Record what reached uv, and return it pinned."""
    calls = {}

    def _compile(requirements, *, python_version, cutoff_rfc3339):
        calls["requirements"] = sorted(requirements)
        calls["cutoff"] = cutoff_rfc3339
        return [f"{r}==1.0" for r in sorted(requirements)]

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    return calls


def test_tooling_never_reaches_the_seed(fake_compile):
    declared = Declared(runtime=["numpy", "pytest", "asv", "hypothesis", "setuptools"], build=["cython"])
    result = pin(declared, python_version="3.11", commit_date=JAN_2026)
    for name in TOOLING_OWNED_BY_BASE_IMAGE:
        assert not any(line.startswith(name) for line in result.requirements), name
    assert any(line.startswith("numpy") for line in result.requirements)


@pytest.mark.parametrize("tool", sorted(TOOLING_OWNED_BY_BASE_IMAGE))
def test_tooling_that_arrives_transitively_is_stripped_too(monkeypatch, tool):
    # Stripping the input is not enough. setuptools-scm is not tooling, compiles
    # cleanly, and requires setuptools -- so tqdm's seed carried
    # setuptools==75.4.0 even though the setuptools tqdm declared was removed
    # before the compile. The transitive pin starts the same version fight with
    # the base image that the exclusion exists to prevent.
    def _compile(requirements, *, python_version, cutoff_rfc3339):
        # The resolver returns a closure, not an echo: the caller never named the
        # tool, and cannot have stripped it from the input.
        return [f"{tool}==75.4.0", "setuptools-scm==8.1.0", "numpy==2.0"]

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    result = pin(Declared(runtime=["numpy"], build=["setuptools-scm"]), python_version="3.11", commit_date=JAN_2026)
    assert not any(line.lower().startswith(f"{tool}==") for line in result.requirements), result.requirements
    assert "numpy==2.0" in result.requirements
    assert "setuptools-scm==8.1.0" in result.requirements


def test_a_compiled_line_the_parser_cannot_read_is_kept(monkeypatch):
    # uv can emit a direct URL, which has no PEP 508 form. Dropping every line
    # the parser refuses would silently delete a real requirement.
    url = "https://files.example.org/wheels/foo-1.0-py3-none-any.whl"

    monkeypatch.setattr(
        "datasmith.resolution.pin.uv_compile",
        lambda requirements, *, python_version, cutoff_rfc3339: [url, "setuptools==75.4.0"],
    )
    result = pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert result.requirements == [url]


def test_build_requires_are_included(fake_compile):
    declared = Declared(runtime=["numpy"], build=["cython", "meson-python"])
    pin(declared, python_version="3.11", commit_date=JAN_2026)
    assert "cython" in fake_compile["requirements"]
    assert "meson-python" in fake_compile["requirements"]


def test_extras_are_excluded_by_default(fake_compile):
    declared = Declared(runtime=["numpy"], extras={"docs": ["sphinx"]})
    pin(declared, python_version="3.11", commit_date=JAN_2026)
    assert "sphinx" not in fake_compile["requirements"]


def test_named_extras_are_included_when_requested(fake_compile):
    declared = Declared(runtime=["numpy"], extras={"docs": ["sphinx"], "gui": ["qt"]})
    pin(declared, python_version="3.11", commit_date=JAN_2026, extras=["gui"])
    assert "qt" in fake_compile["requirements"]
    assert "sphinx" not in fake_compile["requirements"]


def test_operator_pins_are_added(fake_compile):
    declared = Declared(runtime=["numpy"])
    pin(declared, python_version="3.11", commit_date=JAN_2026, operator_pins=["zarr==2.16.0"])
    assert "zarr==2.16.0" in fake_compile["requirements"]


def test_cutoff_is_applied_first(fake_compile):
    pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert fake_compile["cutoff"] is not None
    assert fake_compile["cutoff"].startswith("2026-01-15")


def test_cutoff_is_relaxed_on_failure_and_recorded(monkeypatch):
    attempts = []

    def _compile(requirements, *, python_version, cutoff_rfc3339):
        attempts.append(cutoff_rfc3339)
        if cutoff_rfc3339 is not None:
            raise RuntimeError("no solution with exclude-newer")
        return ["numpy==2.0"]

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    result = pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert len(attempts) == 2
    assert result.cutoff_relaxed is True
    assert result.cutoff_used is None
    assert result.requirements == ["numpy==2.0"]


def test_total_failure_returns_empty_and_records_why(monkeypatch):
    def _compile(requirements, *, python_version, cutoff_rfc3339):
        raise RuntimeError("unsatisfiable")

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _compile)
    result = pin(Declared(runtime=["numpy"]), python_version="3.11", commit_date=JAN_2026)
    assert result.requirements == []
    assert result.dropped and "unsatisfiable" in result.dropped[0].reason


def test_empty_declaration_compiles_nothing(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("uv must not be invoked for an empty declaration")

    monkeypatch.setattr("datasmith.resolution.pin.uv_compile", _boom)
    result = pin(Declared(), python_version="3.11", commit_date=JAN_2026)
    assert result.requirements == []
