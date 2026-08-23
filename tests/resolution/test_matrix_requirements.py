"""ASV's matrix maps package name to required versions.

The previous code iterated the values and discarded the keys, so a pinned entry
became a bare version string that the resolver would receive as a package name.
"""

from __future__ import annotations

from datasmith.resolution.orchestrator import matrix_requirements


def test_pinned_version_becomes_an_equality_requirement():
    assert matrix_requirements({"Cython": {"0.29.21"}}) == {"cython==0.29.21"}


def test_unpinned_package_becomes_a_bare_requirement():
    assert matrix_requirements({"numpy": set()}) == {"numpy"}


def test_bare_version_string_is_never_emitted_alone():
    """The old behaviour. `0.29.21` must never appear as a package name."""
    out = matrix_requirements({"Cython": {"0.29.21"}})
    assert "0.29.21" not in out


def test_several_versions_yield_several_requirements():
    out = matrix_requirements({"numpy": {"1.25.0", "1.26.0"}})
    assert out == {"numpy==1.25.0", "numpy==1.26.0"}


def test_conda_only_package_is_still_emitted():
    """boost-cpp is a conda package. Filtering happens downstream, not here."""
    assert matrix_requirements({"boost-cpp": {"1.68.0"}}) == {"boost-cpp==1.68.0"}


def test_empty_matrix_yields_nothing():
    assert matrix_requirements({}) == set()


def test_flag_like_keys_are_skipped():
    assert matrix_requirements({"-e": {"1.0"}, "numpy": set()}) == {"numpy"}
