"""The orchestrator composes the units and records provenance."""

from dataclasses import fields

from datasmith.resolution.orchestrator import RESOLVER_VERSION, ResolutionResult


def test_result_carries_provenance():
    names = {f.name for f in fields(ResolutionResult)}
    for required in (
        "resolver_version",
        "interpreter_source",
        "cutoff_used",
        "cutoff_relaxed",
        "probe_status",
        "probe_log",
        "dropped_requirements",
        "requires_python",
        "primary_root",
    ):
        assert required in names, required


def test_resolver_version_is_set():
    assert RESOLVER_VERSION and RESOLVER_VERSION != "legacy"


def test_uv_install_real_is_deleted():
    import datasmith.resolution.dependency_resolver as dr

    assert not hasattr(dr, "uv_install_real")


def test_no_dual_path_remains():
    import subprocess

    cp = subprocess.run(
        ["grep", "-cn", "uv_compile_from_pyproject", "src/datasmith/resolution/orchestrator.py"],
        capture_output=True,
        text=True,
    )
    assert cp.stdout.strip() in ("0", ""), "the pyproject fast path must be gone"
