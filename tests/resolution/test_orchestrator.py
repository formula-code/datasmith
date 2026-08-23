"""The orchestrator composes the units and records provenance."""

from dataclasses import fields

from datasmith.resolution.declare import declare
from datasmith.resolution.models import CandidateMeta
from datasmith.resolution.orchestrator import RESOLVER_VERSION, ResolutionResult, _matrix_entries


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


# --- the ASV matrix, whose shape decides what declare() is fed ----------------


def test_nested_matrix_reads_req_and_never_the_section_names():
    # asv >= 0.6 nests requirements under "req". Read flat, "req" and "env"
    # arrive at the resolver as package names.
    entries = _matrix_entries({
        "req": {"numpy": ["1.20", ""]},
        "env": {"OMP_NUM_THREADS": ["1"]},
        "env_nobuild": {"SOMETHING": ["1"]},
    })
    assert entries == {"numpy": ["1.20", ""]}
    for absent in ("req", "env", "env_nobuild", "OMP_NUM_THREADS"):
        assert absent not in entries


def test_flat_legacy_matrix_still_names_its_packages():
    entries = _matrix_entries({"numpy": ["1.20"], "six": None})
    assert entries["numpy"] == ["1.20"]
    # ASV's null means "do not install in this combination", and declare() reads
    # the string "None" as exactly that. Losing the key would invert the answer.
    assert entries["six"] == ["None"]


def test_a_matrix_that_is_not_a_mapping_is_no_matrix():
    for shape in (None, ["numpy"], "numpy", 3):
        assert _matrix_entries(shape) == {}


def test_what_the_matrix_yields_reaches_declare_as_requirements():
    entries = _matrix_entries({"req": {"numpy": ["1.20"], "cython": [], "six": None}})
    declared = declare(CandidateMeta(), {k: set(v) for k, v in entries.items()})
    assert "numpy==1.20" in declared.runtime
    assert "cython" in declared.runtime
    assert not any(r.startswith("six") for r in declared.runtime)
