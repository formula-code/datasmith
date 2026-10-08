"""The orchestrator composes the units and records provenance."""

import json
import subprocess
from dataclasses import fields
from pathlib import Path

from datasmith.resolution.declare import declare
from datasmith.resolution.models import CandidateMeta
from datasmith.resolution.orchestrator import (
    RESOLVER_VERSION,
    ResolutionResult,
    _matrix_entries,
    collect_asv_cfg,
)

# Anchor the grep at the repository, not at the current directory: run from
# anywhere else, a relative path makes grep fail, stdout comes back empty, and
# the guard passes while the dual path is still there.
ORCHESTRATOR = Path(__file__).resolve().parents[2] / "src" / "datasmith" / "resolution" / "orchestrator.py"


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
    assert ORCHESTRATOR.is_file(), f"orchestrator not found at {ORCHESTRATOR}"
    cp = subprocess.run(
        ["grep", "-n", "uv_compile_from_pyproject", str(ORCHESTRATOR)],
        capture_output=True,
        text=True,
    )
    # grep exits 0 on a match and 1 on no match; anything else is grep itself
    # failing, which would make an empty stdout meaningless.
    assert cp.returncode in (0, 1), f"grep failed ({cp.returncode}): {cp.stderr}"
    assert cp.stdout == "", f"the pyproject fast path must be gone:\n{cp.stdout}"


def test_the_dual_path_guard_sees_a_planted_reference(tmp_path):
    # The guard above is only worth having if it can fail. Prove the grep it runs
    # finds a reference, using the same invocation against a decoy file.
    decoy = tmp_path / "decoy.py"
    decoy.write_text("uv_compile_from_pyproject(path)\n")
    cp = subprocess.run(["grep", "-n", "uv_compile_from_pyproject", str(decoy)], capture_output=True, text=True)
    assert cp.returncode == 0
    assert "uv_compile_from_pyproject" in cp.stdout


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


# --- the environment_type decides which namespace the matrix keys are in ------


def test_a_conda_matrix_names_conda_packages_and_none_of_them_reach_the_seed():
    # apache/arrow, shapely, pygeos and versioned-hdf5 all declare
    # environment_type "conda". boost-cpp, libprotobuf, lz4-c and thrift-cpp are
    # not on PyPI at all, so the compile fails and the seed is lost; geos,
    # snappy, re2 and zstd DO resolve on PyPI, to unrelated projects -- shapely's
    # "geos" is a Flask application whose closure took nine packages into
    # shapely's seed.
    conda_matrix = {"Cython": [], "geos": [], "numpy": [], "boost-cpp": ["1.68.0"]}
    assert _matrix_entries(conda_matrix, "conda") == {}


def test_the_nesting_is_not_the_discriminator():
    # versioned-hdf5 nests under req and is still conda: hdf5 is a conda package.
    nested = {"req": {"numpy": [], "h5py": [], "hdf5": [], "ndindex": []}}
    assert _matrix_entries(nested, "conda") == {}
    assert set(_matrix_entries(nested, "virtualenv")) == {"numpy", "h5py", "hdf5", "ndindex"}


def test_every_conda_family_environment_is_treated_as_conda():
    matrix = {"geos": []}
    for env_type in ("conda", "mamba", "rattler", "oggm_conda", "Conda"):
        assert _matrix_entries(matrix, env_type) == {}, env_type


def test_asvs_pip_escape_hatch_survives_a_conda_matrix():
    # "pip+foo" is asv stating that foo is installed with pip, so it is a PyPI
    # name whatever the environment type is.
    assert _matrix_entries({"boost-cpp": ["1.68.0"], "pip+setuptools_scm": []}, "conda") == {"setuptools_scm": []}


def test_the_pip_prefix_is_stripped_in_a_pip_environment_too():
    assert set(_matrix_entries({"pip+tableauhyperapi": [], "pandas": []}, "virtualenv")) == {
        "tableauhyperapi",
        "pandas",
    }


def test_an_absent_environment_type_reads_as_pip():
    # asv's own default is to pick a tool at run time. Both cached configs that
    # omit it name pip packages, and the rungs above the matrix already supply
    # most seeds, so the permissive reading is the useful one.
    assert set(_matrix_entries({"Cython": [], "numpy": []}, None)) == {"Cython", "numpy"}


def test_collect_asv_cfg_reads_the_environment_type_from_the_config(monkeypatch, tmp_path):
    # A correct _matrix_entries is worth nothing if collect_asv_cfg never hands
    # it the environment_type -- which is exactly how the predecessor's
    # getattr-on-a-dict failed: right helper, dead data path.
    conda_cfg = tmp_path / "asv.conf.json"
    conda_cfg.write_text(json.dumps({"environment_type": "conda", "matrix": {"geos": [], "numpy": []}}))
    monkeypatch.setattr("datasmith.resolution.orchestrator.asv_finder", lambda commit: [conda_cfg])
    assert collect_asv_cfg(object()).matrix == {}

    pip_cfg = tmp_path / "asv_pip.conf.json"
    pip_cfg.write_text(json.dumps({"environment_type": "virtualenv", "matrix": {"rich": [], "numpy": []}}))
    monkeypatch.setattr("datasmith.resolution.orchestrator.asv_finder", lambda commit: [pip_cfg])
    assert set(collect_asv_cfg(object()).matrix) == {"rich", "numpy"}
