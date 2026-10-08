"""The ASV config read was a no-op for the life of the project.

`json5.loads` returns a plain dict. The reader used `getattr(cfg, "pythons", [])`,
which is attribute access, so every field came back as its default. The repo's
declared Python version, dependency matrix and build commands were all discarded.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from datasmith.resolution.orchestrator import collect_asv_cfg as _collect_asv_cfg

# Shape taken from pandas-dev/pandas asv_bench/asv.conf.json.
PANDAS_CFG = {
    "version": 1,
    "project": "pandas",
    "pythons": ["3.8"],
    "matrix": {
        "numpy": [],
        "Cython": ["0.29.21"],
        "matplotlib": [],
        "pytables": [None],
    },
    "build_command": [
        "python setup.py build -j4",
        "PIP_NO_BUILD_ISOLATION=false python -mpip wheel --no-deps -w {build_cache_dir} {build_dir}",
    ],
    "install_command": ["in-dir={env_dir} python -mpip install {wheel_file}"],
}

# Shape taken from apache/arrow python/asv.conf.json.
ARROW_CFG = {
    "version": 1,
    "pythons": ["3.9"],
    "matrix": {"boost-cpp": ["1.68.0"], "cmake": [], "cython": []},
    "build_command": ["/bin/bash {build_dir}/asv-build.sh"],
}

# ASV 0.5 and later also accept a grouped matrix.
NESTED_CFG = {
    "version": 1,
    "pythons": ["3.11"],
    "matrix": {
        "req": {"numpy": ["1.26.0"], "scipy": []},
        "env": {"OMP_NUM_THREADS": ["1"]},
    },
}


@pytest.fixture(autouse=True)
def _cfg_dir(tmp_path: Path):
    """Give the config-file reader a directory to read from.

    ``collect_asv_cfg`` takes a commit and finds the configs itself, because that
    is where the reader has to live once the config decides an interpreter. These
    tests supply the parsed shapes directly, so ``asv_finder`` is stubbed to hand
    back files holding them. Every assertion below is unchanged.
    """
    yield tmp_path


def collect_asv_cfg(cfgs: list, _dir: Path | None = None):
    """Write *cfgs* to files and read them back through the real aggregator."""
    directory = _dir or Path(collect_asv_cfg._dir)
    paths = []
    for i, cfg in enumerate(cfgs):
        f = directory / f"asv.{i}.conf.json"
        f.write_text(json.dumps(cfg))
        paths.append(f)
    with patch("datasmith.resolution.orchestrator.asv_finder", return_value=paths):
        return _collect_asv_cfg(commit=None)


@pytest.fixture(autouse=True)
def _bind_dir(tmp_path: Path):
    collect_asv_cfg._dir = str(tmp_path)
    yield


class TestPythons:
    def test_declared_python_is_read(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.pythons == {(3, 8)}

    def test_multiple_configs_union(self):
        agg = collect_asv_cfg([PANDAS_CFG, ARROW_CFG])
        assert agg.pythons == {(3, 8), (3, 9)}

    def test_absent_pythons_yields_empty_not_error(self):
        agg = collect_asv_cfg([{"version": 1}])
        assert agg.pythons == set()


class TestCommands:
    def test_build_command_is_read_and_joined(self):
        agg = collect_asv_cfg([ARROW_CFG])
        assert agg.build_commands == {"/bin/bash {build_dir}/asv-build.sh"}

    def test_mpip_is_normalised(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        joined = next(iter(agg.build_commands))
        assert "-m pip" in joined
        assert "-mpip" not in joined

    def test_install_command_is_read(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.install_commands == {"in-dir={env_dir} python -mpip install {wheel_file}"}


class TestMatrix:
    def test_keys_are_preserved(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert set(agg.matrix) >= {"numpy", "Cython", "matplotlib"}

    def test_pinned_version_is_preserved(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["Cython"] == {"0.29.21"}

    def test_unpinned_package_has_no_versions(self):
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["numpy"] == set()

    def test_null_version_survives_as_a_sentinel(self):
        """ASV's null means "do not install in this combination".

        The predecessor dropped it here, which is what this test asserted when it
        was written. That is safe only while nothing distinguishes an empty
        version set from a null one -- and ``declare`` does: an empty set means
        "any version" and emits the bare name, while a null means "never" and
        emits nothing. Collapsing them here would turn "never install pytables"
        into "install any pytables", so the sentinel is carried and resolved in
        ``_matrix_requirements``, the one reader that needs to know.
        """
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["pytables"] == {"None"}

    def test_the_null_sentinel_installs_nothing_downstream(self):
        """What the sentinel is for -- asserted where it actually matters."""
        from datasmith.resolution.declare import declare
        from datasmith.resolution.models import CandidateMeta

        agg = collect_asv_cfg([PANDAS_CFG])
        declared = declare(CandidateMeta(), agg.matrix)
        assert not any(r.startswith("pytables") for r in declared.runtime)
        # ...while a genuinely unpinned entry in the same matrix does install.
        assert "numpy" in declared.runtime

    def test_grouped_matrix_reads_the_req_group(self):
        agg = collect_asv_cfg([NESTED_CFG])
        assert agg.matrix["numpy"] == {"1.26.0"}
        assert agg.matrix["scipy"] == set()
        assert "OMP_NUM_THREADS" not in agg.matrix

    def test_non_dict_config_is_skipped(self):
        agg = collect_asv_cfg([None, "not a config", PANDAS_CFG])
        assert agg.pythons == {(3, 8)}
