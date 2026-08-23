"""The ASV config read was a no-op for the life of the project.

`json5.loads` returns a plain dict. The reader used `getattr(cfg, "pythons", [])`,
which is attribute access, so every field came back as its default. The repo's
declared Python version, dependency matrix and build commands were all discarded.
"""

from __future__ import annotations

from datasmith.resolution.orchestrator import collect_asv_cfg

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

    def test_null_version_is_dropped_not_stringified(self):
        """ASV uses null to mean "do not install". `str(None)` would leak "None"."""
        agg = collect_asv_cfg([PANDAS_CFG])
        assert agg.matrix["pytables"] == set()

    def test_grouped_matrix_reads_the_req_group(self):
        agg = collect_asv_cfg([NESTED_CFG])
        assert agg.matrix["numpy"] == {"1.26.0"}
        assert agg.matrix["scipy"] == set()
        assert "OMP_NUM_THREADS" not in agg.matrix

    def test_non_dict_config_is_skipped(self):
        agg = collect_asv_cfg([None, "not a config", PANDAS_CFG])
        assert agg.pythons == {(3, 8)}
