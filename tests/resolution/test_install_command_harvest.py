"""Install commands must not contribute bogus package names.

The loop that harvests requirements from an ASV `install_command` fed every
non-flag token to `normalize_requirement`. It was dead while the config read
was a no-op, and reading the config correctly revived it.

ASV's own default install command is:

    in-dir={env_dir} python -mpip install {wheel_file}

which yielded the packages `python` and `install`. Both exist on PyPI, so the
resolver installed real but wrong distributions instead of erroring.
"""

from __future__ import annotations

import shlex

from datasmith.resolution.orchestrator import _requirements_from_install_tokens


def _harvest(cmd: str) -> set[str]:
    return _requirements_from_install_tokens(shlex.split(cmd))


class TestAsvDefaults:
    def test_asv_default_install_command_yields_nothing(self):
        """The regression. It installs the project's own wheel, not a dependency."""
        assert _harvest("in-dir={env_dir} python -mpip install {wheel_file}") == set()

    def test_python_is_never_harvested(self):
        assert "python" not in _harvest("in-dir={env_dir} python -mpip install {wheel_file}")

    def test_install_is_never_harvested(self):
        assert "install" not in _harvest("in-dir={env_dir} python -mpip install {wheel_file}")

    def test_asv_default_with_no_deps_flag(self):
        assert _harvest("in-dir={env_dir} python -mpip install --no-deps {wheel_file}") == set()


class TestRealPackages:
    def test_named_packages_are_harvested(self):
        assert _harvest("python -m pip install numpy scipy") == {"numpy", "scipy"}

    def test_a_pin_is_preserved(self):
        assert _harvest("pip install cython==0.29.21") == {"cython==0.29.21"}

    def test_requirements_file_is_left_to_the_dedicated_branch(self):
        assert _harvest("pip install -r requirements.txt") == set()

    def test_a_path_is_the_project_not_a_dependency(self):
        assert _harvest("pip install /workspace/repo") == set()
        assert _harvest("pip install .") == set()

    def test_a_wheel_file_is_not_a_dependency(self):
        assert _harvest("pip install dist/foo-1.0-py3-none-any.whl") == set()


class TestNoInstallVerb:
    def test_a_command_without_install_yields_nothing(self):
        assert _harvest("python setup.py build_ext --inplace") == set()

    def test_uv_pip_install_still_works(self):
        assert _harvest("uv pip install pandas") == {"pandas"}
