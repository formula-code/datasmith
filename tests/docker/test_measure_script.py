"""Static guards on measure.sh and the image plumbing that carries it.

These are textual assertions on scripts that cannot be executed without a
container. They are a floor, not a ceiling — the executable proof is the
slow integration test in tests/docker/test_manifest_integration.py.
"""

from pathlib import Path

from datasmith.docker.context import DockerContext

_T = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates"


class TestMeasureSh:
    def _src(self) -> str:
        return (_T / "measure.sh").read_text()

    def test_captures_base_sha_before_applying_the_patch(self) -> None:
        """Provenance depends on ordering: the sha must be read before
        anything touches the tree."""
        src = self._src()
        assert src.index("git rev-parse HEAD") < src.index("apply_oracle_patch.py")

    def test_measures_baseline_before_applying_the_patch(self) -> None:
        """The failure this guards is shapely's: baselines measured AFTER
        the patch, which collapses every speedup to ~1.0."""
        src = self._src()
        assert src.index("lsv_init.py") < src.index("apply_oracle_patch.py")

    def test_measures_impact_after_applying_the_patch(self) -> None:
        src = self._src()
        assert src.index("apply_oracle_patch.py") < src.index("lsv_measure.py")

    def test_emits_the_block_last(self) -> None:
        src = self._src()
        assert src.index("lsv_measure.py") < src.index("emit_measure.py")

    def test_shebang_is_on_the_first_line(self) -> None:
        """Dockerfile.pr chmod +x's this script, so the kernel may exec it
        directly — and the kernel only honours `#!` at byte 0.

        run-tests.sh puts the t-bench canary comment ABOVE its shebang and
        gets away with it only because the repo image's
        ENTRYPOINT ["/bin/bash"] runs it through an explicit interpreter.
        Copying that layout here produced a real
        `exec /measure.sh: exec format error`.
        """
        lines = self._src().splitlines()
        assert lines[0] == "#!/usr/bin/env bash", f"first line is {lines[0]!r}"

    def test_canary_marker_is_retained(self) -> None:
        """Moving the shebang must not drop the training-corpus canary."""
        assert "t-bench-canary GUID FORMULACODE-" in self._src()

    def test_rounds_are_env_overridable(self) -> None:
        assert "DATASMITH_VERIFY_MEASURE_ROUNDS" in self._src()

    def test_snapshot_capture_is_disabled(self) -> None:
        """lsv_init.py runs snapshot-tool only when HARBOR_AGENT_NAME is
        'oracle'; verification must not pay for it."""
        src = self._src()
        assert "HARBOR_AGENT_NAME" in src
        assert "oracle" not in src.split("HARBOR_AGENT_NAME")[1].split("\n")[0]

    def test_python_runner_falls_back_when_micromamba_is_absent(self) -> None:
        """Without this fallback the script is untestable outside a 7-13GB
        task image, and an image whose env activation failed would emit no
        block at all rather than a block saying nothing was measured."""
        src = self._src()
        assert "PY_RUN" in src
        assert "python3" in src

    def test_no_bare_micromamba_run_invocations_remain(self) -> None:
        """Every python invocation must go through PY_RUN, or the fallback
        is decorative."""
        src = self._src()
        stripped = src.replace('PY_RUN=(micromamba run -n "$ENV_NAME" python)', "")
        assert "micromamba run -n" not in stripped


class TestDockerfilePlumbing:
    def _src(self) -> str:
        return (_T / "Dockerfile.pr").read_text()

    def test_measure_scripts_are_copied(self) -> None:
        src = self._src()
        for name in ("measure.sh", "apply_oracle_patch.py", "emit_measure.py"):
            assert name in src, f"Dockerfile.pr does not COPY {name}"

    def test_lsv_helpers_are_copied(self) -> None:
        src = self._src()
        for name in ("lsv_init.py", "lsv_measure.py", "parser.py"):
            assert name in src, f"Dockerfile.pr does not COPY {name}"

    def test_solution_patch_is_never_copied_into_the_image(self) -> None:
        """A published task image containing the oracle solution would be
        readable by the agent under evaluation at trial time.

        This static guard is load-bearing: the dynamic equivalent would need
        a real 7-13GB task image, which CI cannot afford at 98% disk. Paired
        with test_solution_patch_is_not_a_context_file below, the two cover
        both routes a file can reach an image layer.

        Scans COPY/ADD directives specifically rather than the whole file:
        the Dockerfile carries a comment explaining why the patch is mounted
        instead of copied, and that comment must not trip the guard.
        """
        directives = [ln.strip() for ln in self._src().splitlines() if ln.strip().upper().startswith(("COPY ", "ADD "))]
        assert directives, "no COPY directives found — the guard would pass vacuously"
        offenders = [ln for ln in directives if "solution.patch" in ln]
        assert not offenders, f"solution.patch reaches an image layer via: {offenders}"

    def test_solution_patch_is_not_a_context_file(self) -> None:
        """The second route into an image: DockerContext round-trips every
        _FILE_MAP entry into the build context directory."""
        assert "solution.patch" not in DockerContext._FILE_MAP

    def test_sealer_exit_status_chain_is_intact(self) -> None:
        """Regression guard inherited from commit 0c38513: the final RUN must
        capture docker_build_final.sh's status and exit with it, so a failing
        build script cannot be masked by the sealer's `|| true`."""
        src = self._src()
        assert "rc=$?" in src
        assert "exit $rc" in src


class TestLsvInstall:
    def test_final_stage_installs_lsv(self) -> None:
        src = (_T / "docker_build_final.sh").read_text()
        assert "formula-code/lsv" in src
