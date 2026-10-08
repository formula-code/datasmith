"""What the verifier RUNS. Facts only, never verdicts.

A command that crashes yields a fact with crashed=True, and the caller turns
that into a FAILING check -- not a skipped one. That departs from
docker/manifest.py's three-valued convention on purpose: the verifier CHOSE to
run this command, so failure to execute is a finding about the container.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from datasmith.agents.reflexive.battery import BATTERY_COMMANDS, run_battery


def _fake_runner(script: dict[str, tuple[str, str, int]]):
    calls: list[list[str]] = []

    def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
        calls.append(argv)
        key = argv[0] if argv else ""
        if key == "BOOM":
            raise RuntimeError("docker daemon went away")
        return script.get(key, ("", "", 0))

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


def test_every_battery_command_produces_a_fact() -> None:
    facts = run_battery("img:1", runner=_fake_runner({}))
    assert len(facts) == len(BATTERY_COMMANDS)
    assert {f.name for f in facts} == {name for name, _ in BATTERY_COMMANDS}


def test_a_fact_carries_the_raw_output() -> None:
    marker = BATTERY_COMMANDS[0][1][0]
    runner = _fake_runner({marker: ("576 passed", "warn", 1)})
    facts = {f.name: f for f in run_battery("img:1", runner=runner)}
    first = facts[BATTERY_COMMANDS[0][0]]
    assert first.stdout == "576 passed"
    assert first.stderr == "warn"
    assert first.rc == 1
    assert first.crashed is False


def test_a_crashing_command_is_a_fact_with_crashed_true_not_an_exception() -> None:
    """run_battery must never propagate. A crash is a finding."""

    def runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
        raise RuntimeError("docker daemon went away")

    facts = run_battery("img:1", runner=runner)
    assert len(facts) == len(BATTERY_COMMANDS)
    assert all(f.crashed for f in facts)
    assert all(f.rc != 0 for f in facts), "a crashed command must never look successful"
    assert all("docker daemon went away" in f.stderr for f in facts)


def test_the_battery_never_writes_to_the_build_context() -> None:
    """Read-only posture, asserted on the actual argv we ship."""
    joined = " ".join(" ".join(argv) for _, argv in BATTERY_COMMANDS)
    for forbidden in ("docker_build_pkg.sh", "docker_build_run.sh", " > ", "tee ", "rm "):
        assert forbidden not in joined, f"battery must not use {forbidden!r}"


def test_the_battery_covers_the_facts_the_spec_names() -> None:
    names = {name for name, _ in BATTERY_COMMANDS}
    assert {"pytest_collect", "asv_discover", "source_benchmark_count", "import_sweep"} <= names


def test_every_env_only_binary_is_invoked_through_micromamba() -> None:
    """`asv` exiting 127 on PATH is the largest defect found this session.

    The first draft of BATTERY_COMMANDS reproduced it in the tool meant to
    detect it: bare `asv`, plus two absolute paths to files that do not exist
    in any image. All three were caught by running one docker command.
    """
    for name, argv in BATTERY_COMMANDS:
        joined = " ".join(argv)
        for binary in ("asv", "pytest", "pip"):
            if f" {binary} " in joined or joined.endswith(f" {binary}"):
                assert "micromamba run -n" in joined, f"{name} calls {binary} outside the env"


def test_no_battery_command_references_a_path_that_does_not_exist_in_the_image() -> None:
    """These two were invented. Neither is in any container we build."""
    joined = " ".join(" ".join(argv) for _, argv in BATTERY_COMMANDS)
    assert "/formulacode_testrunner.py" not in joined
    assert "count_source_benchmarks.py" not in joined


def test_the_battery_collects_integrity_facts() -> None:
    """Without these the negative controls cannot be rejected at all.

    attack-demo:1 does not break function, it defeats DETECTION: pytest
    passes, imports work, benchmarks discover. pysindy#139's replaced grep is
    equally invisible to the functional commands. Both are labelled `reject`,
    so the verifier needs a fact to reject them ON.
    """
    names = {name for name, _ in BATTERY_COMMANDS}
    assert "integrity_probe" in names


def test_the_probe_is_mounted_read_only_not_baked_into_the_image() -> None:
    """A probe baked at build time is a probe the producer could have edited."""
    import inspect

    from datasmith.agents.reflexive import battery

    source = inspect.getsource(battery._docker_runner)
    assert "/opt/fc_probe.py:ro" in source, "the probe mount must be read-only"


class TestPipelinesDoNotMaskFailure:
    """`cmd | tail -N` reports TAIL's exit status, not cmd's.

    Every pipelined battery command ended in `2>&1 | tail -N`, so a command
    that died still produced rc=0. Demonstrated by the Task 3 auditor: the
    payload raised SyntaxError and BatteryFact.rc was 0.

    That is the same inversion as a host-side timeout returning success, which
    silently verified about 34% of candidate_containers. A verifier that keys
    on rc would read every pipelined command as fine.
    """

    def test_every_pipelined_command_sets_pipefail(self) -> None:
        for name, argv in BATTERY_COMMANDS:
            joined = " ".join(argv)
            if "|" not in joined:
                continue
            assert "pipefail" in joined, f"{name} pipes without pipefail, so rc reports tail's status"

    def test_pipefail_actually_propagates_the_failure(self) -> None:
        """Guards the guard: prove the shell behaves as the fix assumes."""
        import subprocess

        without = subprocess.run(
            ["bash", "-lc", 'python -c "raise SystemExit(3)" 2>&1 | tail -5'],
            capture_output=True,
            text=True,
        )
        with_pf = subprocess.run(
            ["bash", "-lc", 'set -o pipefail; python -c "raise SystemExit(3)" 2>&1 | tail -5'],
            capture_output=True,
            text=True,
        )
        assert without.returncode == 0, "the masking this test exists to prevent"
        assert with_pf.returncode == 3, "pipefail must surface the real status"


def test_asv_discover_pins_the_machine_name() -> None:
    """Without --machine, asv_discover fails on EVERY container.

    `docker run --rm` hands the container a fresh random hostname, asv has no
    machine record under that name, and `asv run` exits 1 with "no information
    stored about machine <hostname>". The first validation run rejected a
    healthy 10/10 networkx container on exactly that, and would have rejected
    all 16 -- a gate that fails identically on every input measures nothing.
    Our own run-tests.sh and profile.sh pass --machine=dockertest, and the
    templates seal that record into the image.
    """
    argv = dict(BATTERY_COMMANDS)["asv_discover"]
    assert "--machine=dockertest" in " ".join(argv)


def _run_count_source(conf_dir: Path, env_extra: dict[str, str] | None = None) -> str:
    from datasmith.agents.reflexive.battery import _COUNT_SOURCE

    env = {**os.environ, "CONF_NAME": "asv.conf.json", "REPO_ROOT": str(conf_dir)}
    env.update(env_extra or {})
    proc = subprocess.run([sys.executable, "-c", _COUNT_SOURCE], capture_output=True, text=True, env=env, timeout=60)
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.strip()


_JSONC_CONF = """\
{
    // A line comment, which the first draft did handle.
    "version": 1,
    "project": "demo",
    "branches": ["main"], // an INLINE comment, which it did not.
    "benchmark_dir": "bench",
    "matrix": {
        "numpy": [],
    }
}
"""


def test_the_source_count_survives_a_jsonc_config(tmp_path: Path) -> None:
    """asv.conf.json is JSONC, and 12 of the 16 validation configs prove it.

    Inline `// for git` after a value and a trailing comma before a brace both
    defeat `json.loads`, and the first draft printed `ERR: ...` -- which the
    verifier graded as a hard failure of a perfectly healthy container.
    """
    conf_dir = tmp_path / "conf"
    (conf_dir / "bench").mkdir(parents=True)
    (conf_dir / "asv.conf.json").write_text(_JSONC_CONF, encoding="utf-8")
    (conf_dir / "bench" / "b.py").write_text("def time_a(): pass\ndef track_b(): pass\n", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        json.loads(_JSONC_CONF)  # the config really is not plain JSON

    out = _run_count_source(conf_dir)
    assert "ERR" not in out, out
    assert out.splitlines()[-1] == "2", out


def test_the_source_count_prefers_asvs_own_loader(tmp_path: Path) -> None:
    """asv's loader is the definition of what asv.conf.json means.

    A stub asv on the path that points benchmark_dir somewhere else must move
    the count, or the script is silently ignoring it and guessing.
    """
    conf_dir = tmp_path / "conf"
    (conf_dir / "bench").mkdir(parents=True)
    (conf_dir / "elsewhere").mkdir(parents=True)
    (conf_dir / "asv.conf.json").write_text(_JSONC_CONF, encoding="utf-8")
    (conf_dir / "bench" / "b.py").write_text("def time_a(): pass\ndef track_b(): pass\n", encoding="utf-8")
    (conf_dir / "elsewhere" / "c.py").write_text("def time_only_one(): pass\n", encoding="utf-8")

    stub = tmp_path / "stub"
    (stub / "asv").mkdir(parents=True)
    (stub / "asv" / "__init__.py").write_text("", encoding="utf-8")
    (stub / "asv" / "util.py").write_text(
        "def load_json(path, api_version=None, js_comments=False):\n    return {'benchmark_dir': 'elsewhere'}\n",
        encoding="utf-8",
    )

    out = _run_count_source(conf_dir, {"PYTHONPATH": str(stub)})
    assert out.splitlines()[-1] == "1", out


class TestATimedOutContainerIsKilled:
    """`subprocess.run(timeout=...)` kills the docker client, not the container.

    Without a name to kill, a timed-out container runs on detached and `--rm`
    only fires when it eventually exits by itself. On 2026-08-25 that left 13
    containers alive for up to 274 minutes against a 40-minute timeout, one of
    them running 776 Ray workers, with the host at load 372 on 128 cores doing
    work nothing was waiting for.
    """

    def _fake_docker(self, monkeypatch, *, times_out: bool):
        import subprocess as sp

        from datasmith.agents.reflexive import battery as bat

        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            if times_out and cmd[:2] == ["docker", "run"]:
                raise sp.TimeoutExpired(cmd, kwargs.get("timeout", 1))

            class _P:
                stdout, stderr, returncode = "out", "", 0

            return _P()

        monkeypatch.setattr(bat.subprocess, "run", fake_run)
        return bat, calls

    def test_the_run_is_named_so_it_can_be_killed(self, monkeypatch) -> None:
        bat, calls = self._fake_docker(monkeypatch, times_out=False)
        bat._docker_runner("img:1", ["python", "-c", "pass"], 30)
        run_cmd = calls[0]
        assert "--name" in run_cmd, "an unnamed container cannot be killed on timeout"

    def test_a_timeout_force_removes_the_container(self, monkeypatch) -> None:
        import subprocess as sp

        bat, calls = self._fake_docker(monkeypatch, times_out=True)
        with pytest.raises(sp.TimeoutExpired):
            bat._docker_runner("img:1", ["python", "-c", "pass"], 30)
        names = [c[c.index("--name") + 1] for c in calls if "--name" in c]
        removals = [c for c in calls if c[:3] == ["docker", "rm", "-f"]]
        assert removals, "a timed-out container must be force-removed"
        assert removals[0][3] == names[0], "and it must remove the container it started"

    def test_the_container_is_cpu_capped(self, monkeypatch) -> None:
        """An uncapped container sizes its worker pool from the host's cores."""
        bat, calls = self._fake_docker(monkeypatch, times_out=False)
        monkeypatch.setattr(bat, "DATASMITH_PV_BATTERY_CPUS", 8.0)
        bat._docker_runner("img:1", ["python", "-c", "pass"], 30)
        assert "--cpus" in calls[0]

    def test_the_cap_can_be_switched_off(self, monkeypatch) -> None:
        bat, calls = self._fake_docker(monkeypatch, times_out=False)
        monkeypatch.setattr(bat, "DATASMITH_PV_BATTERY_CPUS", 0.0)
        bat._docker_runner("img:1", ["python", "-c", "pass"], 30)
        assert "--cpus" not in calls[0]

    def test_a_timeout_still_becomes_a_fact_not_a_raise(self, monkeypatch) -> None:
        """run_battery's contract: a crash is a finding, never an exception."""
        bat, _calls = self._fake_docker(monkeypatch, times_out=True)
        facts = bat.run_battery("img:1", timeout_s=1)
        assert facts, "the battery must still report"
        assert all(f.crashed for f in facts)
