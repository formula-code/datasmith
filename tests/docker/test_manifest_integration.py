"""Integration tests for the build manifest chain.

Marked ``slow`` — they build and run real containers.
Run with: uv run pytest tests/docker/test_manifest_integration.py -v -m slow
"""

import json
import subprocess
import textwrap

import pytest

pytestmark = pytest.mark.slow


def _docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


requires_docker = pytest.mark.skipif(not _docker_available(), reason="docker unavailable")


@requires_docker
def test_timeout_now_fails_verification(tmp_path, monkeypatch):
    """The regression that would have caught all 619 rows.

    Builds an image whose /run-tests.sh sleeps past the limit and asserts
    run_tests reports failure. No repo build needed — seconds, not minutes.
    """
    import importlib.util
    import sys

    (tmp_path / "Dockerfile").write_text(
        textwrap.dedent("""\
            FROM alpine:latest
            RUN printf '#!/bin/sh\\nsleep 300\\n' > /run-tests.sh && chmod +x /run-tests.sh
            ENTRYPOINT ["/bin/sh"]
        """)
    )
    tag = "fc-test-timeout:local"
    subprocess.run(
        ["docker", "build", "--network=host", "-t", tag, str(tmp_path)],
        check=True,
        capture_output=True,
        timeout=300,
    )

    spec = importlib.util.spec_from_file_location("local_ci", "src/datasmith/agents/templates/local_ci.py")
    local_ci = importlib.util.module_from_spec(spec)
    # local_ci.py uses `from __future__ import annotations` and declares a
    # @dataclass; resolving its (now-string) field annotations requires the
    # module to be discoverable via sys.modules[cls.__module__], which
    # module_from_spec() alone does not provide. monkeypatch reverts this
    # at teardown so the module table isn't polluted for later tests.
    monkeypatch.setitem(sys.modules, "local_ci", local_ci)
    spec.loader.exec_module(local_ci)

    metrics: dict = {}
    ok, _stdout, stderr, _rc = local_ci.run_tests(tag, timeout=5, metrics=metrics)

    assert ok is False, "a timed-out container must NOT verify"
    assert "exceeded" in stderr
    assert metrics["test_timed_out"] is True
    assert metrics["timeout_s"] == 5

    subprocess.run(["docker", "rmi", "-f", tag], capture_output=True)


@requires_docker
def test_sealer_produces_a_readable_manifest(tmp_path):
    """emit_manifest.py seals breadcrumbs into a manifest inside an image."""
    from datasmith.docker.manifest import evaluate_invariants

    with open("src/datasmith/docker/templates/emit_manifest.py") as fh:
        sealer = fh.read()
    (tmp_path / "emit_manifest.py").write_text(sealer)
    (tmp_path / "Dockerfile").write_text(
        textwrap.dedent("""\
            FROM python:3.11-slim
            COPY emit_manifest.py /emit_manifest.py
            RUN mkdir -p /opt/formulacode \\
             && printf '{"k":"discovered_n","v":"3"}\\n{"k":"secrets_scan_clean","v":"1"}\\n' \\
                > /opt/formulacode/notes.jsonl \\
             && python3 /emit_manifest.py
        """)
    )
    tag = "fc-test-sealer:local"
    subprocess.run(
        ["docker", "build", "--network=host", "-t", tag, str(tmp_path)],
        check=True,
        capture_output=True,
        timeout=600,
    )

    out = subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "cat", tag, "/opt/formulacode/build_manifest.json"],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    manifest = json.loads(out.stdout)

    assert manifest["schema_version"] == 1
    assert manifest["build"]["discovered_n"] == 3
    assert manifest["build"]["secrets_scan_clean"] is True
    assert manifest["verify"] == {}

    # A never-run image has build facts only; runtime invariants skip.
    report = evaluate_invariants(manifest)
    assert "test_timed_out" in report.skipped
    assert "discovered_n_zero" not in report.fatal

    subprocess.run(["docker", "rmi", "-f", tag], capture_output=True)
