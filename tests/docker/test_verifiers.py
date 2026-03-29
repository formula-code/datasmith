"""Tests for datasmith.docker.verifiers — Verifier classes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.docker.verifiers import (
    MultiObjVerifier,
    ProfileVerifier,
    PytestVerifier,
    SmokeVerifier,
    VerifyResult,
)


@pytest.fixture()
def mock_docker() -> MagicMock:
    with patch("datasmith.docker.verifiers.DockerClient") as mock_cls:
        yield mock_cls.return_value


class TestSmokeVerifier:
    def test_smoke_passes_exit_0(self, mock_docker: MagicMock) -> None:
        mock_docker.run.return_value = "import ok"
        verifier = SmokeVerifier(package="numpy")
        result = verifier.verify("test-image:latest")

        assert result.ok is True
        assert result.rc == 0
        assert result.stage == "smoke"
        assert result.duration_s >= 0.0
        mock_docker.run.assert_called_once_with(
            "test-image:latest",
            ["python", "-c", "import numpy"],
            remove=True,
            pull="never",
        )

    def test_smoke_fails_exit_1(self, mock_docker: MagicMock) -> None:
        mock_docker.run.side_effect = Exception("ModuleNotFoundError: No module named 'numpy'")
        verifier = SmokeVerifier(package="numpy")
        result = verifier.verify("test-image:latest")

        assert result.ok is False
        assert result.rc == 1
        assert result.stage == "smoke"
        assert "ModuleNotFoundError" in result.stderr


class TestProfileVerifier:
    def test_profile_passes(self, mock_docker: MagicMock) -> None:
        mock_docker.run.return_value = "profile output"
        verifier = ProfileVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is True
        assert result.rc == 0
        assert result.stage == "profile"

    def test_profile_passes_log_path_arg(self, mock_docker: MagicMock) -> None:
        """ProfileVerifier must pass the required LOG_PATH arg to profile.sh."""
        mock_docker.run.return_value = "profile output"
        verifier = ProfileVerifier(timeout=60)
        verifier.verify("test-image:latest")

        mock_docker.run.assert_called_once_with(
            "test-image:latest",
            ["/bin/bash", "/profile.sh", "/tmp/profile_log"],
            remove=True,
            pull="never",
        )

    def test_profile_timeout_treated_as_success(self, mock_docker: MagicMock) -> None:
        mock_docker.run.side_effect = Exception("Command timed out after timeout seconds")
        verifier = ProfileVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is True
        assert result.rc == 124
        assert result.stage == "profile"

    def test_profile_non_timeout_failure(self, mock_docker: MagicMock) -> None:
        mock_docker.run.side_effect = Exception("segfault")
        verifier = ProfileVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is False
        assert result.rc == 1
        assert result.stage == "profile"


class TestPytestVerifier:
    def test_pytest_passes(self, mock_docker: MagicMock) -> None:
        mock_docker.run.return_value = "all tests passed"
        verifier = PytestVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is True
        assert result.rc == 0
        assert result.stage == "pytest"

    def test_pytest_fails(self, mock_docker: MagicMock) -> None:
        mock_docker.run.side_effect = Exception("FAILED tests/test_foo.py::test_bar")
        verifier = PytestVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is False
        assert result.rc == 1
        assert result.stage == "pytest"

    def test_no_benchmarks_detected(self, mock_docker: MagicMock) -> None:
        """run-tests.sh exit 0 but output contains FORMULACODE_NO_BENCHMARKS sentinel."""
        mock_docker.run.return_value = (
            "FORMULACODE_NO_BENCHMARKS: 0 ASV benchmarks discovered.\n"
            "FORMULACODE_SNAPSHOT_START\n"
            '{"total": 0, "passed": 0, "failed": 0, "skipped": 0}\n'
            "FORMULACODE_SNAPSHOT_END\n"
        )
        verifier = PytestVerifier(timeout=60)
        result = verifier.verify("test-image:latest")

        assert result.ok is False
        assert result.rc == 78
        assert result.stage == "pytest"
        assert "No ASV benchmarks" in result.stderr


class TestMultiObjVerifier:
    def test_multi_obj_short_circuits(self) -> None:
        """When the first verifier fails, subsequent verifiers should not run."""
        v1 = MagicMock(spec=SmokeVerifier)
        v1.verify.return_value = VerifyResult(ok=False, rc=1, stderr="import failed", stage="smoke", duration_s=0.5)
        v2 = MagicMock(spec=ProfileVerifier)
        v2.verify.return_value = VerifyResult(ok=True, rc=0, stdout="profile ok", stage="profile", duration_s=1.0)

        multi = MultiObjVerifier(verifiers=[v1, v2])
        result = multi.verify("test-image:latest")

        assert result.ok is False
        assert result.stage == "smoke"
        v1.verify.assert_called_once()
        v2.verify.assert_not_called()

    def test_multi_obj_all_pass(self) -> None:
        """When all verifiers pass, result should be ok with stage='all'."""
        v1 = MagicMock(spec=SmokeVerifier)
        v1.verify.return_value = VerifyResult(ok=True, rc=0, stdout="import ok", stage="smoke", duration_s=0.3)
        v2 = MagicMock(spec=ProfileVerifier)
        v2.verify.return_value = VerifyResult(ok=True, rc=0, stdout="profile ok", stage="profile", duration_s=0.7)
        v3 = MagicMock(spec=PytestVerifier)
        v3.verify.return_value = VerifyResult(ok=True, rc=0, stdout="tests ok", stage="pytest", duration_s=1.0)

        multi = MultiObjVerifier(verifiers=[v1, v2, v3])
        result = multi.verify("test-image:latest")

        assert result.ok is True
        assert result.rc == 0
        assert result.stage == "all"
        assert result.duration_s == pytest.approx(2.0, abs=0.01)

    def test_multi_obj_preserves_logs(self) -> None:
        """Combined stdout/stderr should contain output from all executed verifiers."""
        v1 = MagicMock(spec=SmokeVerifier)
        v1.verify.return_value = VerifyResult(
            ok=True, rc=0, stdout="smoke output", stderr="smoke warn", stage="smoke", duration_s=0.1
        )
        v2 = MagicMock(spec=ProfileVerifier)
        v2.verify.return_value = VerifyResult(
            ok=False, rc=1, stdout="profile output", stderr="profile error", stage="profile", duration_s=0.2
        )

        multi = MultiObjVerifier(verifiers=[v1, v2])
        result = multi.verify("test-image:latest")

        assert "=== SMOKE ===" in result.stdout
        assert "smoke output" in result.stdout
        assert "=== PROFILE ===" in result.stdout
        assert "profile output" in result.stdout
        assert "=== SMOKE ===" in result.stderr
        assert "smoke warn" in result.stderr
        assert "=== PROFILE ===" in result.stderr
        assert "profile error" in result.stderr

    def test_multi_obj_empty_verifiers(self) -> None:
        """Empty verifier list should return ok."""
        multi = MultiObjVerifier(verifiers=[])
        result = multi.verify("test-image:latest")

        assert result.ok is True
        assert result.stage == "all"
        assert result.duration_s == 0.0


class TestVerifyResult:
    def test_verify_result_defaults(self) -> None:
        r = VerifyResult(ok=True)
        assert r.rc == 0
        assert r.stdout == ""
        assert r.stderr == ""
        assert r.duration_s == 0.0
        assert r.stage == ""

    def test_verify_result_serialization(self) -> None:
        r = VerifyResult(ok=False, rc=1, stderr="error", stage="smoke", duration_s=1.5)
        data = r.model_dump()
        r2 = VerifyResult.model_validate(data)
        assert r2.ok is False
        assert r2.rc == 1
        assert r2.stderr == "error"
