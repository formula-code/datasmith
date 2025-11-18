"""Tests for the DockerValidator integration layer."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

import pytest

from datasmith.core.models import Task
from datasmith.docker.validation import (
    AcceptanceResult,
    DockerValidator,
    ProfileValidationResult,
    TestValidationResult,
    ValidationConfig,
)


@pytest.fixture()
def make_task() -> Callable[[str], Task]:
    def _factory(tag: str = "pkg") -> Task:
        return Task(owner="owner", repo="repo", sha="deadbeef", tag=tag)

    return _factory


def _validator(tmp_path: Path, client: MagicMock, context_registry: MagicMock) -> DockerValidator:
    config = ValidationConfig(output_dir=tmp_path, build_timeout=10, run_timeout=10, tail_chars=64)
    machine_defaults = {"arch": "x86_64"}
    return DockerValidator(
        client=client, context_registry=context_registry, machine_defaults=machine_defaults, config=config
    )


def test_validate_task_profile_and_tests_success(
    tmp_path: Path, make_task: Callable[[str], Task], monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate_task should return accepted=True when both profile and tests pass."""
    client = MagicMock()
    context_registry = MagicMock()
    validator = _validator(tmp_path, client, context_registry)

    profile_res = ProfileValidationResult(
        ok=True,
        stdout="profile ok",
        stderr="",
        duration_s=1.0,
        benchmarks="",
    )
    tests_res = TestValidationResult(
        ok=True,
        stdout="tests ok",
        stderr="",
        duration_s=2.0,
        suite_name="suite",
    )

    monkeypatch.setattr(validator, "validate_profile", MagicMock(return_value=profile_res))
    monkeypatch.setattr(validator, "validate_tests", MagicMock(return_value=tests_res))

    result = validator.validate_task(make_task(), run_labels={})

    assert isinstance(result, AcceptanceResult)
    assert result.accepted is True
    assert result.reason == "All validations passed"
    validator.validate_profile.assert_called_once()
    validator.validate_tests.assert_called_once()


def test_validate_task_profile_failure_skips_tests(
    tmp_path: Path, make_task: Callable[[str], Task], monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate_task should not run tests when profile fails."""
    client = MagicMock()
    context_registry = MagicMock()
    validator = _validator(tmp_path, client, context_registry)

    profile_res = ProfileValidationResult(
        ok=False,
        stdout="",
        stderr="profile failed",
        duration_s=1.0,
        benchmarks="",
    )

    tests_mock = MagicMock()

    monkeypatch.setattr(validator, "validate_profile", MagicMock(return_value=profile_res))
    monkeypatch.setattr(validator, "validate_tests", tests_mock)

    result = validator.validate_task(make_task(), run_labels={})

    assert isinstance(result, AcceptanceResult)
    assert result.accepted is False
    assert result.tests is None
    assert result.reason == "Profile validation failed"
    validator.validate_profile.assert_called_once()
    tests_mock.assert_not_called()


def test_validate_task_tests_failure_sets_reason(
    tmp_path: Path, make_task: Callable[[str], Task], monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate_task should mark acceptance False when tests fail."""
    client = MagicMock()
    context_registry = MagicMock()
    validator = _validator(tmp_path, client, context_registry)

    profile_res = ProfileValidationResult(
        ok=True,
        stdout="profile ok",
        stderr="",
        duration_s=1.0,
        benchmarks="",
    )
    tests_res = TestValidationResult(
        ok=False,
        stdout="",
        stderr="tests failed",
        duration_s=2.0,
        suite_name="suite",
    )

    monkeypatch.setattr(validator, "validate_profile", MagicMock(return_value=profile_res))
    monkeypatch.setattr(validator, "validate_tests", MagicMock(return_value=tests_res))

    result = validator.validate_task(make_task(), run_labels={})

    assert isinstance(result, AcceptanceResult)
    assert result.accepted is False
    assert result.tests is tests_res
    assert result.reason == "Test validation failed"
