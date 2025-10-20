"""Tests for the DockerValidator integration layer."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock

import pytest
from docker.errors import ImageNotFound

from datasmith.core.models import BuildResult, Task
from datasmith.docker.validation import DockerValidator, ValidationConfig


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


def test_skip_when_image_already_exists(tmp_path: Path, make_task: Callable[[str], Task]) -> None:
    client = MagicMock()
    client.images.get.return_value = object()  # image present
    context_registry = MagicMock()

    validator = _validator(tmp_path, client, context_registry)
    result = validator.validate_task(make_task(), run_labels={})

    assert result["stage"] == "build-skipped"
    client.images.get.assert_called_once()
    context_registry.get.assert_not_called()


def test_build_failure_records_error(tmp_path: Path, make_task: Callable[[str], Task]) -> None:
    client = MagicMock()
    client.images.get.side_effect = ImageNotFound("missing", response=None)
    context_registry = MagicMock()

    build_result = BuildResult(
        ok=False,
        image_name="owner-repo:pkg",
        image_id=None,
        rc=1,
        duration_s=2.0,
        stderr_tail="boom",
        stdout_tail="",
    )

    docker_ctx_mock = MagicMock()
    docker_ctx_mock.build_container_streaming.return_value = build_result

    # Mock context_registry.get() and get_default()
    context_registry.get.return_value = docker_ctx_mock
    context_registry.get_default.return_value = MagicMock()  # Different from docker_ctx_mock

    validator = _validator(tmp_path, client, context_registry)
    result = validator.validate_task(make_task(), run_labels={})

    assert result["ok"] is False
    assert (tmp_path / "errors.txt").read_text().strip().startswith("$ docker build")


def test_successful_validation_runs_profile(
    tmp_path: Path, make_task: Callable[[str], Task], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = MagicMock()
    client.images.get.side_effect = ImageNotFound("missing", response=None)
    context_registry = MagicMock()

    build_result = BuildResult(
        ok=True,
        image_name="owner-repo:pkg",
        image_id="sha256:123",
        rc=0,
        duration_s=2.0,
        stderr_tail="",
        stdout_tail="logs",
    )

    docker_ctx_mock = MagicMock()
    docker_ctx_mock.build_container_streaming.return_value = build_result

    # Mock context_registry.get() and get_default()
    context_registry.get.return_value = docker_ctx_mock
    context_registry.get_default.return_value = MagicMock()  # Different from docker_ctx_mock

    monkeypatch.setattr(
        "datasmith.docker.validation._run_quick_profile",
        MagicMock(return_value=(True, "preview")),
    )

    validator = _validator(tmp_path, client, context_registry)
    result = validator.validate_task(make_task(), run_labels={})

    assert result["ok"] is True
    assert result["stderr_tail"] == "preview"
    context_registry.get.assert_called_once()
