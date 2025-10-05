"""Smoke tests for docker builder abstractions."""

from __future__ import annotations

from unittest import mock

import pytest

from datasmith.core.models import Task
from datasmith.docker.build import BuilderFactory, BuildxBuilder, SDKBuilder


def test_builder_factory_creates_expected_types():
    fake_client = mock.MagicMock()

    builder = BuilderFactory.create_builder("sdk", client=fake_client)
    assert isinstance(builder, SDKBuilder)
    assert builder.client is fake_client

    builder = BuilderFactory.create_builder("buildx", client=fake_client)
    assert isinstance(builder, BuildxBuilder)

    with pytest.raises(ValueError):
        BuilderFactory.create_builder("unknown", client=fake_client)


def test_task_image_name_round_trip() -> None:
    task = Task(owner="Acme", repo="Widget", sha="abc123", tag="pkg")
    image_name = task.get_image_name()
    assert image_name == "acme-widget-abc123:pkg"


@pytest.mark.parametrize("tag", ["env", "pkg", "run", "base"])
def test_task_with_tag(tag: str) -> None:
    base = Task(owner="acme", repo="widget")
    updated = base.with_tag(tag)
    assert updated.tag == tag
    assert updated.owner == base.owner
    assert updated.repo == base.repo
