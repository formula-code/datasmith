"""A trial run must be able to build without publishing.

Stage 6 pushes every successful build to DockerHub. That is right for the
pipeline and wrong for a trial that rebuilds two dozen repositories to measure
a build rate -- especially when the templates that produced them are known to
be defective.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

MODULE = "datasmith.runners.synthesize_images"


def _reload(value: str | None):
    env = {} if value is None else {"DATASMITH_SKIP_IMAGE_PUSH": value}
    with patch.dict("os.environ", env, clear=False):
        if value is None:
            import os

            os.environ.pop("DATASMITH_SKIP_IMAGE_PUSH", None)
        return importlib.reload(importlib.import_module(MODULE))


@pytest.fixture(autouse=True)
def _restore():
    yield
    _reload(None)


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes"])
def test_push_is_skipped_when_set(value: str) -> None:
    mod = _reload(value)
    with patch("datasmith.docker.publish.DockerHubPublisher") as publisher:
        mod._push_pr_image("o", "r", "tag:1")
    publisher.assert_not_called()


@pytest.mark.parametrize("value", [None, "0", "false", ""])
def test_push_still_happens_by_default(value: str | None) -> None:
    """The default must not change. The pipeline relies on this push."""
    mod = _reload(value)
    instance = MagicMock()
    with patch("datasmith.docker.publish.DockerHubPublisher", return_value=instance):
        mod._push_pr_image("o", "r", "tag:1")
    assert instance.push.called, "the default path must still publish"
    assert any("tag:1" in str(c) for c in instance.push.call_args_list)
