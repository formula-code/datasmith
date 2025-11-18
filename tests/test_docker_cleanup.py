"""Tests for Docker cleanup utilities.

This module tests Docker resource cleanup functions including container removal,
image pruning, and build cache cleanup.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from docker.errors import APIError, ImageNotFound, NotFound

from datasmith.docker.cleanup import (
    fast_cleanup_run_artifacts,
    remove_containers_by_label,
    soft_prune,
)


class TestRemoveContainersByLabel:
    """Tests for container removal by label."""

    def test_remove_containers_by_label_removes_all(self) -> None:
        """Test that all labeled containers are removed."""
        client = MagicMock()
        container1 = MagicMock()
        container1.name = "container1"
        container1.id = "abc123"
        container2 = MagicMock()
        container2.name = "container2"
        container2.id = "def456"

        client.containers.list.return_value = [container1, container2]

        remove_containers_by_label(client, "test-run-id")

        # Verify containers.list was called with correct filter
        client.containers.list.assert_called_once_with(all=True, filters={"label": "datasmith.run=test-run-id"})

        # Verify both containers were removed
        container1.remove.assert_called_once_with(force=True)
        container2.remove.assert_called_once_with(force=True)

        # Verify prune was called
        client.containers.prune.assert_called_once()

    def test_remove_containers_by_label_handles_notfound(self) -> None:
        """Test that NotFound errors are handled gracefully."""
        client = MagicMock()
        container = MagicMock()
        container.name = "container"
        container.id = "abc123"
        container.remove.side_effect = NotFound("not found", response=None)

        client.containers.list.return_value = [container]

        # Should not raise exception
        remove_containers_by_label(client, "test-run-id")

        container.remove.assert_called_once_with(force=True)

    def test_remove_containers_by_label_empty_list(self) -> None:
        """Test behavior when no containers match the label."""
        client = MagicMock()
        client.containers.list.return_value = []

        remove_containers_by_label(client, "test-run-id")

        client.containers.list.assert_called_once()
        client.containers.prune.assert_called_once()


class TestSoftPrune:
    """Tests for soft pruning of Docker resources."""

    def test_soft_prune_containers_older_than_1h(self) -> None:
        """Test that containers older than 1h are pruned."""
        client = MagicMock()

        soft_prune(client, None)

        client.containers.prune.assert_called_once_with(filters={"until": "1h"})

    def test_soft_prune_images_without_run_label(self) -> None:
        """Test image pruning without run_id filter."""
        client = MagicMock()
        client.images.prune.return_value = {"SpaceReclaimed": 1024000}

        soft_prune(client, None)

        client.images.prune.assert_called_once_with(filters={"until": "1h"})

    def test_soft_prune_images_with_run_label(self) -> None:
        """Test image pruning with run_id filter."""
        client = MagicMock()
        client.images.prune.return_value = {"SpaceReclaimed": 2048000}

        soft_prune(client, "test-run-id")

        client.images.prune.assert_called_once_with(filters={"until": "1h", "label": ["datasmith.run=test-run-id"]})

    def test_soft_prune_buildkit_cache(self) -> None:
        """Test BuildKit cache pruning when available."""
        client = MagicMock()
        client.api.prune_builds = MagicMock()

        soft_prune(client, None)

        # BuildKit prune should be attempted
        client.api.prune_builds.assert_called_once()

    def test_soft_prune_handles_exceptions(self) -> None:
        """Test that exceptions during pruning are caught."""
        client = MagicMock()
        client.containers.prune.side_effect = Exception("prune error")
        client.images.prune.side_effect = Exception("image error")

        # Should not raise exception
        soft_prune(client, None)


class TestFastCleanupRunArtifacts:
    """Tests for aggressive cleanup of run artifacts."""

    def test_fast_cleanup_resolves_image_ids(self) -> None:
        """Test that image references are resolved to IDs."""
        client = MagicMock()

        # Mock image with label
        image1 = MagicMock()
        image1.id = "sha256:abc123"
        image1.labels = {"datasmith.run": "test-run-id"}
        image1.attrs = {"Config": {"Labels": {"datasmith.run": "test-run-id"}}}

        client.images.get.return_value = image1

        fast_cleanup_run_artifacts(client, "test-run-id", extra_image_refs=["owner-repo:pkg"])

        client.images.get.assert_called_with("owner-repo:pkg")

    def test_fast_cleanup_removes_by_id(self) -> None:
        """Test that images are removed by ID."""
        client = MagicMock()

        image = MagicMock()
        image.id = "sha256:abc123"
        image.labels = {"datasmith.run": "test-run-id"}
        image.attrs = {"Config": {"Labels": {"datasmith.run": "test-run-id"}}}

        client.images.get.return_value = image

        fast_cleanup_run_artifacts(client, "test-run-id", extra_image_refs=["test:image"])

        # Should remove by ID
        client.images.remove.assert_called()
        remove_call = client.images.remove.call_args
        assert remove_call[0][0] == "sha256:abc123"
        assert remove_call[1]["force"] is True

    def test_fast_cleanup_prunes_unused_images(self) -> None:
        """Test that server-side prune is called for unused images."""
        client = MagicMock()
        client.images.get.side_effect = ImageNotFound("not found", response=None)

        fast_cleanup_run_artifacts(client, "test-run-id")

        # Should call prune with label filter
        client.images.prune.assert_called()
        prune_call = client.images.prune.call_args
        assert "label" in prune_call[1]["filters"]
        assert "datasmith.run=test-run-id" in prune_call[1]["filters"]["label"]

    def test_fast_cleanup_handles_409_conflict(self) -> None:
        """Test that 409 conflict errors (image in use) are handled."""
        client = MagicMock()

        image = MagicMock()
        image.id = "sha256:abc123"
        image.labels = {"datasmith.run": "test-run-id"}
        image.attrs = {"Config": {"Labels": {"datasmith.run": "test-run-id"}}}

        client.images.get.return_value = image

        # Mock 409 error - APIError requires response object
        response = MagicMock()
        response.status_code = 409
        api_error = APIError("conflict", response=response)
        client.images.remove.side_effect = api_error

        # Should not raise exception
        fast_cleanup_run_artifacts(client, "test-run-id", extra_image_refs=["test:image"])

    def test_fast_cleanup_handles_image_not_found(self) -> None:
        """Test that ImageNotFound errors are handled."""
        client = MagicMock()
        client.images.get.side_effect = ImageNotFound("not found", response=None)

        # Should not raise exception
        fast_cleanup_run_artifacts(client, "test-run-id", extra_image_refs=["missing:image"])

    def test_fast_cleanup_prunes_networks_and_volumes(self) -> None:
        """Test that networks and volumes are pruned."""
        client = MagicMock()
        client.images.get.side_effect = ImageNotFound("not found", response=None)

        fast_cleanup_run_artifacts(client, "test-run-id")

        # Should prune networks and volumes with label filter
        client.networks.prune.assert_called_once()
        client.volumes.prune.assert_called_once()

        # Check filters
        network_call = client.networks.prune.call_args
        assert "datasmith.run=test-run-id" in network_call[1]["filters"]["label"]

        volume_call = client.volumes.prune.call_args
        assert "datasmith.run=test-run-id" in volume_call[1]["filters"]["label"]

    def test_fast_cleanup_build_cache_prune(self) -> None:
        """Test build cache pruning."""
        client = MagicMock()
        client.api = MagicMock()
        client.api.prune_builds = MagicMock()
        client.images.get.side_effect = ImageNotFound("not found", response=None)

        fast_cleanup_run_artifacts(client, "test-run-id")

        # Should attempt to prune builds
        client.api.prune_builds.assert_called_once()

    def test_fast_cleanup_with_no_extra_refs(self) -> None:
        """Test cleanup with no extra image references."""
        client = MagicMock()

        fast_cleanup_run_artifacts(client, "test-run-id")

        # Should still call prune
        client.images.prune.assert_called_once()
        client.networks.prune.assert_called_once()
        client.volumes.prune.assert_called_once()

    def test_fast_cleanup_wrong_run_id_label(self) -> None:
        """Test that images with wrong run_id label are not removed."""
        client = MagicMock()

        # Image with different run_id
        image = MagicMock()
        image.id = "sha256:abc123"
        image.labels = {"datasmith.run": "different-run-id"}
        image.attrs = {"Config": {"Labels": {"datasmith.run": "different-run-id"}}}

        client.images.get.return_value = image

        fast_cleanup_run_artifacts(client, "test-run-id", extra_image_refs=["test:image"])

        # Should NOT remove the image
        client.images.remove.assert_not_called()
