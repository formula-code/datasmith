"""Tests for DockerHub publishing module."""

import os
from unittest.mock import Mock, MagicMock, patch

import pytest

from datasmith.core.models.task import Task
from datasmith.docker.dockerhub import (
    _encode_dockerhub_tag_from_local,
    _get_dockerhub_credentials,
    _list_existing_tags,
    _read_docker_config_credentials,
    filter_tasks_not_on_dockerhub,
    publish_images_to_dockerhub,
)


class TestDockerHubTagEncoding:
    """Test tag encoding for DockerHub."""

    def test_simple_tag_encoding(self):
        """Test basic tag encoding without special characters."""
        result = _encode_dockerhub_tag_from_local("myrepo:latest")
        assert result == "myrepo--latest"

    def test_tag_encoding_with_slashes(self):
        """Test tag encoding with slashes (converted to underscores)."""
        result = _encode_dockerhub_tag_from_local("owner/repo:v1.0")
        assert result == "owner__repo--v1.0"

    def test_tag_encoding_with_complex_names(self):
        """Test tag encoding with both slashes and colons."""
        result = _encode_dockerhub_tag_from_local("owner-repo-abc123:final")
        assert result == "owner-repo-abc123--final"

    def test_tag_encoding_default_latest(self):
        """Test that missing tag defaults to 'latest'."""
        result = _encode_dockerhub_tag_from_local("myrepo")
        assert result == "myrepo--latest"

    def test_long_tag_truncation(self):
        """Test that long tags are truncated with hash suffix."""
        # Create a reference that will exceed 128 chars when encoded
        long_name = "a" * 70
        long_tag = "b" * 70
        local_ref = f"{long_name}:{long_tag}"
        result = _encode_dockerhub_tag_from_local(local_ref)

        # Should be exactly 128 chars
        assert len(result) == 128
        # Should end with hash (8 chars after --)
        assert result[-10:-8] == "--"

    def test_tag_encoding_preserves_determinism(self):
        """Test that same input produces same output (deterministic hashing)."""
        ref = "very/long/repository/name:with-a-very-long-tag-that-exceeds-limits"
        result1 = _encode_dockerhub_tag_from_local(ref)
        result2 = _encode_dockerhub_tag_from_local(ref)
        assert result1 == result2


class TestDockerHubAuthentication:
    """Test credential handling for DockerHub."""

    def test_credentials_from_parameters(self):
        """Test that parameters take priority."""
        username, password = _get_dockerhub_credentials("user1", "pass1")
        assert username == "user1"
        assert password == "pass1"

    def test_credentials_from_env_vars(self):
        """Test loading credentials from environment variables."""
        with patch.dict(os.environ, {"DOCKERHUB_USERNAME": "envuser", "DOCKERHUB_TOKEN": "envpass"}):
            username, password = _get_dockerhub_credentials()
            assert username == "envuser"
            assert password == "envpass"

    def test_credentials_parameter_overrides_env(self):
        """Test that parameters override environment variables."""
        with patch.dict(os.environ, {"DOCKERHUB_USERNAME": "envuser", "DOCKERHUB_TOKEN": "envpass"}):
            username, password = _get_dockerhub_credentials("paramuser", "parampass")
            assert username == "paramuser"
            assert password == "parampass"

    def test_credentials_dockerhub_password_fallback(self):
        """Test fallback to DOCKERHUB_PASSWORD env var."""
        with patch.dict(
            os.environ, {"DOCKERHUB_USERNAME": "user", "DOCKERHUB_PASSWORD": "pass"}, clear=True
        ):
            username, password = _get_dockerhub_credentials()
            assert username == "user"
            assert password == "pass"

    @patch("datasmith.docker.dockerhub._read_docker_config_credentials")
    def test_credentials_from_docker_config(self, mock_read_config):
        """Test loading credentials from docker config.json."""
        mock_read_config.return_value = ("configuser", "configpass")
        with patch.dict(os.environ, {}, clear=True):
            username, password = _get_dockerhub_credentials()
            assert username == "configuser"
            assert password == "configpass"

    def test_missing_credentials_raises_error(self):
        """Test that missing credentials raise ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("datasmith.docker.dockerhub._read_docker_config_credentials", return_value=(None, None)):
                with pytest.raises(ValueError, match="DockerHub credentials not found"):
                    _get_dockerhub_credentials()


class TestDockerConfigParsing:
    """Test parsing Docker config.json for credentials."""

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    def test_read_docker_config_with_auth(self, mock_open, mock_exists):
        """Test reading base64-encoded auth from config.json."""
        import base64
        import json

        mock_exists.return_value = True
        auth_string = base64.b64encode(b"testuser:testpass").decode()
        config_data = {"auths": {"docker.io": {"auth": auth_string}}}
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(config_data)

        username, password = _read_docker_config_credentials()
        assert username == "testuser"
        assert password == "testpass"

    @patch("pathlib.Path.exists")
    def test_read_docker_config_not_exists(self, mock_exists):
        """Test handling of missing config.json."""
        mock_exists.return_value = False
        username, password = _read_docker_config_credentials()
        assert username is None
        assert password is None

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    def test_read_docker_config_index_registry(self, mock_open, mock_exists):
        """Test reading from https://index.docker.io/v1/ registry key."""
        import base64
        import json

        mock_exists.return_value = True
        auth_string = base64.b64encode(b"indexuser:indexpass").decode()
        config_data = {"auths": {"https://index.docker.io/v1/": {"auth": auth_string}}}
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(config_data)

        username, password = _read_docker_config_credentials()
        assert username == "indexuser"
        assert password == "indexpass"


class TestDockerHubTagListing:
    """Test tag listing via DockerHub Registry API v2."""

    @patch("requests.get")
    def test_list_existing_tags_success(self, mock_get):
        """Test successful tag listing."""
        # Mock token endpoint
        token_response = Mock(status_code=200)
        token_response.json.return_value = {"token": "test-token-12345"}

        # Mock tags endpoint
        tags_response = Mock(status_code=200)
        tags_response.json.return_value = {"tags": ["v1.0", "v2.0", "latest"]}

        mock_get.side_effect = [token_response, tags_response]

        tags = _list_existing_tags("myuser", "myrepo", "user", "pass")

        assert tags == {"v1.0", "v2.0", "latest"}
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_list_existing_tags_repo_not_found(self, mock_get):
        """Test handling of repository not found (404)."""
        # Mock token endpoint
        token_response = Mock(status_code=200)
        token_response.json.return_value = {"token": "test-token"}

        # Mock tags endpoint returning 404
        tags_response = Mock(status_code=404)

        mock_get.side_effect = [token_response, tags_response]

        tags = _list_existing_tags("myuser", "nonexistent", "user", "pass")

        assert tags == set()

    @patch("requests.get")
    def test_list_existing_tags_empty_repo(self, mock_get):
        """Test handling of repository with no tags."""
        # Mock token endpoint
        token_response = Mock(status_code=200)
        token_response.json.return_value = {"token": "test-token"}

        # Mock tags endpoint with empty tags
        tags_response = Mock(status_code=200)
        tags_response.json.return_value = {"tags": None}

        mock_get.side_effect = [token_response, tags_response]

        tags = _list_existing_tags("myuser", "emptyrepo", "user", "pass")

        assert tags == set()

    @patch("requests.get")
    def test_list_existing_tags_auth_failure(self, mock_get):
        """Test handling of authentication failure."""
        # Mock token endpoint returning 401
        token_response = Mock(status_code=401)

        mock_get.return_value = token_response

        tags = _list_existing_tags("myuser", "myrepo", "baduser", "badpass")

        assert tags == set()

    @patch("requests.get")
    def test_list_existing_tags_network_error(self, mock_get):
        """Test handling of network errors."""
        mock_get.side_effect = Exception("Network error")

        tags = _list_existing_tags("myuser", "myrepo", "user", "pass")

        assert tags == set()

    @patch("requests.get")
    def test_list_existing_tags_timeout(self, mock_get):
        """Test handling of timeout errors."""
        import requests

        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        tags = _list_existing_tags("myuser", "myrepo", "user", "pass")

        assert tags == set()


class TestFilterTasksNotOnDockerHub:
    """Test filtering tasks based on DockerHub existence."""

    @patch("datasmith.docker.dockerhub._list_dockerhub_tags_single_repo")
    def test_filter_all_tasks_exist(self, mock_list_tags):
        """Test filtering when all tasks already exist on DockerHub."""
        tasks = [
            Task("owner1", "repo1", "abc123", tag="base"),
            Task("owner2", "repo2", "def456", tag="base"),
        ]

        # Mock that all encoded tags exist
        mock_list_tags.return_value = {
            "owner1-repo1-abc123--final",
            "owner2-repo2-def456--final",
        }

        filtered = filter_tasks_not_on_dockerhub(
            tasks, namespace="testns", username="user", password="pass"
        )

        assert len(filtered) == 0

    @patch("datasmith.docker.dockerhub._list_dockerhub_tags_single_repo")
    def test_filter_no_tasks_exist(self, mock_list_tags):
        """Test filtering when no tasks exist on DockerHub."""
        tasks = [
            Task("owner1", "repo1", "abc123", tag="base"),
            Task("owner2", "repo2", "def456", tag="base"),
        ]

        # Mock empty tag list
        mock_list_tags.return_value = set()

        filtered = filter_tasks_not_on_dockerhub(
            tasks, namespace="testns", username="user", password="pass"
        )

        assert len(filtered) == 2

    @patch("datasmith.docker.dockerhub._list_dockerhub_tags_single_repo")
    def test_filter_some_tasks_exist(self, mock_list_tags):
        """Test filtering when some tasks exist on DockerHub."""
        tasks = [
            Task("owner1", "repo1", "abc123", tag="base"),
            Task("owner2", "repo2", "def456", tag="base"),
            Task("owner3", "repo3", "ghi789", tag="base"),
        ]

        # Mock that only first task exists
        mock_list_tags.return_value = {"owner1-repo1-abc123--final"}

        filtered = filter_tasks_not_on_dockerhub(
            tasks, namespace="testns", username="user", password="pass"
        )

        assert len(filtered) == 2
        assert filtered[0].owner == "owner2"
        assert filtered[1].owner == "owner3"

    def test_filter_mirror_mode_not_supported(self):
        """Test that mirror mode returns all tasks with warning."""
        tasks = [Task("owner1", "repo1", "abc123", tag="base")]

        filtered = filter_tasks_not_on_dockerhub(
            tasks,
            namespace="testns",
            username="user",
            password="pass",
            repository_mode="mirror",
        )

        # Should return all tasks unchanged for unsupported mode
        assert len(filtered) == 1


class TestPublishImagesToDockerHub:
    """Test image publishing to DockerHub."""

    @patch("docker.from_env")
    def test_publish_empty_list(self, mock_docker):
        """Test publishing empty list of images."""
        result = publish_images_to_dockerhub(
            local_refs=[],
            namespace="testns",
            username="user",
            password="pass",
        )

        assert result == {}

    @patch("docker.from_env")
    def test_publish_single_image_success(self, mock_docker):
        """Test successful single image push."""
        # Mock Docker client
        mock_client = MagicMock()
        mock_docker.return_value = mock_client

        # Mock image and API
        mock_img = MagicMock()
        mock_client.images.get.return_value = mock_img
        mock_api = MagicMock()
        mock_api.push.return_value = [
            {"status": "Preparing"},
            {"status": "Pushing"},
            {"aux": {"Digest": "sha256:abc123"}},
        ]
        mock_client.api = mock_api

        result = publish_images_to_dockerhub(
            local_refs=["myimage:latest"],
            namespace="testuser",
            username="testuser",
            password="testpass",
            docker_client=mock_client,
        )

        assert "myimage:latest" in result
        assert "docker.io/testuser/all:myimage--latest" in result["myimage:latest"]

    @patch("docker.from_env")
    @patch("datasmith.docker.dockerhub._list_existing_tags")
    def test_publish_skip_existing(self, mock_list_tags, mock_docker):
        """Test skipping images that already exist."""
        # Mock Docker client
        mock_client = MagicMock()
        mock_docker.return_value = mock_client

        # Mock that image already exists
        mock_list_tags.return_value = {"myimage--latest"}

        result = publish_images_to_dockerhub(
            local_refs=["myimage:latest"],
            namespace="testuser",
            username="testuser",
            password="testpass",
            skip_existing=True,
            docker_client=mock_client,
        )

        # Should skip and return the result
        assert "myimage:latest" in result
        # Should not have called push
        mock_client.api.push.assert_not_called()

    @patch("docker.from_env")
    def test_publish_rate_limit_handling(self, mock_docker):
        """Test rate limit detection and retry."""
        # Mock Docker client
        mock_client = MagicMock()
        mock_docker.return_value = mock_client

        # Mock image
        mock_img = MagicMock()
        mock_client.images.get.return_value = mock_img

        # Mock API returning rate limit error then success
        mock_api = MagicMock()
        push_attempts = [
            [{"error": "rate limit exceeded"}],  # First attempt fails
            [{"aux": {"Digest": "sha256:abc123"}}],  # Second attempt succeeds
        ]
        mock_api.push.side_effect = push_attempts
        mock_client.api = mock_api

        with patch.dict(os.environ, {"DOCKERHUB_RATE_LIMIT_WAIT": "1"}):  # Short wait for test
            result = publish_images_to_dockerhub(
                local_refs=["myimage:latest"],
                namespace="testuser",
                username="testuser",
                password="testpass",
                docker_client=mock_client,
            )

        assert "myimage:latest" in result


class TestRegistryConfigIntegration:
    """Test integration with registry_config module."""

    def test_registry_config_dockerhub_from_env(self):
        """Test creating DockerHub config from environment."""
        from datasmith.docker.registry_config import RegistryConfig, RegistryType

        with patch.dict(
            os.environ,
            {
                "DOCKERHUB_NAMESPACE": "testns",
                "DOCKERHUB_USERNAME": "testuser",
                "DOCKERHUB_TOKEN": "testtoken",
            },
        ):
            config = RegistryConfig.from_env(RegistryType.DOCKERHUB)

            assert config.registry_type == RegistryType.DOCKERHUB
            assert config.namespace == "testns"
            assert config.username == "testuser"
            assert config.password == "testtoken"

    def test_registry_config_validation(self):
        """Test registry config validation."""
        from datasmith.docker.registry_config import RegistryConfig, RegistryType

        config = RegistryConfig(
            registry_type=RegistryType.DOCKERHUB,
            namespace="testns",
            repository_mode="single",
        )

        # Should not raise
        config.validate()

    def test_registry_config_invalid_mode(self):
        """Test validation fails for invalid repository mode."""
        from datasmith.docker.registry_config import RegistryConfig, RegistryType

        config = RegistryConfig(
            registry_type=RegistryType.DOCKERHUB,
            namespace="testns",
            repository_mode="invalid",
        )

        with pytest.raises(ValueError, match="repository_mode must be"):
            config.validate()
