"""Tests for datasmith.docker.publish — DockerHubPublisher."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datasmith.docker.publish import DockerHubPublisher


@pytest.fixture()
def publisher() -> DockerHubPublisher:
    with patch("datasmith.docker.publish.DockerClient") as mock_cls:
        pub = DockerHubPublisher(namespace="formulacode")
        pub._mock_docker = mock_cls.return_value  # type: ignore[attr-defined]
        yield pub


class TestPush:
    def test_push_new_tag(self, publisher: DockerHubPublisher) -> None:
        with patch.dict("os.environ", {"DOCKERHUB_USERNAME": "user", "DOCKERHUB_PASSWORD": "pass"}):
            publisher.push("formulacode/pandas:latest")

        publisher._mock_docker.login.assert_called_once_with(  # type: ignore[attr-defined]
            username="user",
            password="pass",  # noqa: S106
        )
        publisher._mock_docker.push.assert_called_once_with(  # type: ignore[attr-defined]
            "formulacode/pandas:latest"
        )

    def test_push_skips_login_when_already_logged_in(self, publisher: DockerHubPublisher) -> None:
        publisher._logged_in = True
        publisher.push("formulacode/pandas:latest")

        publisher._mock_docker.login.assert_not_called()  # type: ignore[attr-defined]
        publisher._mock_docker.push.assert_called_once()  # type: ignore[attr-defined]

    def test_push_skips_login_without_credentials(self, publisher: DockerHubPublisher) -> None:
        with patch.dict("os.environ", {"DOCKERHUB_USERNAME": "", "DOCKERHUB_PASSWORD": ""}, clear=False):
            publisher.push("formulacode/pandas:latest")

        publisher._mock_docker.login.assert_not_called()  # type: ignore[attr-defined]
        assert publisher._logged_in is False


class TestTagWithVersion:
    def test_tag_with_version_format(self, publisher: DockerHubPublisher) -> None:
        new_tag = publisher.tag_with_version("formulacode/pandas:latest")

        # Should append @YYYY-MM
        assert new_tag.startswith("formulacode/pandas:latest@")
        assert len(new_tag.split("@")) == 2
        version_part = new_tag.split("@")[1]
        # Should be YYYY-MM format
        assert len(version_part) == 7
        assert version_part[4] == "-"

        publisher._mock_docker.tag.assert_called_once_with(  # type: ignore[attr-defined]
            "formulacode/pandas:latest", new_tag
        )


class TestListRemoteTags:
    def test_list_remote_tags_success(self, publisher: DockerHubPublisher) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {"name": "latest"},
                {"name": "v1.0"},
                {"name": "v2.0"},
            ]
        }
        with patch("datasmith.docker.publish.httpx.get", return_value=mock_response) as mock_get:
            tags = publisher.list_remote_tags("pandas")

        assert tags == ["latest", "v1.0", "v2.0"]
        mock_get.assert_called_once_with(
            "https://hub.docker.com/v2/repositories/formulacode/pandas/tags/",
            timeout=10.0,
        )

    def test_list_remote_tags_failure_returns_empty(self, publisher: DockerHubPublisher) -> None:
        with patch("datasmith.docker.publish.httpx.get", side_effect=Exception("network error")):
            tags = publisher.list_remote_tags("pandas")

        assert tags == []

    def test_list_remote_tags_non_200(self, publisher: DockerHubPublisher) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 404
        with patch("datasmith.docker.publish.httpx.get", return_value=mock_response):
            tags = publisher.list_remote_tags("nonexistent")

        assert tags == []


class TestFilterUnpublished:
    def test_filter_unpublished_with_5_local_3_remote(self, publisher: DockerHubPublisher) -> None:
        local_tags = [
            "formulacode/pandas:latest",
            "formulacode/pandas:v1.0",
            "formulacode/pandas:v2.0",
            "formulacode/pandas:v3.0",
            "formulacode/pandas:v4.0",
        ]
        remote_tags = ["latest", "v1.0", "v2.0"]

        unpublished = publisher.filter_unpublished(local_tags, remote_tags)

        assert unpublished == [
            "formulacode/pandas:v3.0",
            "formulacode/pandas:v4.0",
        ]

    def test_filter_unpublished_all_published(self, publisher: DockerHubPublisher) -> None:
        local_tags = ["formulacode/pandas:latest", "formulacode/pandas:v1.0"]
        remote_tags = ["latest", "v1.0"]

        unpublished = publisher.filter_unpublished(local_tags, remote_tags)
        assert unpublished == []

    def test_filter_unpublished_none_published(self, publisher: DockerHubPublisher) -> None:
        local_tags = ["formulacode/pandas:v1.0", "formulacode/pandas:v2.0"]
        remote_tags: list[str] = []

        unpublished = publisher.filter_unpublished(local_tags, remote_tags)
        assert unpublished == local_tags

    def test_filter_unpublished_empty_local(self, publisher: DockerHubPublisher) -> None:
        local_tags: list[str] = []
        remote_tags = ["latest", "v1.0"]

        unpublished = publisher.filter_unpublished(local_tags, remote_tags)
        assert unpublished == []
