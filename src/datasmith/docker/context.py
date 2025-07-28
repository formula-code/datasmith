from __future__ import annotations

import io
import sys
import tarfile
from copy import deepcopy
from pathlib import Path

import docker
from docker.errors import DockerException, ImageNotFound

from datasmith.logging_config import get_logger

logger = get_logger("docker.context")


class DockerContext:
    default_dockerfile_loc = Path(__file__).parent / "Dockerfile"
    default_entrypoint_loc = Path(__file__).parent / "entrypoint.sh"
    dockerfile_data: str
    entrypoint_data: str

    def __init__(self, dockerfile_data: str | None = None, entrypoint_data: str | None = None):
        if dockerfile_data is None:
            dockerfile_data = self.default_dockerfile_loc.read_text()
        if entrypoint_data is None:
            entrypoint_data = self.default_entrypoint_loc.read_text()

        self.dockerfile_data = dockerfile_data
        self.entrypoint_data = entrypoint_data
        self.tarball_stream = self.build_tarball_stream()

    def build_tarball_stream(self) -> io.BytesIO:
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            # Add Dockerfile
            dockerfile_bytes = self.dockerfile_data.encode("utf-8")
            dockerfile_info = tarfile.TarInfo(name="Dockerfile")
            dockerfile_info.size = len(dockerfile_bytes)
            tar.addfile(dockerfile_info, io.BytesIO(dockerfile_bytes))

            # Add entrypoint.sh
            entrypoint_data = self.entrypoint_data.encode("utf-8")
            entrypoint_info = tarfile.TarInfo(name="entrypoint.sh")
            entrypoint_info.size = len(entrypoint_data)
            entrypoint_info.mode = 0o755  # Make it executable
            tar.addfile(entrypoint_info, io.BytesIO(entrypoint_data))

        tar_stream.seek(0)
        return deepcopy(tar_stream)

    def build_container(self, client: docker.DockerClient, image_name: str, repo_url: str, force: bool = False) -> None:
        """Builds the Docker image if it does not exist or if force is True."""
        image_exists = False
        try:
            image = client.images.get(image_name)
            image_exists = True
            if force:
                logger.info("Force rebuild requested. Removing existing Docker image '%s'.", image_name)
                client.images.remove(image=image.id, force=True)
                image_exists = False
            else:
                logger.info("Docker image '%s' found locally.", image_name)
        except ImageNotFound:
            pass  # Image doesn't exist or was removed, proceed to build

        if not image_exists:
            if repo_url:
                logger.info("Building Docker image '%s' with REPO_URL=%s", image_name, repo_url)
                try:
                    client.images.build(
                        fileobj=self.tarball_stream,
                        custom_context=True,
                        tag=image_name,
                        buildargs={"REPO_URL": repo_url},
                        rm=True,
                    )
                except DockerException as exc:
                    sys.exit(f"Failed to build image {image_name}: {exc}")
            else:
                raise RuntimeError(f"Docker image '{image_name}' not found and no REPO_URL provided for build.")  # noqa: TRY003

        if not client.images.get(image_name):
            raise RuntimeError(f"Image '{image_name}' failed to build and is not found.")  # noqa: TRY003
