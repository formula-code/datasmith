from __future__ import annotations

import io
import tarfile
from pathlib import Path

import docker
from docker.errors import DockerException, ImageNotFound

from datasmith.logging_config import get_logger

logger = get_logger("docker.context")


class DockerContext:
    """
    A docker context stores all the necessary files to build a docker container
    for running ASV benchmarks. It includes the Dockerfile, entrypoint script,
    and a script to build the container.

    This allows customizing the Docker image without needing to modify the Dockerfile directly.
    """

    default_dockerfile_loc = Path(__file__).parent / "Dockerfile"
    default_entrypoint_loc = Path(__file__).parent / "entrypoint.sh"
    default_builder_loc = Path(__file__).parent / "docker_build.sh"
    dockerfile_data: str
    entrypoint_data: str
    building_data: str

    def __init__(
        self, building_data: str | None = None, dockerfile_data: str | None = None, entrypoint_data: str | None = None
    ):
        if building_data is None:
            building_data = self.default_builder_loc.read_text()
        if dockerfile_data is None:
            dockerfile_data = self.default_dockerfile_loc.read_text()
        if entrypoint_data is None:
            entrypoint_data = self.default_entrypoint_loc.read_text()

        self.building_data = building_data
        self.dockerfile_data = dockerfile_data
        self.entrypoint_data = entrypoint_data

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

            # Add docker_build.sh
            building_data = self.building_data.encode("utf-8")
            builder_info = tarfile.TarInfo(name="docker_build.sh")
            builder_info.size = len(building_data)
            builder_info.mode = 0o755  # Make it executable
            tar.addfile(builder_info, io.BytesIO(building_data))

        # Reset the stream position to the beginning
        tar_stream.seek(0)
        return tar_stream

    def build_container(
        self, client: docker.DockerClient, image_name: str, build_args: dict[str, str], force: bool = False
    ) -> None:
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
            logger.info("Docker image '%s' not found locally. Building new image.", image_name)
            pass  # Image doesn't exist or was removed, proceed to build

        if not image_exists:
            if len(build_args):
                build_args_str = " --build-arg ".join(f"{k}={v}" for k, v in build_args.items())
                logger.info("$ docker build -t %s src/datasmith/docker/ --build-arg %s", image_name, build_args_str)
                try:
                    client.images.build(
                        fileobj=self.build_tarball_stream(),
                        custom_context=True,
                        tag=image_name,
                        buildargs=build_args,
                    )
                except DockerException:
                    logger.exception("Failed to build Docker image '%s'", image_name)
            else:
                raise RuntimeError(f"Docker image '{image_name}' not found and no REPO_URL provided for build.")

        if not client.images.get(image_name):
            raise RuntimeError(f"Image '{image_name}' failed to build and is not found.")


class ContextRegistry:
    """Registry for Docker contexts to avoid rebuilding the same context multiple times."""

    def __init__(self, registry: dict[str, DockerContext] | None = None, default_context: DockerContext | None = None):
        if registry is None:
            registry = {}
        self.registry = registry

        if "default" not in self.registry:
            if default_context is None:
                default_context = DockerContext()
            self.registry["default"] = default_context
            logger.debug("Default Docker context initialized.")

    def register(self, key: str, context: DockerContext) -> None:
        """Register a new Docker context."""
        if key in self.registry:
            logger.warning(f"Context '{key}' is already registered, overwriting.")
        self.registry[key] = context
        logger.debug(f"Registered Docker context: {key}")

    def get(self, key: str) -> DockerContext:
        """
        Retrieve a Docker context by key using hierarchical matching.
        "asv-astropy-astropy-14134" should query these queries in-order:
            "asv-astropy-astropy-14134"
            "asv-astropy-astropy"
        """
        # Build candidate keys in the required order, deduplicated while preserving order.
        candidates = [key]

        if "-" in key:
            # e.g., "asv-owner-repo-sha" -> "asv-owner-repo"
            owner_repo_key = key.rsplit("-", 1)[0]
            candidates.append(owner_repo_key)

        # Preserve order but remove duplicates
        seen = set()
        ordered_candidates = []
        for c in candidates:
            if c not in seen:
                ordered_candidates.append(c)
                seen.add(c)

        # Try each candidate in order
        for candidate in ordered_candidates:
            if candidate in self.registry:
                if candidate == key:
                    logger.debug(f"Found exact context for key '{key}'.")
                else:
                    logger.debug(f"Found fallback context '{candidate}' for key '{key}'.")
                return self.registry[candidate]

        logger.info(f"No context found for key '{key}'. Using default context.")
        return self.registry["default"]

    def __getitem__(self, key: str) -> DockerContext:
        return self.get(key)
