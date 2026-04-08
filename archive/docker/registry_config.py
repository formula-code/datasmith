"""Registry configuration abstraction for container registries.

This module provides shared abstractions for working with DockerHub
(and potentially other container registries in the future) in a unified way.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RegistryType(Enum):
    """Supported container registry types."""

    DOCKERHUB = "dockerhub"
    GCR = "gcr"  # Google Container Registry (future)
    GHCR = "ghcr"  # GitHub Container Registry (future)


@dataclass
class RegistryConfig:
    """Configuration for a container registry.

    This dataclass encapsulates all configuration needed to publish images
    to a specific registry type. It supports:
    - Common fields used by all registries
    - Registry-specific fields (DockerHub namespace, etc.)
    - Loading from environment variables
    - Validation of required fields per registry type
    """

    registry_type: RegistryType

    # Common fields (apply to all registry types)
    repository_mode: str = "single"  # "single" or "mirror"
    single_repo: str = "all"  # Repository name when mode="single"
    skip_existing: bool = True  # Skip images that already exist
    parallelism: int = 4  # Number of concurrent push operations
    verbose: bool = True  # Enable detailed logging

    # DockerHub-specific fields
    namespace: str | None = None  # DockerHub namespace (user or org)
    username: str | None = None  # DockerHub username
    password: str | None = None  # DockerHub password/token
    dockerhub_repo_prefix: str | None = None  # Prefix for mirror mode

    # GCR-specific fields (future)
    project_id: str | None = None  # GCP project ID
    gcr_hostname: str | None = None  # e.g., "gcr.io", "us.gcr.io"

    # GHCR-specific fields (future)
    github_token: str | None = None  # GitHub personal access token
    github_owner: str | None = None  # GitHub user or org

    # Additional options
    extra_options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, registry_type: RegistryType) -> "RegistryConfig":
        """
        Create registry configuration from environment variables.

        Environment variables by registry type:

        DockerHub:
          - DOCKERHUB_NAMESPACE (required)
          - DOCKERHUB_USERNAME (required)
          - DOCKERHUB_TOKEN or DOCKERHUB_PASSWORD (required)
          - DOCKERHUB_REPO_PREFIX (optional)
          - DOCKERHUB_REPOSITORY_MODE (default: "single")
          - DOCKERHUB_SINGLE_REPO (default: "all")

        Common:
          - REGISTRY_SKIP_EXISTING (default: "true")
          - REGISTRY_PARALLELISM (default: "4")
          - REGISTRY_VERBOSE (default: "true")

        Args:
            registry_type: Type of registry to configure

        Returns:
            RegistryConfig instance populated from environment

        Raises:
            ValueError: If required environment variables are missing
        """
        # Common configuration
        skip_existing = os.environ.get("REGISTRY_SKIP_EXISTING", "true").lower() in ("true", "1", "yes")
        parallelism = int(os.environ.get("REGISTRY_PARALLELISM", "4"))
        verbose = os.environ.get("REGISTRY_VERBOSE", "true").lower() in ("true", "1", "yes")

        if registry_type == RegistryType.DOCKERHUB:
            namespace = os.environ.get("DOCKERHUB_NAMESPACE")
            username = os.environ.get("DOCKERHUB_USERNAME")
            password = os.environ.get("DOCKERHUB_TOKEN") or os.environ.get("DOCKERHUB_PASSWORD")

            if not namespace:
                raise ValueError("DOCKERHUB_NAMESPACE environment variable is required for DockerHub registry")

            return cls(
                registry_type=registry_type,
                namespace=namespace,
                username=username,
                password=password,
                dockerhub_repo_prefix=os.environ.get("DOCKERHUB_REPO_PREFIX"),
                repository_mode=os.environ.get("DOCKERHUB_REPOSITORY_MODE", "single"),
                single_repo=os.environ.get("DOCKERHUB_SINGLE_REPO", "all"),
                skip_existing=skip_existing,
                parallelism=parallelism,
                verbose=verbose,
            )

        elif registry_type == RegistryType.GCR:
            # Future implementation
            raise NotImplementedError("GCR registry support not yet implemented")

        elif registry_type == RegistryType.GHCR:
            # Future implementation
            raise NotImplementedError("GHCR registry support not yet implemented")

        else:
            raise ValueError(f"Unsupported registry type: {registry_type}")

    def validate(self) -> None:
        """
        Validate that required fields are set for the registry type.

        Raises:
            ValueError: If required fields are missing
        """
        if self.registry_type == RegistryType.DOCKERHUB:
            if not self.namespace:
                raise ValueError("namespace is required for DockerHub registry")
            # Note: username/password validation happens in dockerhub.py
            # to allow fallback to docker config

        elif self.registry_type == RegistryType.GCR:
            if not self.project_id:
                raise ValueError("project_id is required for GCR registry")

        elif self.registry_type == RegistryType.GHCR and not self.github_owner:
            raise ValueError("github_owner is required for GHCR registry")

        # Validate common fields
        if self.repository_mode not in ("single", "mirror"):
            raise ValueError(f"repository_mode must be 'single' or 'mirror', got: {self.repository_mode}")

        if self.parallelism < 1:
            raise ValueError(f"parallelism must be >= 1, got: {self.parallelism}")

    def get_registry_url(self) -> str:
        """
        Get the registry URL for this configuration.

        Returns:
            Registry URL (e.g., "docker.io")

        Raises:
            NotImplementedError: For registry types that need account-specific URLs
        """
        if self.registry_type == RegistryType.DOCKERHUB:
            return "docker.io"

        elif self.registry_type == RegistryType.GCR:
            return self.gcr_hostname or "gcr.io"

        elif self.registry_type == RegistryType.GHCR:
            return "ghcr.io"

        else:
            raise ValueError(f"Unsupported registry type: {self.registry_type}")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            Dictionary representation of the config
        """
        return {
            "registry_type": self.registry_type.value,
            "repository_mode": self.repository_mode,
            "single_repo": self.single_repo,
            "skip_existing": self.skip_existing,
            "parallelism": self.parallelism,
            "verbose": self.verbose,
            "namespace": self.namespace,
            "username": self.username,
            "password": "***" if self.password else None,  # Redact password
            "dockerhub_repo_prefix": self.dockerhub_repo_prefix,
            "project_id": self.project_id,
            "gcr_hostname": self.gcr_hostname,
            "github_token": "***" if self.github_token else None,  # Redact token
            "github_owner": self.github_owner,
            "extra_options": self.extra_options,
        }

    def __str__(self) -> str:
        """String representation with redacted credentials."""
        data = self.to_dict()
        return f"RegistryConfig({', '.join(f'{k}={v}' for k, v in data.items() if v is not None)})"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return self.__str__()


def get_default_config(registry_type: RegistryType) -> RegistryConfig:
    """
    Get default configuration for a registry type.

    This is a convenience function that creates a config with sensible defaults
    and attempts to load from environment variables.

    Args:
        registry_type: Type of registry

    Returns:
        RegistryConfig with defaults and environment values
    """
    try:
        config = RegistryConfig.from_env(registry_type)
        config.validate()
    except ValueError as e:
        raise ValueError(
            f"Failed to create default config for {registry_type.value}: {e}\n"
            f"Ensure required environment variables are set."
        ) from e
    else:
        return config
