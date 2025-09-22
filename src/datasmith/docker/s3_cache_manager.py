from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError

from datasmith.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class S3CacheConfig:
    """Configuration for S3-based Docker layer caching."""

    bucket: str = os.environ.get("AWS_S3_BUCKET_DOCKER", "")
    prefix: str = "docker-cache"
    region: str = os.environ.get("AWS_REGION", "us-east-1")
    max_cache_age_days: int = 30  # Clean up cache layers older than this
    max_cache_size_gb: int = 100  # Maximum total cache size
    compression: bool = True  # Use gzip compression for cache metadata
    cache_ttl_hours: int = 720  # How long to keep cache entries (30 days)


class S3DockerCacheManager:
    """
    Manages Docker layer caching using S3 as a backend.

    This class provides:
    1. Layer push/pull operations to/from S3
    2. Cache metadata management
    3. Cache cleanup and size management
    4. Integration with Docker BuildKit cache mounts
    """

    def __init__(self, config: S3CacheConfig):
        self.config = config
        self.s3 = boto3.client("s3", region_name=config.region)
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Ensure the S3 bucket exists, create if it doesn't."""
        try:
            self.s3.head_bucket(Bucket=self.config.bucket)
            logger.debug("S3 cache bucket %s exists", self.config.bucket)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.info("Creating S3 cache bucket %s", self.config.bucket)
                try:
                    if self.config.region == "us-east-1":
                        # us-east-1 doesn't need LocationConstraint
                        self.s3.create_bucket(Bucket=self.config.bucket)
                    else:
                        self.s3.create_bucket(
                            Bucket=self.config.bucket,
                            CreateBucketConfiguration={"LocationConstraint": self.config.region},
                        )
                    logger.info("Created S3 cache bucket %s", self.config.bucket)
                except ClientError:
                    logger.exception("Failed to create S3 bucket %s", self.config.bucket)
                    raise
            else:
                logger.exception("Error checking S3 bucket %s", self.config.bucket)
                raise

    def _get_cache_key(self, layer_id: str, cache_type: str = "layer") -> str:
        """Generate a cache key for a layer or metadata."""
        return f"{self.config.prefix}/{cache_type}/{layer_id}"

    def _get_metadata_key(self, build_context_hash: str) -> str:
        """Get the metadata key for a build context."""
        return self._get_cache_key(f"metadata/{build_context_hash}", "metadata")

    def _hash_build_context(self, dockerfile_content: str, build_args: dict[str, str]) -> str:
        """Generate a hash for the build context to use as cache key."""
        # Include dockerfile content and build args in hash
        content = f"{dockerfile_content}:{json.dumps(build_args, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def get_cache_mount_config(self, dockerfile_content: str, build_args: dict[str, str]) -> dict[str, str]:
        """
        Get Docker BuildKit cache mount configuration for S3.

        Returns a dict with cache mount options that can be used with
        docker build --cache-from and --cache-to.
        """
        context_hash = self._hash_build_context(dockerfile_content, build_args)

        # S3 cache mount configuration
        cache_config = {
            "type": "s3",
            "bucket": self.config.bucket,
            "region": self.config.region,
            "prefix": f"{self.config.prefix}/layers/{context_hash}",
        }

        return cache_config

    def get_docker_build_args(self, dockerfile_content: str, build_args: dict[str, str]) -> list[str]:
        """
        Get Docker build arguments for cache-from and cache-to.

        Returns a list of command line arguments to add to docker build.
        """
        cache_config = self.get_cache_mount_config(dockerfile_content, build_args)

        # Build cache mount string
        cache_mount_str = (
            f"type=s3,bucket={cache_config['bucket']},region={cache_config['region']},prefix={cache_config['prefix']}"
        )

        return ["--cache-from", cache_mount_str, "--cache-to", f"{cache_mount_str},mode=max"]

    def store_build_metadata(
        self, dockerfile_content: str, build_args: dict[str, str], image_layers: list[str], build_duration: float
    ) -> None:
        """Store build metadata for cache optimization."""
        context_hash = self._hash_build_context(dockerfile_content, build_args)
        metadata_key = self._get_metadata_key(context_hash)

        metadata = {
            "context_hash": context_hash,
            "dockerfile_hash": hashlib.sha256(dockerfile_content.encode()).hexdigest(),
            "build_args": build_args,
            "image_layers": image_layers,
            "build_duration": build_duration,
            "timestamp": time.time(),
            "cache_hits": 0,
        }

        try:
            content_str = json.dumps(metadata, indent=2)
            if self.config.compression:
                import gzip

                content = gzip.compress(content_str.encode())
                content_type = "application/gzip"
            else:
                content = content_str.encode()
                content_type = "application/json"

            self.s3.put_object(
                Bucket=self.config.bucket,
                Key=metadata_key,
                Body=content,
                ContentType=content_type,
                Metadata={
                    "context-hash": context_hash,
                    "timestamp": str(int(time.time())),
                },
            )
            logger.debug("Stored build metadata for context %s", context_hash)
        except Exception:
            logger.warning("Failed to store build metadata")

    def get_build_metadata(self, dockerfile_content: str, build_args: dict[str, str]) -> dict[str, Any] | None:
        """Retrieve build metadata for cache optimization."""
        context_hash = self._hash_build_context(dockerfile_content, build_args)
        metadata_key = self._get_metadata_key(context_hash)

        try:
            response = self.s3.get_object(Bucket=self.config.bucket, Key=metadata_key)
            content = response["Body"].read()

            # Handle compression
            if response.get("ContentType") == "application/gzip":
                import gzip

                content = gzip.decompress(content)

            metadata: dict[str, Any] = json.loads(content.decode())

            # Update cache hit count
            metadata["cache_hits"] = metadata.get("cache_hits", 0) + 1
            self.store_build_metadata(
                dockerfile_content, build_args, metadata.get("image_layers", []), metadata.get("build_duration", 0.0)
            )

            logger.debug("Retrieved build metadata for context %s (hit #%d)", context_hash, metadata["cache_hits"])
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.debug("No cached metadata found for context %s", context_hash)
                return None
            else:
                logger.warning("Error retrieving build metadata")
                return None
        except Exception:
            logger.warning("Error parsing build metadata")
            return None
        else:
            return metadata

    def cleanup_old_cache(self) -> dict[str, int]:  # noqa: C901
        """
        Clean up old cache entries based on age and size limits.

        Returns a dict with cleanup statistics.
        """
        stats = {"deleted_metadata": 0, "deleted_layers": 0, "freed_bytes": 0}
        cutoff_time = time.time() - (self.config.max_cache_age_days * 24 * 3600)

        try:
            # List all cache objects
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.config.bucket, Prefix=self.config.prefix)

            objects_to_delete = []
            total_size = 0

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    size = obj["Size"]
                    last_modified = obj["LastModified"].timestamp()

                    total_size += size

                    # Check if object is too old
                    if last_modified < cutoff_time:
                        objects_to_delete.append({"Key": key})
                        stats["freed_bytes"] += size

                        if "metadata/" in key:
                            stats["deleted_metadata"] += 1
                        else:
                            stats["deleted_layers"] += 1

            # Also check size limit
            size_limit_bytes = self.config.max_cache_size_gb * 1024 * 1024 * 1024
            if total_size > size_limit_bytes:
                # Sort by last modified and delete oldest first
                logger.info(
                    "Cache size %d GB exceeds limit %d GB, cleaning up",
                    total_size // (1024**3),
                    self.config.max_cache_size_gb,
                )

                # This is a simplified cleanup - in production you might want
                # more sophisticated LRU-based cleanup
                for obj in sorted(objects_to_delete, key=lambda x: x["Key"]):
                    if total_size <= size_limit_bytes:
                        break
                    objects_to_delete.append(obj)

            # Delete objects in batches
            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):  # S3 batch delete limit
                    batch = objects_to_delete[i : i + 1000]
                    self.s3.delete_objects(Bucket=self.config.bucket, Delete={"Objects": batch})
                    logger.info("Deleted %d cache objects", len(batch))

            logger.info("Cache cleanup completed: %s", stats)
            return stats  # noqa: TRY300

        except Exception:
            logger.exception("Error during cache cleanup")
            return stats

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics and usage information."""
        stats: dict[str, Any] = {
            "total_objects": 0,
            "total_size_bytes": 0,
            "metadata_objects": 0,
            "layer_objects": 0,
            "oldest_object": None,
            "newest_object": None,
        }

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.config.bucket, Prefix=self.config.prefix)

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    size = obj["Size"]
                    last_modified = obj["LastModified"]

                    stats["total_objects"] += 1
                    stats["total_size_bytes"] += size

                    if "metadata/" in key:
                        stats["metadata_objects"] += 1
                    else:
                        stats["layer_objects"] += 1

                    if stats["oldest_object"] is None or last_modified < stats["oldest_object"]:
                        stats["oldest_object"] = last_modified
                    if stats["newest_object"] is None or last_modified > stats["newest_object"]:
                        stats["newest_object"] = last_modified

            # Convert to human-readable sizes
            if stats["total_size_bytes"] is not None:
                stats["total_size_gb"] = stats["total_size_bytes"] / (1024**3)
                stats["total_size_mb"] = stats["total_size_bytes"] / (1024**2)

        except Exception:
            logger.exception("Error getting cache stats")

        return stats

    def invalidate_cache(self, dockerfile_content: str, build_args: dict[str, str]) -> None:
        """Invalidate cache for a specific build context."""
        context_hash = self._hash_build_context(dockerfile_content, build_args)

        try:
            # Delete metadata
            metadata_key = self._get_metadata_key(context_hash)
            self.s3.delete_object(Bucket=self.config.bucket, Key=metadata_key)

            # Delete associated layers
            layer_prefix = f"{self.config.prefix}/layers/{context_hash}/"
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.config.bucket, Prefix=layer_prefix)

            objects_to_delete = []
            for page in pages:
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                self.s3.delete_objects(Bucket=self.config.bucket, Delete={"Objects": objects_to_delete})

            logger.info("Invalidated cache for context %s (%d objects)", context_hash, len(objects_to_delete) + 1)

        except Exception:
            logger.warning("Error invalidating cache")
