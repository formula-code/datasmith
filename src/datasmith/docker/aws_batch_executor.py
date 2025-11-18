from __future__ import annotations

import base64
import dataclasses
import json
import random
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3

from datasmith.core.models import BuildResult, Task
from datasmith.docker.context import DockerContext
from datasmith.docker.s3_cache_manager import S3CacheConfig, S3DockerCacheManager
from datasmith.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class AwsBatchConfig:
    """Extended AWS configuration for batch build and benchmark execution."""

    region: str
    s3_bucket: str
    s3_prefix: str = "datasmith-batch-execution"
    subnet_id: str = ""  # required
    security_group_ids: Sequence[str] = ()  # required
    iam_instance_profile_name: str = ""  # required
    ami_id: str = ""  # AL2023 (Docker available) or a custom AMI with docker preinstalled
    instance_type: str = "c6i.xlarge"  # Larger instance for benchmarks
    key_name: str | None = None
    spot_max_price: str | None = None
    tags: Mapping[str, str] = dataclasses.field(default_factory=dict)
    root_volume_gb: int = 100
    gp3_iops: int | None = None
    gp3_throughput: int | None = None
    stream_logs: bool = True  # Enable real-time log streaming via SSM
    log_output_dir: str = "output/batch_logs"  # Directory to store streamed logs

    # Batch tuning
    max_tasks_per_instance: int = 100  # Number of tasks (build + benchmark) per instance
    batch_timeout_s: int = 2 * 60 * 60  # 2 hours max time per batch
    poll_interval_s: int = 30  # Poll every 30 seconds
    max_batch_retries: int = 1  # relaunch if batch times out

    # Benchmark specific
    num_cores_per_task: int = 4  # CPU cores per benchmark task
    asv_args: str = "--append-samples -a rounds=2 -a repeat=2 --python=same"

    # Docker layer caching
    enable_s3_cache: bool = True  # Enable S3-based Docker layer caching
    cache_bucket: str = ""  # S3 bucket for Docker layer cache (required if enable_s3_cache=True)
    cache_prefix: str = "docker-cache"  # S3 prefix for cache objects
    cache_region: str = ""  # S3 region for cache (defaults to same as region)
    max_cache_age_days: int = 30  # Clean up cache layers older than this
    max_cache_size_gb: int = 400  # Maximum total cache size

    # Buildx configuration
    use_buildx: bool = True  # Use docker buildx for advanced caching and multi-platform builds
    buildx_builder_name: str = "aws-builder"  # Name for the buildx builder instance


@dataclass
class BatchTask:
    """Represents a single task (build + benchmark) in a batch."""

    task: Task
    context: DockerContext
    machine_args: dict[str, str]
    asv_args: str
    num_cores: int
    task_id: str  # Unique identifier for this task in the batch


@dataclass
class BatchResult:
    """Result of a batch execution containing build and benchmark results."""

    task_id: str
    build_result: BuildResult
    benchmark_exit_code: int
    benchmark_files: dict[str, str]
    benchmark_logs: str
    duration_s: float


class AWSBatchExecutor:
    """
    Executes Docker builds and ASV benchmarks in batches on AWS EC2 instances.

    This class provides a scalable alternative to local execution by:
    1. Batching tasks into groups of ~100 per EC2 instance
    2. Building Docker images and running benchmarks on each instance
    3. Collecting results back via S3
    4. Managing instance lifecycle (launch, monitor, terminate)
    """

    def __init__(self, cfg: AwsBatchConfig):
        self.cfg = cfg
        self.s3 = boto3.client("s3", region_name=cfg.region)
        self.ec2 = boto3.client("ec2", region_name=cfg.region)

        # Initialize S3 cache manager if enabled
        self.cache_manager = None
        if cfg.enable_s3_cache:
            if not cfg.cache_bucket:
                raise ValueError("cache_bucket must be specified when enable_s3_cache=True")

            cache_region = cfg.cache_region or cfg.region
            cache_config = S3CacheConfig(
                bucket=cfg.cache_bucket,
                prefix=cfg.cache_prefix,
                region=cache_region,
                max_cache_age_days=cfg.max_cache_age_days,
                max_cache_size_gb=cfg.max_cache_size_gb,
            )
            self.cache_manager = S3DockerCacheManager(cache_config)
            logger.info(
                "Initialized S3 Docker cache manager with bucket=%s, prefix=%s", cfg.cache_bucket, cfg.cache_prefix
            )

    def execute_batch(
        self,
        tasks: Sequence[tuple[Task, DockerContext]],
        machine_args: dict[str, str],
        asv_args: str,
        *,
        run_id: str | None = None,
    ) -> list[BatchResult]:
        """
        Execute a batch of tasks (build + benchmark) on AWS EC2 instances.

        Args:
            tasks: List of (Task, DockerContext) pairs to execute
            machine_args: ASV machine configuration
            asv_args: ASV command line arguments
            run_id: Optional run identifier for tracking

        Returns:
            List of BatchResult objects containing build and benchmark results
        """
        if not run_id:
            run_id = f"batch-{int(time.time())}-{random.randint(1000, 9999)}"  # noqa: S311

        logger.info("Starting batch execution for %d tasks with run_id=%s", len(tasks), run_id)

        # Prepare batch tasks
        batch_tasks = []
        for i, (task, context) in enumerate(tasks):
            batch_task = BatchTask(
                task=task,
                context=context,
                machine_args=machine_args,
                asv_args=asv_args,
                num_cores=self.cfg.num_cores_per_task,
                task_id=f"{run_id}-task-{i:03d}",
            )
            batch_tasks.append(batch_task)

        # Split into batches of max_tasks_per_instance
        batches = []
        for i in range(0, len(batch_tasks), self.cfg.max_tasks_per_instance):
            batch = batch_tasks[i : i + self.cfg.max_tasks_per_instance]
            batches.append(batch)

        logger.info("Split %d tasks into %d batches", len(tasks), len(batches))

        # Execute batches in parallel using ThreadPoolExecutor
        all_results = []
        with ThreadPoolExecutor(max_workers=min(len(batches), 10)) as executor:
            future_to_batch = {
                executor.submit(self._execute_single_batch, batch, run_id, batch_idx): (batch_idx, batch)
                for batch_idx, batch in enumerate(batches)
            }

            # Collect results as they complete
            for future in as_completed(future_to_batch):
                batch_idx, batch = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    logger.info("Completed batch %d/%d with %d tasks", batch_idx + 1, len(batches), len(batch))
                except Exception as e:
                    logger.exception("Batch %d failed", batch_idx + 1)
                    # Create failure results for this batch
                    failure_results = [
                        BatchResult(
                            task_id=task.task_id,
                            build_result=BuildResult(
                                ok=False,
                                image_name="",
                                image_id=None,
                                rc=1,
                                duration_s=0.0,
                                stderr_tail=str(e),
                                stdout_tail="",
                            ),
                            benchmark_exit_code=1,
                            benchmark_files={},
                            benchmark_logs=str(e),
                            duration_s=0.0,
                        )
                        for task in batch
                    ]
                    all_results.extend(failure_results)

        logger.info("Completed batch execution: %d results", len(all_results))
        return all_results

    def _execute_single_batch(
        self,
        batch: Sequence[BatchTask],
        run_id: str,
        batch_idx: int,
    ) -> list[BatchResult]:
        """Execute a single batch of tasks on one EC2 instance."""

        # Upload batch data to S3
        batch_data_key = self._upload_batch_data(batch, run_id, batch_idx)

        # Launch EC2 instance
        instance_id = self._launch_batch_instance(batch, run_id, batch_idx, batch_data_key)

        try:
            # Wait for results while streaming logs
            results = self._wait_for_batch_results(batch, run_id, batch_idx, instance_id)
            return results
        finally:
            # Clean up instance
            self._terminate_instance(instance_id)

    def _upload_batch_data(
        self,
        batch: Sequence[BatchTask],
        run_id: str,
        batch_idx: int,
    ) -> str:
        """Upload batch configuration and contexts to S3."""

        # Prepare batch data
        batch_data: dict[str, Any] = {
            "run_id": run_id,
            "batch_idx": batch_idx,
            "config": {
                "num_cores_per_task": self.cfg.num_cores_per_task,
                "asv_args": self.cfg.asv_args,
                "batch_timeout_s": self.cfg.batch_timeout_s,
            },
            "tasks": [],
        }

        # Serialize each task's context and metadata
        for batch_task in batch:
            task_data = {
                "task_id": batch_task.task_id,
                "task": {
                    "owner": batch_task.task.owner,
                    "repo": batch_task.task.repo,
                    "sha": batch_task.task.sha,
                    "commit_date": batch_task.task.commit_date,
                    "tag": batch_task.task.tag,
                },
                "context": batch_task.context.to_dict(),
                "machine_args": batch_task.machine_args,
                "asv_args": batch_task.asv_args,
                "num_cores": batch_task.num_cores,
            }
            batch_data["tasks"].append(task_data)  # pyright: ignore[reportListAppend]

        # Upload to S3
        batch_data_key = f"{self.cfg.s3_prefix}/batches/{run_id}/batch-{batch_idx:03d}/batch-data.json"
        batch_data_json = json.dumps(batch_data, indent=2)

        self.s3.put_object(
            Bucket=self.cfg.s3_bucket,
            Key=batch_data_key,
            Body=batch_data_json.encode("utf-8"),
            ContentType="application/json",
        )

        logger.info("Uploaded batch data to s3://%s/%s", self.cfg.s3_bucket, batch_data_key)
        return batch_data_key

    def _launch_batch_instance(
        self,
        batch: Sequence[BatchTask],
        run_id: str,
        batch_idx: int,
        batch_data_key: str,
    ) -> str:
        """Launch an EC2 instance to execute the batch."""

        user_data = self._generate_user_data(batch_data_key)

        spec = {
            "ImageId": self.cfg.ami_id,
            "InstanceType": self.cfg.instance_type,
            "SubnetId": self.cfg.subnet_id,
            "SecurityGroupIds": list(self.cfg.security_group_ids),
            "IamInstanceProfile": {"Name": self.cfg.iam_instance_profile_name},
            "UserData": base64.b64encode(user_data.encode("utf-8")).decode("ascii"),
            "TagSpecifications": [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": f"ds-batch-exec-{run_id}-b{batch_idx:03d}"},
                        {"Key": "RunId", "Value": run_id},
                        {"Key": "BatchIdx", "Value": str(batch_idx)},
                        *({"Key": k, "Value": v} for k, v in self.cfg.tags.items()),
                    ],
                }
            ],
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "VolumeSize": self.cfg.root_volume_gb,
                        "VolumeType": "gp3",
                        "DeleteOnTermination": True,
                        **({"Iops": self.cfg.gp3_iops} if self.cfg.gp3_iops else {}),
                        **({"Throughput": self.cfg.gp3_throughput} if self.cfg.gp3_throughput else {}),
                    },
                }
            ],
        }

        # Use Spot only when a max price is explicitly provided; otherwise On-Demand
        if self.cfg.spot_max_price:
            spec["InstanceMarketOptions"] = {
                "MarketType": "spot",
                "SpotOptions": {
                    "InstanceInterruptionBehavior": "terminate",
                    "MaxPrice": self.cfg.spot_max_price,
                },
            }

        if self.cfg.key_name:
            spec["KeyName"] = self.cfg.key_name

        resp = self.ec2.run_instances(MinCount=1, MaxCount=1, **spec)
        instance_id: str = resp["Instances"][0]["InstanceId"]

        logger.info("Launched instance %s for batch %d", instance_id, batch_idx)
        return instance_id

    def _generate_user_data(self, batch_data_key: str) -> str:
        """Generate user data script for EC2 instance (Amazon Linux 2 compatible)."""
        script = """#!/bin/bash
set -eox pipefail

# Log user-data to a file for debugging
exec > >(tee -a /var/log/user-data.log) 2>&1

retry() {
    local n=0
    local max=5
    local delay=5
    while true; do
    "$@" && break || {
        n=$((n+1))
        if [ $n -ge $max ]; then
        echo "Command failed after $n attempts: $*"
        return 1
        fi
        echo "Retry $n/$max for: $*"; sleep $delay;
    }
    done
}

echo "==> Installing prerequisites (Amazon Linux 2)"
retry yum update -y
retry yum install -y jq git python3 python3-pip awscli bc
retry amazon-linux-extras install -y docker

echo "==> Enabling and starting Docker"
systemctl enable docker || true
systemctl start docker || true

# Wait for Docker to be ready
for i in $(seq 1 10); do
    if docker info >/dev/null 2>&1; then
    break
    fi
    echo "Waiting for Docker daemon... ($i/10)"
    sleep 2
done

echo "==> Installing Python packages"
retry pip3 install --upgrade boto3 docker

echo "==> Preparing workspace"
mkdir -p /opt/datasmith
cd /opt/datasmith

echo "==> Downloading batch data"
retry aws s3 cp "s3://{s3_bucket}/{batch_data_key}" batch-data.json

echo "==> Parsing batch data"
BATCH_DATA=$(cat batch-data.json)
RUN_ID=$(echo "$BATCH_DATA" | jq -r '.run_id')
BATCH_IDX=$(echo "$BATCH_DATA" | jq -r '.batch_idx')
NUM_CORES=$(echo "$BATCH_DATA" | jq -r '.config.num_cores_per_task')
ASV_ARGS=$(echo "$BATCH_DATA" | jq -r '.config.asv_args')
BATCH_TIMEOUT=$(echo "$BATCH_DATA" | jq -r '.config.batch_timeout_s')

echo "Starting batch execution: run_id=$RUN_ID, batch_idx=$BATCH_IDX"

TASK_COUNT=$(echo "$BATCH_DATA" | jq '.tasks | length')
for i in $(seq 0 $((TASK_COUNT - 1))); do
    echo "Processing task $((i + 1))/$TASK_COUNT"

    TASK_DATA=$(echo "$BATCH_DATA" | jq ".tasks[$i]")
    TASK_ID=$(echo "$TASK_DATA" | jq -r '.task_id')
    OWNER=$(echo "$TASK_DATA" | jq -r '.task.owner' | tr '[:upper:]' '[:lower:]')
    REPO=$(echo "$TASK_DATA" | jq -r '.task.repo' | tr '[:upper:]' '[:lower:]')
    SHA=$(echo "$TASK_DATA" | jq -r '.task.sha' | tr '[:upper:]' '[:lower:]')
    TAG=$(echo "$TASK_DATA" | jq -r '.task.tag' | tr '[:upper:]' '[:lower:]')
    ENV_PAYLOAD=$(echo "$TASK_DATA" | jq -r '.task.env_payload // ""')

    mkdir -p "task-$TASK_ID"
    cd "task-$TASK_ID"

    echo "==> Writing Docker context files"
    echo "$TASK_DATA" | jq -r '.context.dockerfile_data' > Dockerfile
    echo "$TASK_DATA" | jq -r '.context.entrypoint_data' > entrypoint.sh
    echo "$TASK_DATA" | jq -r '.context.building_data' > docker_build_pkg.sh
    echo "$TASK_DATA" | jq -r '.context.env_building_data' > docker_build_env.sh
    echo "$TASK_DATA" | jq -r '.context.base_building_data' > docker_build_base.sh
    echo "$TASK_DATA" | jq -r '.context.run_building_data' > docker_build_run.sh
    echo "$TASK_DATA" | jq -r '.context.profile_data' > profile.sh
    echo "$TASK_DATA" | jq -r '.context.run_tests_data' > run_tests.sh
    chmod +x entrypoint.sh docker_build_*.sh profile.sh run_tests.sh || true

    echo "==> Building Docker image for $OWNER/$REPO@$SHA"
    REPO_URL="https://github.com/$OWNER/$REPO.git"
    IMAGE_NAME="$OWNER-$REPO-$SHA:$TAG"

    # Set up Docker BuildKit for advanced caching
    export DOCKER_BUILDKIT=1

    # Capture build timing and logs
    BUILD_START=$(date +%s.%N)
    BUILD_LOG_FILE="build.log"

    # Check if we should use buildx
    USE_BUILDX="{use_buildx}"
    BUILDER_NAME="{buildx_builder_name}"

    if [ "$USE_BUILDX" = "true" ]; then
        echo "Setting up docker buildx builder: $BUILDER_NAME"

        # Create buildx builder if it doesn't exist
        if ! docker buildx ls | grep -q "$BUILDER_NAME"; then
            echo "Creating buildx builder: $BUILDER_NAME"
            docker buildx create --name "$BUILDER_NAME" --use --driver docker-container || {
                echo "Failed to create buildx builder, falling back to default"
                docker buildx use default
            }
        else
            echo "Using existing buildx builder: $BUILDER_NAME"
            docker buildx use "$BUILDER_NAME"
        fi

        # Build Docker command with buildx and S3 cache support
        DOCKER_BUILD_CMD="timeout $BATCH_TIMEOUT docker buildx build --load --progress=plain -t $IMAGE_NAME . --build-arg REPO_URL=$REPO_URL --build-arg COMMIT_SHA=$SHA --build-arg ENV_PAYLOAD=\"$ENV_PAYLOAD\" --target $TAG"

        # Add S3 cache arguments if cache is enabled
        if [ "{enable_s3_cache}" = "true" ]; then
            CACHE_BUCKET="{cache_bucket}"
            CACHE_PREFIX="{cache_prefix}"
            CACHE_REGION="{cache_region}"

            # Generate cache mount configuration for buildx
            CACHE_FROM="type=s3,bucket=$CACHE_BUCKET,region=$CACHE_REGION,prefix=$CACHE_PREFIX/layers/$OWNER-$REPO-$SHA"
            CACHE_TO="type=s3,bucket=$CACHE_BUCKET,region=$CACHE_REGION,prefix=$CACHE_PREFIX/layers/$OWNER-$REPO-$SHA,mode=max"

            DOCKER_BUILD_CMD="$DOCKER_BUILD_CMD --cache-from $CACHE_FROM --cache-to $CACHE_TO"

            echo "Using buildx with S3 cache: bucket=$CACHE_BUCKET, prefix=$CACHE_PREFIX"
        else
            echo "Using buildx without S3 cache"
        fi
    else
        echo "Using standard docker build"

        # Build Docker command with S3 cache support (legacy)
        DOCKER_BUILD_CMD="timeout $BATCH_TIMEOUT docker build -t $IMAGE_NAME . --build-arg REPO_URL=$REPO_URL --build-arg COMMIT_SHA=$SHA --build-arg ENV_PAYLOAD=\"$ENV_PAYLOAD\" --target $TAG"

        # Add S3 cache arguments if cache is enabled
        if [ "{enable_s3_cache}" = "true" ]; then
            CACHE_BUCKET="{cache_bucket}"
            CACHE_PREFIX="{cache_prefix}"
            CACHE_REGION="{cache_region}"

            # Generate cache mount configuration
            CACHE_MOUNT="type=s3,bucket=$CACHE_BUCKET,region=$CACHE_REGION,prefix=$CACHE_PREFIX/layers/$OWNER-$REPO-$SHA"

            DOCKER_BUILD_CMD="$DOCKER_BUILD_CMD --cache-from $CACHE_MOUNT --cache-to $CACHE_MOUNT,mode=max"

            echo "Using S3 cache: bucket=$CACHE_BUCKET, prefix=$CACHE_PREFIX"
        fi
    fi

    if $DOCKER_BUILD_CMD > "$BUILD_LOG_FILE" 2>&1; then
    BUILD_RC=0
    BUILD_DURATION=$(echo "$(date +%s.%N) - $BUILD_START" | bc -l)
    BUILD_STDOUT_TAIL=$(tail -c 1000 "$BUILD_LOG_FILE" | base64 | tr -d '\n')
    BUILD_STDERR_TAIL=""
    echo "Build completed successfully in $BUILD_DURATION seconds"
    else
    BUILD_RC=$?
    BUILD_DURATION=$(echo "$(date +%s.%N) - $BUILD_START" | bc -l)
    BUILD_STDOUT_TAIL=$(tail -c 1000 "$BUILD_LOG_FILE" | base64 | tr -d '\n')
    BUILD_STDERR_TAIL=$(tail -c 1000 "$BUILD_LOG_FILE" | base64 | tr -d '\n')
    echo "Build failed with exit code $BUILD_RC after $BUILD_DURATION seconds"

    # Create failure result.json with jq (compatible with older jq versions)
    BUILD_LOG_B64=$(base64 -w 0 "$BUILD_LOG_FILE" 2>/dev/null || base64 "$BUILD_LOG_FILE" | tr -d '\n')
    jq -n \
        --arg task_id "$TASK_ID" \
        --arg build_rc "$BUILD_RC" \
        --arg build_duration "$BUILD_DURATION" \
        --arg stderr_tail "$BUILD_STDERR_TAIL" \
        --arg stdout_tail "$BUILD_STDOUT_TAIL" \
        --arg build_log "$BUILD_LOG_B64" '
    {
        task_id: $task_id,
        build_result: {
        ok: false,
        image_name: "",
        image_id: null,
        rc: ($build_rc|tonumber),
        duration_s: ($build_duration|tonumber),
        stderr_tail: $stderr_tail,
        stdout_tail: $stdout_tail
        },
        benchmark_exit_code: 1,
        benchmark_files: {
        "build.log": $build_log
        },
        benchmark_logs: ("Build failed with exit code " + $build_rc),
        duration_s: ($build_duration|tonumber)
    }' > result.json

    retry aws s3 cp result.json "s3://{s3_bucket}/{s3_prefix}/results/$RUN_ID/batch-$BATCH_IDX/$TASK_ID/result.json" || true
    cd ..
    continue
    fi

    echo "==> Running benchmark for $TASK_ID"
    CONTAINER_NAME="benchmark-$TASK_ID"
    mkdir -p output

    # Capture benchmark timing and logs
    BENCHMARK_START=$(date +%s.%N)
    # We must not use --rm if we want to access container logs after the run,
    # because --rm deletes the container immediately after exit.
    if timeout "$BATCH_TIMEOUT" docker run \
        --name "$CONTAINER_NAME" \
        --cpus="$NUM_CORES" \
        -v "$(pwd)/output:/output" \
        --entrypoint /profile.sh \
        "$IMAGE_NAME" \
        /output/profile "" > benchmark.log 2>&1; then
    BENCHMARK_EXIT_CODE=0
    echo "Benchmark completed successfully"
    else
    BENCHMARK_EXIT_CODE=$?
    echo "Benchmark failed with exit code $BENCHMARK_EXIT_CODE"
    fi

    BENCHMARK_DURATION=$(echo "$(date +%s.%N) - $BENCHMARK_START" | bc -l)

    # Save container logs alongside outputs
    docker logs "$CONTAINER_NAME" > output/container.log 2>&1 || true

    # Clean up the container after logs are saved
    docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || true
    echo "==> Collecting results for $TASK_ID (exit=$BENCHMARK_EXIT_CODE)"

    # Calculate total duration
    TOTAL_DURATION=$(echo "$BUILD_DURATION + $BENCHMARK_DURATION" | bc -l)

    # Build success result.json incrementally with jq (no sed / huge args)
    jq -n \
    --arg task_id "$TASK_ID" \
    --arg image_name "$IMAGE_NAME" \
    --arg build_rc "$BUILD_RC" \
    --arg build_duration "$BUILD_DURATION" \
    --arg stderr_tail "$BUILD_STDERR_TAIL" \
    --arg stdout_tail "$BUILD_STDOUT_TAIL" \
    --arg bench_exit "$BENCHMARK_EXIT_CODE" \
    --arg total_duration "$TOTAL_DURATION" \
    '
    {
    task_id: $task_id,
    build_result: {
        ok: true,
        image_name: $image_name,
        image_id: null,
        rc: ($build_rc|tonumber),
        duration_s: ($build_duration|tonumber),
        stderr_tail: $stderr_tail,
        stdout_tail: $stdout_tail
    },
    benchmark_exit_code: ($bench_exit|tonumber),
    benchmark_files: {},
    benchmark_logs: "",
    duration_s: ($total_duration|tonumber)
    }' > result.json

    # Add build.log (base64) if present
    if [ -f "$BUILD_LOG_FILE" ]; then
    BUILD_LOG_B64=$(base64 -w 0 "$BUILD_LOG_FILE" 2>/dev/null || base64 "$BUILD_LOG_FILE" | tr -d '\n')
    jq --arg build_log "$BUILD_LOG_B64" \
        '.benchmark_files += {"build.log": $build_log}' \
        result.json > result.tmp && mv result.tmp result.json
    fi

    # Add benchmark.log (base64) and set benchmark_logs to its base64 content
    if [ -f "benchmark.log" ]; then
    BENCHMARK_LOG_B64=$(base64 -w 0 "benchmark.log" 2>/dev/null || base64 "benchmark.log" | tr -d '\n')
    jq --arg benchmark_log "$BENCHMARK_LOG_B64" \
        '.benchmark_files += {"benchmark.log": $benchmark_log} | .benchmark_logs = $benchmark_log' \
        result.json > result.tmp && mv result.tmp result.json
    fi

    # Add any files from output/ (base64)
    if [ -d "output" ]; then
    for file in output/*; do
        if [ -f "$file" ]; then
        filename=$(basename "$file")
        FILE_B64=$(base64 -w 0 "$file" 2>/dev/null || base64 "$file" | tr -d '\n')
        jq --arg content "$FILE_B64" \
            --arg name "$filename" \
            '.benchmark_files += {($name): $content}' \
            result.json > result.tmp && mv result.tmp result.json
        fi
    done
    fi

    # Upload
    retry aws s3 cp result.json "s3://{s3_bucket}/{s3_prefix}/results/$RUN_ID/batch-$BATCH_IDX/$TASK_ID/result.json" || true

    docker rmi "$IMAGE_NAME" || true
    cd ..
done

echo "Batch execution completed for run_id=$RUN_ID, batch_idx=$BATCH_IDX"
# Allow S3 eventual consistency to settle
sleep 10
shutdown -h now || poweroff || halt
    """

        # Replace placeholders in the script
        return (
            script.replace("{s3_bucket}", self.cfg.s3_bucket)
            .replace("{s3_prefix}", self.cfg.s3_prefix)
            .replace("{batch_data_key}", batch_data_key)
            .replace("{enable_s3_cache}", str(self.cfg.enable_s3_cache).lower())
            .replace("{cache_bucket}", self.cfg.cache_bucket or "")
            .replace("{cache_prefix}", self.cfg.cache_prefix)
            .replace("{cache_region}", self.cfg.cache_region or self.cfg.region)
            .replace("{use_buildx}", str(self.cfg.use_buildx).lower())
            .replace("{buildx_builder_name}", self.cfg.buildx_builder_name)
        )

    def _stream_user_data_logs(self, instance_id: str, batch_idx: int, run_id: str, last_position: int = 0) -> int:
        """Stream user-data logs from EC2 instance to output directory and return the last position read."""
        try:
            # Use AWS Systems Manager to execute commands on the instance
            ssm = boto3.client("ssm", region_name=self.cfg.region)

            s3_log_key = f"{self.cfg.s3_prefix}/logs/{run_id}/batch-{batch_idx:03d}-{instance_id}.log"

            # Command to copy the log file to S3 to bypass the 24KB SSM output limit
            # This overwrites the same file each time, keeping it up to date
            command = f"aws s3 cp /var/log/user-data.log s3://{self.cfg.s3_bucket}/{s3_log_key} 2>/dev/null || echo 'Log file not found or S3 upload failed'"

            response = ssm.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": [command]},
                TimeoutSeconds=60,  # Increased timeout for S3 upload
            )

            command_id = response["Command"]["CommandId"]
            time.sleep(3)  # Give it time to complete

            # Wait for command to complete
            tries = 15  # Increased tries for S3 upload
            output = None
            while tries > 0:
                try:
                    output = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
                    if output["Status"] not in ["Pending", "InProgress"]:
                        break
                except Exception:  # noqa: S110
                    pass
                time.sleep(2)
                tries -= 1

            if output and output["Status"] == "Success":
                # Download the log file from S3
                try:
                    s3_response = self.s3.get_object(Bucket=self.cfg.s3_bucket, Key=s3_log_key)
                    full_log_content = s3_response["Body"].read().decode("utf-8")

                    # Create output directory for logs
                    log_dir = Path(self.cfg.log_output_dir) / run_id
                    log_dir.mkdir(parents=True, exist_ok=True)

                    # Write logs to batch-specific file
                    log_file = log_dir / f"batch-{batch_idx:03d}-{instance_id}.log"

                    # Always rewrite the entire file with the current content
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.write(full_log_content)

                    # Count total lines
                    total_lines = len([line for line in full_log_content.split("\n") if line.strip()])
                    if total_lines > 0:
                        logger.info(
                            "Updated log file with %d total lines from %s to %s (S3: s3://%s/%s)",
                            total_lines,
                            instance_id,
                            log_file,
                            self.cfg.s3_bucket,
                            s3_log_key,
                        )

                    # Return the length of the full content as the new position
                    return len(full_log_content.encode("utf-8"))

                except Exception as e:
                    logger.debug("Error downloading log file from S3 for %s: %s", instance_id, e)
                    return last_position
            else:
                logger.debug(
                    "SSM command failed for %s: %s",
                    instance_id,
                    output.get("Status", "Unknown") if output else "No output",
                )
                return last_position

        except Exception as e:
            # SSM agent may not be installed or IAM permissions may be missing
            # This is expected and we should continue without log streaming
            logger.debug("Error streaming user-data logs for %s: %s", instance_id, e)
            return last_position

    def _create_batch_summary_log(self, run_id: str, batch_idx: int, instance_id: str) -> Path:
        """Create a summary log file for the batch execution."""
        log_dir = Path(self.cfg.log_output_dir) / run_id
        log_dir.mkdir(parents=True, exist_ok=True)

        summary_file = log_dir / f"batch-{batch_idx:03d}-summary.log"

        # Create the S3 log key for reference
        s3_log_key = f"{self.cfg.s3_prefix}/logs/{run_id}/batch-{batch_idx:03d}-{instance_id}.log"

        with open(summary_file, "w", encoding="utf-8") as f:
            f.write("Batch Execution Summary\n")
            f.write("======================\n")
            f.write(f"Run ID: {run_id}\n")
            f.write(f"Batch Index: {batch_idx}\n")
            f.write(f"Instance ID: {instance_id}\n")
            f.write(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
            f.write(f"Log Directory: {log_dir}\n")
            f.write(f"Instance Log: batch-{batch_idx:03d}-{instance_id}.log\n")
            f.write(f"S3 Log File: s3://{self.cfg.s3_bucket}/{s3_log_key}\n")
            f.write("\n")
            f.write(f"Real-time logs are being streamed to: batch-{batch_idx:03d}-{instance_id}.log\n")
            f.write(f"Persistent S3 log file: s3://{self.cfg.s3_bucket}/{s3_log_key}\n")
            f.write(f"Use 'tail -f {summary_file}' to monitor this summary\n")
            f.write(f"Use 'tail -f {log_dir}/batch-{batch_idx:03d}-{instance_id}.log' to see instance logs\n")
            f.write(f"Use 'aws s3 cp s3://{self.cfg.s3_bucket}/{s3_log_key} -' to view S3 log directly\n")
            f.write("\n")

        return summary_file

    def _wait_for_batch_results(  # noqa: C901
        self,
        batch: Sequence[BatchTask],
        run_id: str,
        batch_idx: int,
        instance_id: str,
    ) -> list[BatchResult]:
        """Wait for batch results to be uploaded to S3 while streaming user-data logs."""

        deadline = time.time() + self.cfg.batch_timeout_s
        results: dict[str, BatchResult] = {}
        log_position = 0

        # Create summary log file
        summary_file = self._create_batch_summary_log(run_id, batch_idx, instance_id)
        logger.info(
            "Waiting for %d results from batch %d (streaming logs from %s to %s)",
            len(batch),
            batch_idx,
            instance_id,
            summary_file.parent,
        )

        while time.time() < deadline and len(results) < len(batch):
            # Stream user-data logs during polling if enabled (do this first)
            if self.cfg.stream_logs:
                log_position = self._stream_user_data_logs(instance_id, batch_idx, run_id, log_position)

            for batch_task in batch:
                if batch_task.task_id in results:
                    continue

                result_key = (
                    f"{self.cfg.s3_prefix}/results/{run_id}/batch-{batch_idx:03d}/{batch_task.task_id}/result.json"
                )

                try:
                    obj = self.s3.get_object(Bucket=self.cfg.s3_bucket, Key=result_key)
                    result_data = json.loads(obj["Body"].read().decode("utf-8"))

                    # Parse benchmark files (base64 encoded)
                    benchmark_files = {}
                    for filename, content_b64 in result_data.get("benchmark_files", {}).items():
                        try:
                            content = base64.b64decode(content_b64).decode("utf-8")
                            benchmark_files[filename] = content
                        except Exception:
                            benchmark_files[filename] = f"<binary content: {len(content_b64)} bytes>"

                    result = BatchResult(
                        task_id=batch_task.task_id,
                        build_result=BuildResult(
                            ok=result_data["build_result"]["ok"],
                            image_name=result_data["build_result"]["image_name"],
                            image_id=result_data["build_result"]["image_id"],
                            rc=result_data["build_result"]["rc"],
                            duration_s=result_data["build_result"]["duration_s"],
                            stderr_tail=result_data["build_result"]["stderr_tail"],
                            stdout_tail=result_data["build_result"]["stdout_tail"],
                        ),
                        benchmark_exit_code=result_data["benchmark_exit_code"],
                        benchmark_files=benchmark_files,
                        benchmark_logs=result_data["benchmark_logs"],
                        duration_s=result_data["duration_s"],
                    )

                    results[batch_task.task_id] = result
                    logger.info("Received result for task %s", batch_task.task_id)

                    # Update summary log
                    with open(summary_file, "a", encoding="utf-8") as f:
                        f.write(
                            f"[{time.strftime('%H:%M:%S')}] Completed task {batch_task.task_id} "
                            f"(build: {'✓' if result.build_result.ok else '✗'}, "
                            f"benchmark: {'✓' if result.benchmark_exit_code == 0 else '✗'})\n"
                        )

                except self.s3.exceptions.NoSuchKey:
                    continue
                except Exception as e:
                    logger.warning("Error fetching result for %s: %s", batch_task.task_id, e)
                    continue

            if len(results) < len(batch):
                time.sleep(self.cfg.poll_interval_s)

                if not self._is_instance_running(instance_id):
                    logger.warning("Instance %s has terminated, stopping batch result polling", instance_id)
                    break

        # Determine the reason for incomplete results
        instance_terminated = not self._is_instance_running(instance_id)

        if len(results) < len(batch):
            if instance_terminated:
                logger.warning(
                    "Instance terminated while waiting for batch results: got %d/%d", len(results), len(batch)
                )
            else:
                logger.warning("Timeout waiting for batch results: got %d/%d", len(results), len(batch))

        # Write final summary
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write("\n")
            f.write(f"Batch completed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n")
            f.write(f"Results: {len(results)}/{len(batch)} tasks completed\n")
            if len(results) < len(batch):
                if instance_terminated:
                    f.write(f"Status: INSTANCE_TERMINATED - {len(batch) - len(results)} tasks did not complete\n")
                else:
                    f.write(f"Status: TIMEOUT - {len(batch) - len(results)} tasks did not complete\n")
            else:
                f.write("Status: SUCCESS - All tasks completed\n")
            f.write(f"Log files available in: {summary_file.parent}\n")

        return list(results.values())

    def _is_instance_running(self, instance_id: str) -> bool:
        """Check if an EC2 instance is still running."""
        try:
            response = self.ec2.describe_instances(InstanceIds=[instance_id])
            if not response["Reservations"]:
                logger.warning("No reservations found for instance %s", instance_id)
                return False

            instance = response["Reservations"][0]["Instances"][0]
            state = instance["State"]["Name"]

            # Instance is considered running if it's in 'running' state
            # Other states like 'stopping', 'stopped', 'terminating', 'terminated' mean it's not running
            is_running = state == "running"

            if not is_running:
                logger.info("Instance %s is in state '%s' (not running)", instance_id, state)
                return False
            else:
                return True
        except Exception as e:
            logger.warning("Error checking instance state for %s: %s", instance_id, e)
            # If we can't check the state, assume it's still running to avoid false positives
            return True

    def _terminate_instance(self, instance_id: str) -> None:
        """Terminate an EC2 instance."""
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            logger.info("Terminated instance %s", instance_id)
        except Exception as e:
            logger.warning("Error terminating instance %s: %s", instance_id, e)

    def cleanup_cache(self) -> dict[str, int]:
        """Clean up old Docker layer cache entries."""
        if not self.cache_manager:
            logger.warning("S3 cache not enabled, skipping cleanup")
            return {}

        logger.info("Starting Docker layer cache cleanup")
        stats = self.cache_manager.cleanup_old_cache()
        logger.info("Cache cleanup completed: %s", stats)
        return stats

    def get_cache_stats(self) -> dict[str, Any]:
        """Get Docker layer cache statistics."""
        if not self.cache_manager:
            return {"enabled": False}

        stats = self.cache_manager.get_cache_stats()
        stats["enabled"] = True
        return stats
