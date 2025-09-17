from __future__ import annotations

import base64
import dataclasses
import json
import random
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import boto3

from datasmith.docker.context import BuildResult, DockerContext, Task
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

    # Batch tuning
    max_tasks_per_instance: int = 100  # Number of tasks (build + benchmark) per instance
    batch_timeout_s: int = 2 * 60 * 60  # 2 hours max time per batch
    poll_interval_s: int = 30  # Poll every 30 seconds
    max_batch_retries: int = 1  # relaunch if batch times out

    # Benchmark specific
    num_cores_per_task: int = 4  # CPU cores per benchmark task
    asv_args: str = "--append-samples -a rounds=2 -a repeat=2 --python=same"


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

        logger.info(f"Starting batch execution for {len(tasks)} tasks with run_id={run_id}")

        # Prepare batch tasks
        batch_tasks = []
        for i, (task, context) in enumerate(tasks):
            batch_task = BatchTask(
                task=task,
                context=context,
                machine_args=machine_args,
                asv_args=asv_args,
                num_cores=self.cfg.num_cores_per_task,
                task_id=f"{run_id}-task-{i:04d}",
            )
            batch_tasks.append(batch_task)

        # Split into batches of max_tasks_per_instance
        batches = []
        for i in range(0, len(batch_tasks), self.cfg.max_tasks_per_instance):
            batch = batch_tasks[i : i + self.cfg.max_tasks_per_instance]
            batches.append(batch)

        logger.info(f"Split {len(tasks)} tasks into {len(batches)} batches")

        # Execute batches in parallel using ThreadPoolExecutor
        all_results = []
        with ThreadPoolExecutor(max_workers=min(len(batches), 10)) as executor:
            # Submit all batches for parallel execution
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

        logger.info(f"Completed batch execution: {len(all_results)} results")
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
            # Wait for results
            results = self._wait_for_batch_results(batch, run_id, batch_idx)
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

        logger.info(f"Uploaded batch data to s3://{self.cfg.s3_bucket}/{batch_data_key}")
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
        instance_id = resp["Instances"][0]["InstanceId"]

        logger.info("Launched instance %s for batch %d", instance_id, batch_idx)
        return instance_id  # type: ignore[no-any-return]

    def _generate_user_data(self, batch_data_key: str) -> str:
        """Generate user data script for EC2 instance (Amazon Linux 2 compatible)."""
        return f"""#!/bin/bash
set -eox pipefail

# Log user-data to a file for debugging
exec > >(tee -a /var/log/user-data.log) 2>&1

retry() {{
  local n=0
  local max=5
  local delay=5
  while true; do
    "$@" && break || {{
      n=$((n+1))
      if [ $n -ge $max ]; then
        echo "Command failed after $n attempts: $*"
        return 1
      fi
      echo "Retry $n/$max for: $*"; sleep $delay;
    }}
  done
}}

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
retry aws s3 cp "s3://{self.cfg.s3_bucket}/{batch_data_key}" batch-data.json

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
  chmod +x entrypoint.sh docker_build_*.sh profile.sh || true

        echo "==> Building Docker image for $OWNER/$REPO@$SHA"
        REPO_URL="https://github.com/$OWNER/$REPO.git"
        IMAGE_NAME="$OWNER-$REPO-$SHA:$TAG"

        # Capture build timing and logs
        BUILD_START=$(date +%s.%N)
        BUILD_LOG_FILE="build.log"
        if timeout "$BATCH_TIMEOUT" docker build -t "$IMAGE_NAME" . \
            --build-arg REPO_URL="$REPO_URL" \
            --build-arg COMMIT_SHA="$SHA" \
            --target "$TAG" > "$BUILD_LOG_FILE" 2>&1; then
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

          # Create failure result with proper JSON formatting
          BUILD_LOG_CONTENT=$(base64 < "$BUILD_LOG_FILE" | tr -d '\n' | sed 's/"/\\"/g')
          ESCAPED_STDERR_TAIL=$(echo "$BUILD_STDERR_TAIL" | sed 's/"/\\"/g')
          ESCAPED_STDOUT_TAIL=$(echo "$BUILD_STDOUT_TAIL" | sed 's/"/\\"/g')

          # Create failure result JSON using a template file approach
          cat > failure_template.json << 'EOF'
{{
  "task_id": "TASK_ID_PLACEHOLDER",
  "build_result": {{
    "ok": false,
    "image_name": "",
    "image_id": null,
    "rc": BUILD_RC_PLACEHOLDER,
    "duration_s": BUILD_DURATION_PLACEHOLDER,
    "stderr_tail": "STDERR_TAIL_PLACEHOLDER",
    "stdout_tail": "STDOUT_TAIL_PLACEHOLDER"
  }},
  "benchmark_exit_code": 1,
  "benchmark_files": {{"build.log": "BUILD_LOG_CONTENT_PLACEHOLDER"}},
  "benchmark_logs": "Build failed with exit code BUILD_RC_PLACEHOLDER",
  "duration_s": BUILD_DURATION_PLACEHOLDER
}}
EOF

          # Replace placeholders with actual values
          sed -i "s/TASK_ID_PLACEHOLDER/$TASK_ID/g" failure_template.json
          sed -i "s/BUILD_RC_PLACEHOLDER/$BUILD_RC/g" failure_template.json
          sed -i "s/BUILD_DURATION_PLACEHOLDER/$BUILD_DURATION/g" failure_template.json
          sed -i "s/STDERR_TAIL_PLACEHOLDER/$ESCAPED_STDERR_TAIL/g" failure_template.json
          sed -i "s/STDOUT_TAIL_PLACEHOLDER/$ESCAPED_STDOUT_TAIL/g" failure_template.json
          sed -i "s/BUILD_LOG_CONTENT_PLACEHOLDER/$BUILD_LOG_CONTENT/g" failure_template.json

          mv failure_template.json result.json
          retry aws s3 cp result.json "s3://{self.cfg.s3_bucket}/{self.cfg.s3_prefix}/results/$RUN_ID/batch-$BATCH_IDX/$TASK_ID/result.json" || true
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
        BENCHMARK_LOG_CONTENT=$(base64 < benchmark.log | tr -d '\n')

        # Save container logs alongside outputs
        docker logs "$CONTAINER_NAME" > output/container.log 2>&1 || true

        # Clean up the container after logs are saved
        docker rm "$CONTAINER_NAME" > /dev/null 2>&1 || true
        echo "==> Collecting results for $TASK_ID (exit=$BENCHMARK_EXIT_CODE)"

        # Build JSON files object properly
        BENCHMARK_FILES_JSON="{{}}"

        # Add build log to files
        if [ -f "$BUILD_LOG_FILE" ]; then
          BUILD_LOG_CONTENT=$(base64 < "$BUILD_LOG_FILE" | tr -d '\n' | sed 's/"/\\"/g')
          BENCHMARK_FILES_JSON=$(echo "$BENCHMARK_FILES_JSON" | jq --arg content "$BUILD_LOG_CONTENT" '. + {{"build.log": $content}}')
        fi

        # Add benchmark log to files
        if [ -f "benchmark.log" ]; then
          BENCHMARK_LOG_FILE_CONTENT=$(base64 < benchmark.log | tr -d '\n' | sed 's/"/\\"/g')
          BENCHMARK_FILES_JSON=$(echo "$BENCHMARK_FILES_JSON" | jq --arg content "$BENCHMARK_LOG_FILE_CONTENT" '. + {{"benchmark.log": $content}}')
        fi

        # Add output directory files
        if [ -d "output" ]; then
          for file in output/*; do
            if [ -f "$file" ]; then
              filename=$(basename "$file")
              content=$(base64 < "$file" | tr -d '\n' | sed 's/"/\\"/g')
              BENCHMARK_FILES_JSON=$(echo "$BENCHMARK_FILES_JSON" | jq --arg filename "$filename" --arg content "$content" '. + {{($filename): $content}}')
            fi
          done
        fi

        # Calculate total duration
        TOTAL_DURATION=$(echo "$BUILD_DURATION + $BENCHMARK_DURATION" | bc -l)

        # Escape variables for JSON
        ESCAPED_IMAGE_NAME=$(echo "$IMAGE_NAME" | sed 's/"/\\"/g')
        ESCAPED_STDERR_TAIL=$(echo "$BUILD_STDERR_TAIL" | sed 's/"/\\"/g')
        ESCAPED_STDOUT_TAIL=$(echo "$BUILD_STDOUT_TAIL" | sed 's/"/\\"/g')
        ESCAPED_BENCHMARK_LOGS=$(echo "$BENCHMARK_LOG_CONTENT" | sed 's/"/\\"/g')

        # Create success result JSON using a template file approach
        cat > success_template.json << 'EOF'
{{
  "task_id": "TASK_ID_PLACEHOLDER",
  "build_result": {{
    "ok": true,
    "image_name": "IMAGE_NAME_PLACEHOLDER",
    "image_id": null,
    "rc": BUILD_RC_PLACEHOLDER,
    "duration_s": BUILD_DURATION_PLACEHOLDER,
    "stderr_tail": "STDERR_TAIL_PLACEHOLDER",
    "stdout_tail": "STDOUT_TAIL_PLACEHOLDER"
  }},
  "benchmark_exit_code": BENCHMARK_EXIT_CODE_PLACEHOLDER,
  "benchmark_files": BENCHMARK_FILES_JSON_PLACEHOLDER,
  "benchmark_logs": "BENCHMARK_LOGS_PLACEHOLDER",
  "duration_s": TOTAL_DURATION_PLACEHOLDER
}}
EOF

        # Replace placeholders with actual values
        sed -i "s/TASK_ID_PLACEHOLDER/$TASK_ID/g" success_template.json
        sed -i "s/IMAGE_NAME_PLACEHOLDER/$ESCAPED_IMAGE_NAME/g" success_template.json
        sed -i "s/BUILD_RC_PLACEHOLDER/$BUILD_RC/g" success_template.json
        sed -i "s/BUILD_DURATION_PLACEHOLDER/$BUILD_DURATION/g" success_template.json
        sed -i "s/STDERR_TAIL_PLACEHOLDER/$ESCAPED_STDERR_TAIL/g" success_template.json
        sed -i "s/STDOUT_TAIL_PLACEHOLDER/$ESCAPED_STDOUT_TAIL/g" success_template.json
        sed -i "s/BENCHMARK_EXIT_CODE_PLACEHOLDER/$BENCHMARK_EXIT_CODE/g" success_template.json
        sed -i "s/BENCHMARK_FILES_JSON_PLACEHOLDER/$BENCHMARK_FILES_JSON/g" success_template.json
        sed -i "s/BENCHMARK_LOGS_PLACEHOLDER/$ESCAPED_BENCHMARK_LOGS/g" success_template.json
        sed -i "s/TOTAL_DURATION_PLACEHOLDER/$TOTAL_DURATION/g" success_template.json

        mv success_template.json result.json

  retry aws s3 cp result.json "s3://{self.cfg.s3_bucket}/{self.cfg.s3_prefix}/results/$RUN_ID/batch-$BATCH_IDX/$TASK_ID/result.json" || true

  docker rmi "$IMAGE_NAME" || true
  cd ..
done

echo "Batch execution completed for run_id=$RUN_ID, batch_idx=$BATCH_IDX"
# Allow S3 eventual consistency to settle
sleep 10
shutdown -h now || poweroff || halt
"""  # noqa: S608

    def _wait_for_batch_results(
        self,
        batch: Sequence[BatchTask],
        run_id: str,
        batch_idx: int,
    ) -> list[BatchResult]:
        """Wait for batch results to be uploaded to S3."""

        deadline = time.time() + self.cfg.batch_timeout_s
        results: dict[str, BatchResult] = {}

        logger.info("Waiting for %d results from batch %d", len(batch), batch_idx)

        while time.time() < deadline and len(results) < len(batch):
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

                except self.s3.exceptions.NoSuchKey:
                    continue
                except Exception as e:
                    logger.warning("Error fetching result for %s: %s", batch_task.task_id, e)
                    continue

            if len(results) < len(batch):
                time.sleep(self.cfg.poll_interval_s)

        if len(results) < len(batch):
            logger.warning("Timeout waiting for batch results: got %d/%d", len(results), len(batch))

        return list(results.values())

    def _terminate_instance(self, instance_id: str) -> None:
        """Terminate an EC2 instance."""
        try:
            self.ec2.terminate_instances(InstanceIds=[instance_id])
            logger.info("Terminated instance %s", instance_id)
        except Exception as e:
            logger.warning("Error terminating instance %s: %s", instance_id, e)
