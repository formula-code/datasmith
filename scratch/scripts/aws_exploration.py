import logging
import os
import random
from collections import defaultdict
from pathlib import Path

import asv

from datasmith.docker.aws_batch_executor import AwsBatchConfig, AWSBatchExecutor
from datasmith.docker.context import ContextRegistry, DockerContext, Task
from datasmith.docker.orchestrator import _compute_deterministic_run_id
from datasmith.logging_config import configure_logging
from datasmith.notebooks.utils import update_cr

configure_logging(level=logging.INFO)

cr = ContextRegistry.load_from_file(Path("scratch/artifacts/pipeflush/context_registry_filtered_perfonly.json"))

test_tasks: list[tuple[Task, DockerContext]] = list(update_cr(cr).registry.items())
all_owner_repos = defaultdict(list)
for t, c in test_tasks:
    if "default" in t.repo:
        continue
    all_owner_repos[f"{t.owner}/{t.repo}"].append((t, c))

sampled_tasks = [random.sample(v, 2) if len(v) > 1 else v for v in all_owner_repos.values()]
test_tasks = [(t.with_tag("run"), c) for v in sampled_tasks for (t, c) in v]

print(f"Created {len(test_tasks)} test tasks:")
for task, _ in test_tasks:
    print(task.get_image_name())

# ASV configuration
asv_args = "--append-samples -a rounds=2 -a repeat=2 --python=same"
machine_args: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
machine_args["num_cpu"] = "4"

print(f"\nASV args: {asv_args}")
print(f"Machine args: {machine_args}")


def test_aws_config_validation():
    """Test AWS configuration validation."""
    print("🧪 Testing AWS configuration validation...")
    print("\n2. Testing with complete config (should reach AWS API)...")
    aws_batch_config = {
        "region": os.environ["AWS_REGION"],
        "s3_bucket": os.environ["AWS_S3_BUCKET"],
        "subnet_id": os.environ["AWS_SUBNET_ID"],
        "security_group_ids": [os.environ["AWS_SECURITY_GROUP_ID"]],
        "iam_instance_profile_name": os.environ["AWS_IAM_INSTANCE_PROFILE_NAME"],
        "ami_id": os.environ["AWS_AMI_ID"],
        "instance_type": os.environ["AWS_INSTANCE_TYPE"],
        "max_tasks_per_instance": 20,
        "batch_timeout_s": 5 * 60 * 60,
        "poll_interval_s": 120,
        "max_batch_retries": 1,
        "spot_max_price": "0.20",
        "stream_logs": True,
        "log_output_dir": "aws_logs/remote_logs",
    }
    # AWS batch execution
    if not aws_batch_config:
        raise ValueError("aws_batch_config is required when use_aws_batch=True")

    contexts = test_tasks
    n_cores = 4
    # Create AWS batch config
    aws_cfg = AwsBatchConfig(
        region=aws_batch_config["region"],
        s3_bucket=aws_batch_config["s3_bucket"],
        s3_prefix=aws_batch_config.get("s3_prefix", "datasmith-batch-execution"),
        subnet_id=aws_batch_config["subnet_id"],
        security_group_ids=aws_batch_config["security_group_ids"],
        iam_instance_profile_name=aws_batch_config["iam_instance_profile_name"],
        ami_id=aws_batch_config["ami_id"],
        instance_type=aws_batch_config.get("instance_type", "c6i.xlarge"),
        key_name=aws_batch_config.get("key_name"),
        spot_max_price=aws_batch_config.get("spot_max_price"),
        tags=aws_batch_config.get("tags", {}),
        stream_logs=aws_batch_config.get("stream_logs", True),
        log_output_dir=aws_batch_config.get("log_output_dir", "output/batch_logs"),
        max_tasks_per_instance=aws_batch_config.get("max_tasks_per_instance", 100),
        batch_timeout_s=aws_batch_config.get("batch_timeout_s", 2 * 60 * 60),
        poll_interval_s=aws_batch_config.get("poll_interval_s", 30),
        max_batch_retries=aws_batch_config.get("max_batch_retries", 1),
        num_cores_per_task=n_cores,
        asv_args=asv_args,
    )

    # Create batch executor
    batch_executor = AWSBatchExecutor(aws_cfg)

    # Execute batch
    run_id = os.environ.get("DATASMITH_RUN_ID") or _compute_deterministic_run_id(
        contexts, asv_args=asv_args, machine_args=machine_args, n_cores=n_cores
    )
    batch_results = batch_executor.execute_batch(
        tasks=contexts,
        machine_args=machine_args,
        asv_args=asv_args,
        run_id=run_id,
    )
    return batch_results
    # try:
    #     results = await batch_orchestrate(
    #         contexts=test_tasks,
    #         asv_args=asv_args,
    #         machine_args=machine_args,
    #         max_concurrency=2,
    #         n_cores=4,
    #         output_dir=Path("aws_logs/"),
    #         client=None,
    #         use_aws_batch=True,
    #         aws_batch_config=aws_config,
    #     )
    #     return results
    # except Exception as e:
    #     error_msg = str(e).lower()
    #     if any(keyword in error_msg for keyword in ["aws", "s3", "ec2", "access", "denied"]):
    #         print("   ✅ Correctly reached AWS API (failed as expected without real setup)")
    #         print(f"   Error: {type(e).__name__}")
    #     else:
    #         print(f"   ❌ Unexpected error: {e}")
    #     return None


output = test_aws_config_validation()
# import IPython; IPython.embed()
