# %% [markdown]
# # Migrate Containers from ECR to DockerHub
# 
# This notebook:
# 1. Lists all containers on ECR
# 2. Filters ECR images by push date (only includes images pushed after a specified date)
# 3. Lists all containers on DockerHub
# 4. Finds containers that are on ECR but not on DockerHub
# 5. Pulls those containers from ECR
# 6. Pushes them to DockerHub

# %% [markdown]
# ## Setup and Imports

# %%
# %cd /mnt/sdd1/atharvas/formulacode/datasmith/
import os
import base64
from datetime import datetime, timezone
import docker
import boto3
from botocore.exceptions import ClientError
from datasmith.docker.dockerhub import _list_dockerhub_tags_single_repo, _get_dockerhub_credentials
from datasmith.docker.ecr import _list_ecr_tags_single_repo
from datasmith.logging_config import configure_logging

logger = configure_logging()

# Configuration
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
ECR_REPO = "formulacode/all"
DOCKERHUB_NAMESPACE = "formulacode"  # Change to your DockerHub namespace
DOCKERHUB_REPO = "all"

# Date filter - only migrate images pushed after this date
MIN_PUSHED_AT_UTC = datetime(2025, 11, 7, 1, 0, 0, tzinfo=timezone.utc)

# Get DockerHub credentials
DOCKERHUB_USERNAME = os.environ.get("DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = os.environ.get("DOCKERHUB_TOKEN") or os.environ.get("DOCKERHUB_PASSWORD")

# Get Docker client
docker_client = docker.from_env()

# Get AWS account ID for ECR
session = boto3.session.Session(region_name=AWS_REGION)
sts = session.client("sts")
account_id = sts.get_caller_identity()["Account"]
ECR_REGISTRY = f"{account_id}.dkr.ecr.{AWS_REGION}.amazonaws.com"

print(f"AWS Account ID: {account_id}")
print(f"ECR Registry: {ECR_REGISTRY}")
print(f"ECR Repository: {ECR_REPO}")
print(f"DockerHub Namespace: {DOCKERHUB_NAMESPACE}")
print(f"DockerHub Repository: {DOCKERHUB_REPO}")
print(f"Date Filter: Only images pushed after {MIN_PUSHED_AT_UTC}")

# %% [markdown]
# ## List All Containers on ECR

# %%
print("Listing all images on ECR...")
ecr_tags = _list_ecr_tags_single_repo(region=AWS_REGION, repo_name=ECR_REPO)

print(f"\nFound {len(ecr_tags)} images on ECR")
print(f"\nFirst 10 ECR images:")
for tag in sorted(ecr_tags)[:10]:
    print(f"  - {ECR_REGISTRY}/{ECR_REPO}:{tag}")

# %%
def filter_ecr_tags_by_push_date(
    tags: set[str],
    *,
    region: str,
    repo_name: str,
    cutoff: datetime,
) -> set[str]:
    """
    Filter ECR tags to only include those pushed after the cutoff date.
    
    Args:
        tags: Set of ECR image tags to filter
        region: AWS region
        repo_name: ECR repository name
        cutoff: Only include images pushed after this datetime (must be timezone-aware)
    
    Returns:
        Set of tags that were pushed after the cutoff date
    """
    if not tags:
        return set()
    
    session = boto3.session.Session(region_name=region)
    ecr_client = session.client("ecr")
    
    kept: set[str] = set()
    
    # Query ECR in chunks of 100 tags (API limit)
    CHUNK_SIZE = 100
    tag_list = sorted(tags)
    
    for i in range(0, len(tag_list), CHUNK_SIZE):
        chunk = tag_list[i : i + CHUNK_SIZE]
        image_ids = [{"imageTag": tag} for tag in chunk]
        
        try:
            resp = ecr_client.describe_images(
                repositoryName=repo_name,
                imageIds=image_ids
            )
        except ClientError as ce:
            code = ce.response.get("Error", {}).get("Code")
            if code in {"RepositoryNotFoundException", "ImageNotFoundException"}:
                # Skip this chunk if repo/images don't exist
                logger.warning(f"Repository or images not found for chunk {i}-{i+len(chunk)}")
                continue
            # Re-raise unexpected errors
            raise
        
        # Process image details
        for detail in resp.get("imageDetails", []):
            pushed_at = detail.get("imagePushedAt")  # boto3 returns timezone-aware datetime
            image_tags = detail.get("imageTags", [])
            
            if not pushed_at or not image_tags:
                continue
            
            # If this image was pushed after the cutoff, keep all its tags
            if pushed_at > cutoff:
                for tag in image_tags:
                    if tag in tags:  # Only add if it was in our original set
                        kept.add(tag)
    
    return kept


print(f"Filtering {len(ecr_tags)} ECR images by push date (after {MIN_PUSHED_AT_UTC})...")
filtered_ecr_tags = filter_ecr_tags_by_push_date(
    ecr_tags,
    region=AWS_REGION,
    repo_name=ECR_REPO,
    cutoff=MIN_PUSHED_AT_UTC
)

print(f"\nFiltered from {len(ecr_tags)} to {len(filtered_ecr_tags)} images")
print(f"Removed {len(ecr_tags) - len(filtered_ecr_tags)} images pushed before {MIN_PUSHED_AT_UTC}")
print(f"\nFirst 10 filtered ECR images:")
for tag in sorted(filtered_ecr_tags)[:10]:
    print(f"  - {ECR_REGISTRY}/{ECR_REPO}:{tag}")

# Update ecr_tags to use the filtered set
ecr_tags = filtered_ecr_tags

# %% [markdown]
# ## Filter ECR Images by Push Date
# 
# Filter ECR images to only include those pushed after the specified date.

# %% [markdown]
# ## List All Containers on DockerHub

# %%
if not DOCKERHUB_USERNAME or not DOCKERHUB_PASSWORD:
    raise ValueError(
        "DockerHub credentials required. Set DOCKERHUB_USERNAME and DOCKERHUB_TOKEN environment variables.\n"
        "Generate tokens at: https://hub.docker.com/settings/security"
    )

print("Listing all images on DockerHub...")
dockerhub_tags = _list_dockerhub_tags_single_repo(
    namespace=DOCKERHUB_NAMESPACE,
    repo_name=DOCKERHUB_REPO,
    username=DOCKERHUB_USERNAME,
    password=DOCKERHUB_PASSWORD
)

print(f"\nFound {len(dockerhub_tags)} images on DockerHub")
print(f"\nFirst 10 DockerHub images:")
for tag in sorted(dockerhub_tags)[:10]:
    print(f"  - docker.io/{DOCKERHUB_NAMESPACE}/{DOCKERHUB_REPO}:{tag}")

# %% [markdown]
# ## Find Containers on ECR but Not on DockerHub

# %%
# Find images that exist on ECR but not on DockerHub
missing_tags = ecr_tags - dockerhub_tags

print(f"\nFound {len(missing_tags)} images on ECR that are NOT on DockerHub")
print(f"\nFirst 20 missing images:")
for tag in sorted(missing_tags)[:20]:
    print(f"  - {tag}")

# Store sorted list for migration
tags_to_migrate = sorted(missing_tags)
print(f"\nTotal images to migrate: {len(tags_to_migrate)}")

# %% [markdown]
# ## Authenticate with ECR and DockerHub

# %%
def login_to_ecr(docker_client, session, region):
    """Login to ECR and return auth credentials. Can be called to refresh expired tokens."""
    ecr_client = session.client("ecr")
    auth_response = ecr_client.get_authorization_token()
    auth_data = auth_response["authorizationData"][0]
    ecr_username, ecr_password = base64.b64decode(auth_data["authorizationToken"]).decode().split(":", 1)
    ecr_endpoint = auth_data["proxyEndpoint"].replace("https://", "")

    docker_client.login(
        username=ecr_username,
        password=ecr_password,
        registry=ecr_endpoint
    )

    return {
        "username": ecr_username,
        "password": ecr_password,
        "endpoint": ecr_endpoint
    }

def looks_like_ecr_auth_error(error_msg):
    """Check if error message indicates ECR token expiration."""
    if not error_msg:
        return False
    lower = str(error_msg).lower()
    return any(
        token in lower
        for token in [
            "authorization token has expired",
            "no basic auth credentials",
            "authorization failed",
            "access denied",
            "requested access to the resource is denied",
            "pull access denied",
        ]
    )

# Login to ECR
print("Logging in to ECR...")
ecr_auth = login_to_ecr(docker_client, session, AWS_REGION)
print(f"✓ Logged in to ECR: {ecr_auth['endpoint']}")

# Login to DockerHub
print("\nLogging in to DockerHub...")
docker_client.login(
    username=DOCKERHUB_USERNAME,
    password=DOCKERHUB_PASSWORD,
    registry="docker.io"
)
print(f"✓ Logged in to DockerHub as {DOCKERHUB_USERNAME}")

# %% [markdown]
# ## Pull Containers from ECR
#
# This cell pulls all missing containers from ECR with automatic token refresh on expiration.
# This may take a long time depending on the number and size of images.

# %%
from tqdm.auto import tqdm

pulled_images = {}
pull_errors = {}

print(f"Pulling {len(tags_to_migrate)} images from ECR...\n")

for tag in tqdm(tags_to_migrate, desc="Pulling from ECR"):
    ecr_image_ref = f"{ECR_REGISTRY}/{ECR_REPO}:{tag}"

    # Try pulling with auth retry on token expiration
    max_retries = 2
    success = False

    for attempt in range(max_retries):
        try:
            if attempt == 0:
                print(f"Pulling {ecr_image_ref}...")
            else:
                print(f"  Retrying {ecr_image_ref} (attempt {attempt + 1}/{max_retries})...")

            # Pull with explicit auth config
            image = docker_client.images.pull(
                ecr_image_ref,
                auth_config={
                    "username": ecr_auth["username"],
                    "password": ecr_auth["password"]
                }
            )
            pulled_images[tag] = image
            print(f"  ✓ Pulled {ecr_image_ref}")
            success = True
            break

        except Exception as e:
            error_msg = str(e)

            # If it's an auth error and we haven't exhausted retries, refresh token and retry
            if looks_like_ecr_auth_error(error_msg) and attempt < max_retries - 1:
                print(f"  ⚠ ECR token expired for {ecr_image_ref}, refreshing and retrying...")
                try:
                    ecr_auth = login_to_ecr(docker_client, session, AWS_REGION)
                    print(f"  ✓ Refreshed ECR token")
                except Exception as auth_error:
                    pull_errors[tag] = f"Failed to refresh ECR token: {auth_error}"
                    print(f"  ✖ Failed to refresh ECR token: {auth_error}")
                    break
            else:
                # Non-auth error or exhausted retries
                if not success:
                    pull_errors[tag] = error_msg
                    print(f"  ✖ Failed to pull {ecr_image_ref}: {e}")
                break

print(f"\n✓ Successfully pulled {len(pulled_images)} images")
if pull_errors:
    print(f"✖ Failed to pull {len(pull_errors)} images")
    print("\nPull errors:")
    for tag, error in list(pull_errors.items())[:10]:
        print(f"  - {tag}: {error}")

# %% [markdown]
# ## Push Containers to DockerHub
#
# This cell pushes all pulled containers to DockerHub. This may take a long time.
#
# **Note:** Rate limiting may occur with DockerHub. The cell includes retry logic.

# %%
import time
from tqdm.auto import tqdm

pushed_images = {}
push_errors = {}

print(f"Pushing {len(pulled_images)} images to DockerHub...\n")

for tag, image in tqdm(pulled_images.items(), desc="Pushing to DockerHub"):
    ecr_image_ref = f"{ECR_REGISTRY}/{ECR_REPO}:{tag}"
    dockerhub_image_ref = f"{DOCKERHUB_NAMESPACE}/{DOCKERHUB_REPO}:{tag}"

    try:
        # Tag the image for DockerHub
        image.tag(f"{DOCKERHUB_NAMESPACE}/{DOCKERHUB_REPO}", tag=tag)

        # Push to DockerHub with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Pushing {dockerhub_image_ref}... (attempt {attempt + 1}/{max_retries})")

                # Push and wait for completion
                push_result = docker_client.images.push(
                    f"{DOCKERHUB_NAMESPACE}/{DOCKERHUB_REPO}",
                    tag=tag,
                    stream=True,
                    decode=True
                )

                # Check for errors in stream
                success = False
                for line in push_result:
                    if "error" in line:
                        raise RuntimeError(line.get("error", "Unknown error"))
                    if "aux" in line and "Digest" in line.get("aux", {}):
                        success = True
                        digest = line["aux"]["Digest"]
                        print(f"  ✓ Pushed {dockerhub_image_ref} ({digest})")
                        pushed_images[tag] = dockerhub_image_ref
                        break

                if success:
                    break

            except Exception as e:
                error_msg = str(e).lower()
                if "rate limit" in error_msg or "429" in error_msg or "too many requests" in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = 60 * (2 ** attempt)
                        print(f"  ⚠ Rate limit hit, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                raise

    except Exception as e:
        push_errors[tag] = str(e)
        print(f"  ✖ Failed to push {dockerhub_image_ref}: {e}")

print(f"\n✓ Successfully pushed {len(pushed_images)} images")
if push_errors:
    print(f"✖ Failed to push {len(push_errors)} images")
    print("\nPush errors:")
    for tag, error in list(push_errors.items())[:10]:
        print(f"  - {tag}: {error}")

# %% [markdown]
# ## Summary

# %%
print("=" * 80)
print("MIGRATION SUMMARY")
print("=" * 80)
print(f"Date Filter:                        Images pushed after {MIN_PUSHED_AT_UTC}")
print(f"Total images on ECR (filtered):     {len(ecr_tags)}")
print(f"Total images on DockerHub (before): {len(dockerhub_tags)}")
print(f"Images needing migration:           {len(tags_to_migrate)}")
print(f"Images successfully pulled:         {len(pulled_images)}")
print(f"Images successfully pushed:         {len(pushed_images)}")
print(f"Images failed to pull:              {len(pull_errors)}")
print(f"Images failed to push:              {len(push_errors)}")
print("=" * 80)

if pushed_images:
    print("\nSuccessfully migrated images:")
    for tag in sorted(pushed_images.keys())[:20]:
        print(f"  ✓ {tag}")
    if len(pushed_images) > 20:
        print(f"  ... and {len(pushed_images) - 20} more")

if pull_errors or push_errors:
    print("\n⚠ Some images failed to migrate. Review the errors above.")

# %% [markdown]
# ## Optional: Clean Up Local Images
#
# Uncomment and run this cell to remove the pulled images from local Docker storage to free up space.

# %%
# print("Cleaning up local images...")
# for tag, image in pulled_images.items():
#     try:
#         docker_client.images.remove(image.id, force=True)
#         print(f"  Removed {tag}")
#     except Exception as e:
#         print(f"  Failed to remove {tag}: {e}")
# print("✓ Cleanup complete")


