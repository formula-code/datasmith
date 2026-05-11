from __future__ import annotations

from datetime import UTC, datetime

from datasmith.publish.huggingface import HuggingFacePublisher
from datasmith.publish.records import records_from_supabase
from datasmith.utils import get_client, get_logger

logger = get_logger("publish.pipeline")


async def publish_pipeline(
    start_date: str,
    end_date: str,
    dockerhub_push: bool = True,
    hf_publish: bool = True,
) -> int:
    """Full publishing pipeline: query DB -> push DockerHub -> upload HuggingFace -> mark published.

    Returns the number of records published.
    """
    records = records_from_supabase(start_date=start_date, end_date=end_date)
    if not records:
        logger.info("No unpublished records found for %s to %s", start_date, end_date)
        return 0

    logger.info("Found %d unpublished records", len(records))

    version = f"formulacode@{datetime.now(tz=UTC).strftime('%Y-%m')}"

    # DockerHub push (optional)
    if dockerhub_push:
        from datasmith.docker.publish import DockerHubPublisher

        publisher = DockerHubPublisher()
        for record in records:
            if record.container_name:
                try:
                    publisher.push(record.container_name)
                except Exception:
                    logger.warning("Failed to push %s", record.container_name)

    # HuggingFace publish (optional)
    if hf_publish:
        hf = HuggingFacePublisher()
        hf.publish(records, version)

    # Mark as published in Supabase
    client = get_client()
    now = datetime.now(tz=UTC).isoformat()
    for record in records:
        try:
            client.table("pull_requests").update({"published_at": now}).eq("owner", record.owner).eq(
                "repo", record.repo
            ).eq("issue_number", record.issue_number).execute()
        except Exception:
            logger.warning("Failed to mark %s/%s#%d as published", record.owner, record.repo, record.issue_number)

    logger.info("Published %d records as %s", len(records), version)
    return len(records)
