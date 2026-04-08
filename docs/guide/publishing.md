# Publishing

fc-data publishes verified tasks to DockerHub (Docker images) and HuggingFace (Parquet datasets).

## Publishing workflow

```python
from datasmith.utils.db import get_client
from datasmith.publish import records_from_supabase, HuggingFacePublisher

# Query all verified, unpublished perf PRs from the last month
records = records_from_supabase(start_date="2026-02-01", end_date="2026-03-01")

# Publish to HuggingFace as a versioned Parquet dataset
hf = HuggingFacePublisher()
hf.publish(records, version="formulacode@2026-03")
```

## Querying the database directly

For more control, query Supabase directly:

```python
from datasmith.utils.db import get_client

sb = get_client()
rows = sb.table("pull_requests") \
    .select("*") \
    .eq("is_performance_commit", True) \
    .not_.is_("container_name", "null") \
    .execute()
```

## Dataset versioning

- Versions follow `@YYYY-MM` format (e.g., `formulacode@2026-03`)
- The dataset is updated monthly
- Each publish run creates a new version tag on HuggingFace
- Publishing is append-only — prior versions are never overwritten

## DockerHub publishing

`DockerHubPublisher` handles:

- Lazy authentication with DockerHub credentials
- Retry with exponential backoff via `@with_backoff`
- Version tagging with `@YYYY-MM` suffix
- Remote tag listing for delta publish (only push new/changed images)

## Pipeline integration

Publishing is the final stage of the `fc-data` pipeline (stage 7). The `publish_pipeline()` function orchestrates:

1. Query DB for unpublished, verified PRs
2. Push Docker images to DockerHub
3. Upload Parquet dataset to HuggingFace
4. Mark records with `published_at` timestamp
