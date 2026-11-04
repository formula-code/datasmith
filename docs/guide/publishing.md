# Publishing

fc-data publishes verified tasks to DockerHub (Docker images) and HuggingFace (Parquet datasets).

## Publishing workflow

```python
from datasmith.publish import records_from_supabase, HuggingFacePublisher

# Verified, unpublished performance PRs merged in the window.
records = records_from_supabase(start_date="2026-02-01", end_date="2026-03-01")

# Upload to HuggingFace as a versioned Parquet file.
hf = HuggingFacePublisher()
hf.publish(records, version="formulacode@2026-03")
```

`records_from_supabase` keeps a PR only when its best successful Harbor run reaches a 1.05x speedup in an admitted
environment and its container has `verification_state = 'verified'`. See
[Pipeline, stage 8](pipeline.md#stage-8-publish).

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

- Versions use the `@YYYY-MM` format (for example `formulacode@2026-03`).
- Each version is one file, `data/<version>.parquet`, in the `formulacode/formulacode` dataset repository.
- A second publish in the same month replaces that month's file. Files of other months do not change.

## DockerHub publishing

`DockerHubPublisher` provides:

- Login with `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` at the first push
- `push`, with retries and exponential backoff (`@with_backoff`)
- `tag_with_version`, which adds a `@YYYY-MM` suffix
- `list_remote_tags` and `filter_unpublished`, to push only tags that are not on DockerHub

## Pipeline integration

Publishing is stage 8 of the `fc-data` pipeline. `publish_pipeline()` does these steps:

1. Query the database for unpublished, verified PRs
2. Push their Docker images to DockerHub
3. Upload the Parquet file to HuggingFace
4. Set `published_at` on each published record
