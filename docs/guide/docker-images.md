# Docker Images

fc-data uses a three-tier Docker image hierarchy to build reproducible environments for each pull request.

## Image hierarchy

1. **Base image** (`formulacode/base:latest`) — Common dependencies and tooling
2. **Repo image** (`formulacode/{owner}-{repo}:latest`) — Repository-specific setup
3. **PR image** (`formulacode/{owner}-{repo}:{issue_number}`) — PR-specific build scripts and verification

## Building images

```python
from datasmith.docker import ImageManager

mgr = ImageManager()

# Build each tier
mgr.build_base_image()                               # formulacode/base:latest
mgr.build_repo_image("pandas-dev", "pandas")          # formulacode/pandas-dev-pandas:latest
mgr.build_pr_image("pandas-dev", "pandas", 16222)     # formulacode/pandas-dev-pandas:16222

# Use custom build contexts
mgr.build_base_image(context="path/to/custom/context")
mgr.build_repo_image("pandas-dev", "pandas", context="path/to/custom/context")
mgr.build_pr_image("pandas-dev", "pandas", 16222, context="path/to/custom/context")
```

## Build contexts

A `DockerContext` is a Pydantic model holding a Dockerfile and shell scripts. It can be loaded from a task directory or constructed programmatically.

```python
from datasmith.docker.context import DockerContext

# Load from a task directory
ctx = DockerContext.from_directory("dataset/formulacode_verified/pandas-dev_pandas/abc123")

# Produce a reproducible tarball (zero mtimes, deterministic order)
tar_bytes = ctx.to_tar_bytes()
```

## Verification

Verification is performed by `local_ci.py` during stage-6 synthesis, which
builds the image, runs the test suite, runs the measure step, and gates on the
build manifest's FATAL invariants. See the
[Verification guide](verification.md) for the full pipeline.

To inspect an already-built image, read its manifest:

```python
from datasmith.docker import read_build_manifest, evaluate_invariants

manifest = read_build_manifest("formulacode/pandas-dev-pandas:16222")
report = evaluate_invariants(manifest)

report.ok        # True (all fatals held) / False (a fatal failed) / None (no manifest)
report.fatal     # ["asv_exec_failed", "oracle_patch_failed"]
report.warnings  # ["speedup_direction", "discovery_fallback_used"]
report.skipped   # invariants whose inputs were absent
```

`ok` is deliberately three-valued. Every image built before build manifests
existed has none, and for those `read_build_manifest` returns `None` and
`evaluate_invariants(None)` reports `ok=None` with every invariant skipped —
rather than raising, or worse, reporting a clean pass it never checked.

This reads facts the build already recorded, so it is fast, offline, and works
against any published image without rebuilding it.

### Invariant severity

| Severity | Effect |
|----------|--------|
| `fatal` | Fails the verification step; recorded in `report.fatal` |
| `warn` | Recorded in `candidate_containers.manifest_warnings`, non-blocking |
| *(skipped)* | The invariant's inputs were absent, so it was not evaluated |

## Implementation notes

- Docker operations use `python-on-whales` (not `docker-py`) — subprocess-based and thread-safe by design
- Scales to 40-50+ concurrent threads without connection pool issues
- Image tags are lowercased to comply with Docker registry requirements
