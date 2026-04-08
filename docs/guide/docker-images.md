# Docker Images

DataSmith uses a three-tier Docker image hierarchy to build reproducible environments for each pull request.

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

Verify images with a chain of verifiers that short-circuits on first failure:

```python
from datasmith.docker import MultiObjVerifier, SmokeVerifier, ProfileVerifier

verifier = MultiObjVerifier(verifiers=[
    SmokeVerifier("pandas"),      # can we import the package?
    ProfileVerifier(timeout=300), # can we discover and run ASV benchmarks?
])

result = verifier.verify("formulacode/pandas-dev-pandas:16222")
# result.ok, result.rc, result.stdout, result.stderr, result.duration_s
```

### Available verifiers

| Verifier | Purpose |
|----------|---------|
| `SmokeVerifier` | Runs `import {package}` inside the container |
| `ProfileVerifier` | Runs `profile.sh`, treats timeout (exit 124) as success |
| `PytestVerifier` | Runs `run-tests.sh` with pytest |
| `MultiObjVerifier` | Chains verifiers, short-circuits on failure |

## Implementation notes

- Docker operations use `python-on-whales` (not `docker-py`) — subprocess-based and thread-safe by design
- Scales to 40-50+ concurrent threads without connection pool issues
- Image tags are lowercased to comply with Docker registry requirements
