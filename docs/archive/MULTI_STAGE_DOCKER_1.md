# Variant 1: Single Dockerfile, Multiple Targets

## Intent
Keep one canonical Dockerfile and use named targets to support local debugging, CI validation, and production benchmark runs.

This is the closest evolution of the current layout in `src/datasmith/docker/Dockerfile`.

## Proposed Repository Shape

```text
docs/
  MULTI_STAGE_DOCKER_1.md
src/datasmith/docker/
  Dockerfile
  docker_build_base.sh
  docker_build_env.sh
  docker_build_pkg.sh
  docker_build_run.sh
  docker_build_final.sh
  context.py
```

## Stage Topology

```mermaid
flowchart LR
  base[base: toolchain + micromamba + uv]
  repo[repo: clone target repository]
  env[env: checkout sha + create benchmark env]
  pkg[pkg: install package + smoke checks]
  run[run: runtime setup + entrypoint wiring]
  final[final: benchmark selection + ready image]

  base --> repo --> env --> pkg --> run --> final
```

## How Builds Are Used

- Local debug image: `docker build --target env ...`
- Package validation image: `docker build --target pkg ...`
- Benchmark execution image: `docker build --target final ...`

## Why This Variant

- Lowest migration risk because it aligns with current code and `DockerContext`.
- Easy to reason about stage boundaries for failures.
- Good for incremental adoption of `buildx --target` and inline cache.

## Trade-offs

- One Dockerfile can become large and harder to maintain.
- Changes to shared sections can invalidate many downstream layers.
- Less explicit ownership than split Dockerfiles.

