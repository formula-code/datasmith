# Design: Layered Docker Build Overhaul

| Author(s) | Created | Status | Last Updated |
|-----------|---------|--------|--------------|
| Codex | 2026-03-01 | Proposal | 2026-03-01 |

---

## Context and Problem

The current build path uses one monolithic multi-stage Dockerfile and selects partial outputs with `--target` (`base -> repo -> env -> pkg -> run -> final`).

This causes repeated, avoidable work in the agent loop:

1. Every synthesis iteration can rebuild multiple unchanged stages.
2. Cache invalidation is coarse: upstream stage churn invalidates downstream work.
3. Layer artifacts are not independently addressable/publishable by responsibility.
4. Build contexts include scripts that are irrelevant to a specific iteration.

The desired end state is a SWE-bench-style layered model with explicit base/env/instance images and independent build lifecycles.

---

## Goals

1. Rebuild only what changed in agent iteration loops.
2. Make base and env images reusable across many attempts and workers.
3. Keep interfaces modular so build, validation, publishing, and cleanup can evolve independently.
4. Improve determinism, observability, and operational safety (including container spool-up/spool-down).
5. Preserve backward compatibility while migrating callers and persisted context objects.

## Non-Goals

1. Replacing Docker/BuildKit with another build substrate.
2. Rewriting verifier semantics from scratch.
3. Solving cross-cluster scheduling policy beyond the build/validate workflow.

---

## Proposed Architecture

### Layer Model

```text
┌─────────────────────────────────────────────────────┐
│ Base Image (dsm.base.{arch}.{hash}:v1)             │
│ Built rarely. Shared globally.                     │
│ Dockerfile.base + docker_build_base.sh             │
├─────────────────────────────────────────────────────┤
│ Env Image (dsm.env.{owner}-{repo}-{sha}:v1)        │
│ FROM base. Per commit.                             │
│ Dockerfile.env + docker_build_env.sh               │
├─────────────────────────────────────────────────────┤
│ Instance Image (dsm.inst.{owner}-{repo}-{sha}:v1)  │
│ FROM env. Rebuilt per agent attempt.               │
│ Dockerfile.instance + pkg/run/final/test/profile   │
└─────────────────────────────────────────────────────┘
```

### Key Shift

During iterative synthesis, only `instance` rebuilds. `base` and `env` are treated as mostly immutable cacheable dependencies.

---

## Build Contracts by Layer

### 1) `BaseLayerContext`

- Inputs: `Dockerfile.base`, `docker_build_base.sh`, architecture metadata.
- Image name: `dsm.base.{arch}.{content_hash}:v1`.
- API:
  - `build_if_missing(client) -> BuildResult`
  - `exists(client) -> bool`
- Cache key basis: hash of Dockerfile + base script + fixed toolchain args.

### 2) `EnvLayerContext`

- Inputs: `Dockerfile.env`, `docker_build_env.sh`, repo URL, commit SHA, env payload, Python version.
- Image name: `dsm.env.{owner}-{repo}-{sha}:v1`.
- API:
  - `build_if_missing(client, base_image_name: str) -> BuildResult`
- Cache key basis: `(owner, repo, sha, env script hash, build args subset)`.

### 3) `InstanceLayerContext`

- Inputs: `Dockerfile.instance`, `docker_build_pkg.sh`, `docker_build_run.sh`, `docker_build_final.sh`, `profile.sh`, `run-tests.sh`, `entrypoint.sh`.
- Image name: `dsm.inst.{owner}-{repo}-{sha}:v1` (or attempt-tagged during loop, then retagged on success).
- API:
  - `build(client, env_image_name: str) -> BuildResult`
- Cache key basis: script bundle hash + `BENCHMARKS` + env image digest.

### Compatibility Facade

Keep `DockerContext` as a facade during migration:

1. It decomposes to `BaseLayerContext`, `EnvLayerContext`, `InstanceLayerContext`.
2. `building_data` remains mapped to instance `docker_build_pkg.sh`.
3. Back-compat methods continue to exist but delegate to layered implementations.

---

## File/Module Plan

### New Files

1. `src/datasmith/docker/Dockerfile.base`
2. `src/datasmith/docker/Dockerfile.env`
3. `src/datasmith/docker/Dockerfile.instance`
4. `src/datasmith/docker/layers.py`

### Updated Files

1. `src/datasmith/docker/context.py`
2. `src/datasmith/core/models/task.py`
3. `src/datasmith/agents/build.py`
4. `src/datasmith/docker/validation.py`
5. `src/datasmith/docker/dockerhub.py`
6. `src/datasmith/docker/cleanup.py`
7. `src/datasmith/docker/aws_batch_executor.py` (remove stale `--target` comments/logic)
8. `src/datasmith/agents/context_synthesis.py` re-exports if names change

### Removed

1. `src/datasmith/docker/Dockerfile` (after cutover)
2. `process_image_name()` and all `--target` handling
3. Deprecated tags: `pkg`, `run`, `final` (after migration window)

---

## Build Engine and Cache Strategy

### Buildx Driver

Default to `buildx` and support selecting a named builder.

- Preferred driver: `docker-container` for advanced cache behavior and explicit BuildKit lifecycle.
- Keep SDK fallback path where `buildx` is unavailable.

### Cache Import/Export

Use explicit remote cache wiring for `base` and `env`:

- `--cache-from type=registry,ref=<cache-ref>`
- `--cache-to type=registry,ref=<cache-ref>,mode=max`

Notes:

1. `mode=max` improves cache hit rate for intermediate layers.
2. Cache refs must be separate from final image refs.
3. Enforce registry-compatible media-type settings when required by the target registry.

### Context Minimization

Each layer tarball should include only required files, with deterministic ordering and fixed metadata (mtime/uid/gid), reusing existing `add_bytes`/tar patterns.

### Optional Buildx Bake

Add an optional `docker-bake.hcl` for maintainability and concurrent prebuilds:

1. `base` target group
2. `env` target group (parameterized by repo/sha)
3. `instance` target (single-shot per attempt)

---

## Agent Loop Overhaul

### Current (simplified)

```text
probe env build (monolith --target env)
loop:
  build monolith --target pkg/run/final
  validate
```

### Target

```text
base_ctx.build_if_missing()
env_ctx.build_if_missing(base_image)
loop:
  instance_ctx.build(env_image)
  validate instance image
  if success: persist updated context
```

### `build_once_with_context()` simplification

For instance builds, pass only layer-relevant args (`FROM_IMAGE`, `BENCHMARKS`, possibly `PY_VERSION` if still needed). Repo checkout args move entirely to env layer.

---

## Task Model Changes

Update `Task.tag` domain to:

1. `base`
2. `env`
3. `instance`

Image naming:

1. `base`: `dsm.base.{arch}.{hash}:v1`
2. `env`: `dsm.env.{owner}-{repo}-{sha}:v1`
3. `instance`: `dsm.inst.{owner}-{repo}-{sha}:v1`

Migration approach:

1. Keep parsing support for legacy tags during transition.
2. Map legacy `final` reads to `instance` where safe.

---

## Validation and Publishing Updates

### Validation

`DockerValidator` should validate the `instance` image directly (profile + tests). No behavior change needed for run mechanics, only image/tag source updates.

### Publishing

DockerHub flows should publish `instance` image names.

- Replace `with_tag("final")` with `with_tag("instance")`.
- Keep push logic otherwise unchanged.

### ContextRegistry

Registry canonical key for synthesized scripts becomes `instance`.

- Store/lookup equivalent script payloads against `(owner, repo, sha, tag=instance)`.
- Similarity lookup remains unchanged in principle.

---

## Safe Spool-Up and Spool-Down (Container Lifecycle)

This section explicitly addresses safe startup/shutdown of build and validation containers.

### Lifecycle State Machine

```text
INIT
  -> PREPARE (labels, network, volumes, timeouts)
  -> STARTING (container create/start)
  -> HEALTHY (ready checks passed)
  -> RUNNING (build/verify task executing)
  -> DRAINING (no new work accepted)
  -> STOPPING (SIGTERM + grace timeout)
  -> TERMINATED (removed)
  -> CLEANED (network/volumes/prune complete)
```

### Spool-Up Rules

1. Create resources with run-scoped labels (`datasmith.run`, task identifiers, layer type, attempt id).
2. Use user-defined bridge networks by default; avoid host-network mode except explicit debug override.
3. Add host reachability via `--add-host host.docker.internal=host-gateway` when host access is required.
4. Enforce resource limits (`cpu`, `mem`, `pids`) and timeout budgets at container creation.
5. Perform readiness checks before scheduling real work.

### Spool-Down Rules

1. Mark worker draining before stop so orchestration stops dispatching new tasks.
2. Stop containers gracefully (`SIGTERM`, explicit stop timeout).
3. Force kill only after grace timeout.
4. Remove containers (`--rm`/explicit remove) and ephemeral networks/volumes by label.
5. Prune only run-scoped instance artifacts; do not prune shared base/env images.
6. Run periodic TTL garbage collection for orphaned resources (`until` + label filter).

### Failure Safety

1. Every run registers a best-effort finalizer in `try/finally`.
2. Orchestrator crash recovery scans for stale labeled resources and reclaims them.
3. Cleanup operations are idempotent and tolerate `NotFound` races.

---

## Concurrency and Parallelism

### Per-Image Build Locks

Use lock-by-image-name for `build_if_missing`:

1. Check image exists.
2. Acquire lock.
3. Re-check image exists.
4. Build once.

This prevents N workers from rebuilding identical base/env images concurrently.

### RayIO Orchestration

Parallelization model:

1. One worker actor per DockerContext candidate.
2. Shared semaphores/resources to cap concurrent env/base builds.
3. Backpressure with bounded pending tasks.
4. Retries on transient infra failures; bounded retries on deterministic script failures.

Suggested split:

1. `BaseBuildCoordinator` actor (global dedupe)
2. `EnvBuildCoordinator` actor (repo/sha dedupe)
3. `InstanceWorker` actors (high fan-out)

---

## Migration Phases

### Phase 1: Add Layered Path (Non-Breaking)

1. Add new Dockerfiles and `layers.py`.
2. Add dual-path plumbing behind feature flag (`DATASMITH_LAYERED_BUILD=1`).
3. Keep existing monolithic path as fallback.

### Phase 2: Move Agent/Validator/Publishers

1. Switch `agents/build.py` to layered flow.
2. Update validation/publish tag usage to `instance`.
3. Add lifecycle labeling and cleanup guarantees.

### Phase 3: Flip Default + Soak

1. Make layered path default.
2. Collect perf, cache-hit, and reliability metrics.
3. Resolve migration edge cases (legacy pickles/tasks).

### Phase 4: Remove Legacy Path

1. Delete monolithic Dockerfile and `--target` logic.
2. Remove feature flag and deprecated tags.
3. Finalize docs and tests.

---

## Backward Compatibility and Data Migration

1. Keep `DockerContext.__init__` tolerant of old fields.
2. Add `__setstate__` migration to map legacy serialized payloads into layered representation.
3. Support reading legacy tags during transition, but emit warnings.
4. Provide one-time migration script for existing context registries if needed.

---

## Verification Plan

1. Build base layer; verify expected toolchain presence.
2. Build env layer from base; verify repo clone/checkout and env setup.
3. Build instance layer from env; verify editable install and entrypoint wiring.
4. Run full agent loop; confirm only instance rebuilds between attempts.
5. Confirm cleanup never removes shared base/env images.
6. Validate DockerHub publishing with new names/tags.
7. Run full test suite and targeted regression tests for build/validation/publishing/cleanup.

---

## Operational Metrics

Track at minimum:

1. Build latency by layer (`base`, `env`, `instance`).
2. Cache hit ratio by layer.
3. Rebuild count per task attempt.
4. Cleanup success/failure counts.
5. Orphaned resource count (containers/networks/volumes/images) by label and age.
6. Agent attempts-to-success distribution before vs after migration.

---

## Risks and Mitigations

1. Legacy pickle incompatibility.
   - Mitigation: tolerant deserialization + migration tests.
2. Concurrent duplicate builds for shared layers.
   - Mitigation: keyed locks + double-checked existence.
3. Registry cache drift or corruption.
   - Mitigation: retry without cache on known cache errors; allow periodic cache refresh.
4. Resource leaks in high parallelism runs.
   - Mitigation: strict labels, finalizers, TTL GC, and startup reconciliation.
5. Hidden dependency on removed tags (`final`, `run`, `pkg`).
   - Mitigation: compatibility shim + repo-wide static checks before removal.

---

## Acceptance Criteria

1. Repeated agent attempts on same `(repo, sha)` rebuild only `instance`.
2. Base/env image reuse observed across tasks where applicable.
3. No regression in validation semantics (profile/tests).
4. Publish flows produce expected images under new naming.
5. Cleanup is safe: shared layers preserved, ephemeral resources reclaimed.
6. Parallel runs under RayIO complete without resource explosion or deadlocks.

---

## External References

1. Docker Build cache backends: https://docs.docker.com/build/cache/backends/
2. Docker registry cache backend: https://docs.docker.com/build/cache/backends/registry/
3. Docker build drivers: https://docs.docker.com/build/builders/drivers/
4. Docker `docker-container` driver: https://docs.docker.com/build/builders/drivers/docker-container/
5. Docker build context and `.dockerignore`: https://docs.docker.com/build/building/context/
6. Docker buildx CLI (`--cache-*`, `--metadata-file`, `--target`): https://docs.docker.com/engine/reference/commandline/build
7. Docker networking and user-defined bridge guidance: https://docs.docker.com/engine/network/ and https://docs.docker.com/engine/network/drivers/bridge/
8. Docker `host-gateway` usage (`--add-host`): https://docs.docker.com/reference/cli/dockerd/
9. Docker graceful stop semantics: https://docs.docker.com/reference/cli/docker/container/stop/
10. Docker pruning/filtering semantics: https://docs.docker.com/engine/manage-resources/pruning/ and https://docs.docker.com/reference/cli/docker/image/prune/
11. Docker Buildx Bake: https://docs.docker.com/build/bake/
12. Ray backpressure pattern (`ray.wait`): https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html
13. Ray resource-based concurrency limits: https://docs.ray.io/en/latest/ray-core/patterns/limit-running-tasks.html
14. Ray task fault tolerance: https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html
15. SWE-bench harness API/docs: https://www.swebench.com/SWE-bench/api/harness/
