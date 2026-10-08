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

## Build caches

`Dockerfile.pr` declares two BuildKit cache mounts, on both the `env` and the
`pkg` stage — the two steps that download and compile:

| Mount | Backs | Holds |
|-------|-------|-------|
| `/ccache` | `CCACHE_DIR` | C and Cython object files |
| `/opt/uvcache` | `UV_CACHE_DIR` | Downloaded and built wheels |

Three other caches were measured and deliberately left out. `/opt/conda/pkgs`:
only 16.9% of tasks run `micromamba install` at PR-build time, and a warm `pkgs`
dir did not speed that install (5.30s vs 5.80s, noise). `/root/.cache/conda`: 7.2s
off the solve, on those same 16.9% of tasks. `/opt/pipcache`: the only pip call is
an editable install of the local repo, which pip does not cache (4 KB - 7.6 MB per
image).

Measured on `pandas-dev/pandas#50501`, a `--target final` build with an
otherwise cold layer cache:

| Build | Wall | `env` step | `pkg` step | Image |
|-------|------|-----------|-----------|-------|
| before the mounts existed, cold | 832.3s | 64.2s | 624.1s | 11.15 GB |
| cold — mounts present but empty | 841.1s | 72.5s | 618.6s | 10.66 GB |
| **warm** — same task, layer cache busted | **268.5s** | 29.4s | 86.8s | 10.66 GB |
| a late-stage file changed (`docker_build_final.sh`) | 30.8s | cached | cached | 10.66 GB |
| **a neighbouring commit of the same repo** | **258.4s** | 30.3s | 75.9s | 10.66 GB |

A warm rebuild is 3.1x faster, and the same recipe at a neighbouring commit —
the case a producer/verifier split actually pays for — is 3.2x. Cold costs 8.8s
(+1.1%): the 3.4s ccache bootstrap plus ccache's store-on-miss overhead. The uv
mount takes 0.49 GB off the image, because a cache mount never enters one.

The `pkg` stage reports ccache's counters into the build log. Per-build deltas:

| Build | cacheable calls | hits |
|-------|----------------|------|
| cold | 117 | 15 (12.8%) |
| warm, same task | 68 | 68 (**100%**) |
| neighbouring commit, first time | 69 | 64 (92.8%) |
| neighbouring commit, cache holding both | 68 | 68 (**100%**) |

All 43 `.so` files rebuilt entirely from the cache are byte-for-byte identical to
the ones the pre-change build produced.

What the caches cannot touch is the ~133s floor: `docker_build_run.sh`'s git
repack (87s), the final stage (18s) and the image export (27s).

After five pandas builds the mounts hold 14 MB (`fc-ccache`) and 1.6 GB
(`fc-uv`) — read them with `docker buildx du --verbose`.

Compilation is where the time is: 54.6% of the fleet's 1.69M recorded
`build_duration_s` comes from the 54 of 134 repos whose PRs touch compiled
sources, and `pandas-dev/pandas` alone is 18.1% of it.

A cache mount lives in the BuildKit state directory, never in the image.

### How ccache is wired in

`ccache` is installed by `Dockerfile.pr` itself, into its own `/opt/ccache-env`
prefix, and interposed through masquerade symlinks in `/opt/ccache-bin`.

It has to live in the Dockerfile rather than in `docker_build_base.sh` or
`docker_build_env.sh`: `candidate_containers` stores a frozen per-task copy of
`build_env_sh`, so a template edit there never reaches the rows already
synthesized, and `formulacode/base:latest` is rebuilt too rarely to depend on.
`Dockerfile.pr` is backfilled from the templates directory on every build
(`runners/synthesize_images._fill_missing_scripts`, `agents/sandbox.py`), and
`local_ci.py` lists it in `_IMMUTABLE_FILES`, so it is the one surface that
reaches every task and that no synthesis agent can edit.

The bootstrap `RUN` sits **above** `ARG COMMIT_SHA` and the `git checkout`.
Nothing in it depends on the commit, so its layer cache key carries no per-task
input and BuildKit builds it once per repo image rather than once per task —
134 conda-forge solves instead of 1856. Below the checkout it would miss on
every task.

ccache setup is deliberately non-fatal: a build must not fail because
conda-forge was unreachable. The price of that tolerance is a build that
succeeds while compiling nothing from cache, so both the no-ccache branch and
the stats step print `FORMULACODE_CCACHE_UNAVAILABLE`. Grep a suspiciously slow
build for it.

Interposition is by `PATH`, not by `CC`. conda's `activate-gcc_linux-64.sh` sets
`CC` to the *bare* name `x86_64-conda-linux-gnu-cc`, resolved through `PATH`;
`CC="ccache gcc"` must not be used because distutils, meson and cmake each split
a multi-word `CC` differently. Across all 1856 stored `build_pkg_sh` scripts only
one sets `CC` to an unconditional absolute path, so `PATH` reaches effectively
the whole fleet.

`PATH` gets `/opt/ccache-bin` prepended twice, deliberately. The Dockerfile `ENV`
covers uv's isolated sdist build environments, which never activate a conda env.
`micromamba run -n <env>` prepends the environment's own `bin` *after* any
Dockerfile `ENV`, so for those calls the prepend has to run post-activation —
hence the `activate.d/zz-ccache.sh` hook the bootstrap writes into every `asv_*`
env, named `zz-` so it sorts after `activate-gcc_linux-64.sh`.

`CCACHE_COMPILERCHECK=content` rather than the default `mtime`: a
conda-installed compiler gets a fresh mtime in every image, which would miss
100% across images.

### What these caches do not do

- **They are host-local.** BuildKit cache mounts cannot be exported or pulled. A
  verifier rebuilding on a *different* host starts cold, and additionally pays a
  full base-image pull. This makes same-host rebuilds cheap; it does not make a
  cross-host rebuild cheap.
- **BuildKit garbage-collects them.** `docker buildx inspect default` shows GC
  rule#0 covering `type==exec.cachemount` with `Keep Duration: 48h` and
  `Max Used Space: 91.37GiB`. `fc-data` runs monthly, so a cache populated by one
  run is gone before the next. Lifting that needs a `builder.gc` policy in
  `/etc/docker/daemon.json` and a `sudo systemctl restart docker`, which
  interrupts running builds.
- **`--no-cache` empties them**, and the previously accumulated contents do not
  come back. No `docker.build()` call site passes it;
  `tests/docker/test_build_caches.py` asserts none starts.
- **`docker builder prune` deletes them.** `utils/docker_prune.py` is disk-aware
  and skips below `DATASMITH_DOCKER_PRUNE_MIN_USED_PCT` (default 85).

`tests/docker/test_build_caches.py` also asserts that each mount target still
equals the environment variable the tool reads. A mount whose target drifts from
`CCACHE_DIR`/`UV_CACHE_DIR` still builds successfully — it is just permanently
cold, with nothing to report it.

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
