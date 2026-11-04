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

`Dockerfile.pr` declares two BuildKit cache mounts on the `env` and `pkg` stages, the two steps that download and
compile:

| Mount | Backs | Holds |
|-------|-------|-------|
| `/ccache` | `CCACHE_DIR` | C and Cython object files |
| `/opt/uvcache` | `UV_CACHE_DIR` | Downloaded and built wheels |

There are no cache mounts for `/opt/conda/pkgs`, `/root/.cache/conda` or `/opt/pipcache`. Few tasks run
`micromamba install` at PR build time, and the only pip call is an editable install of the local repository, which pip
does not cache.

A warm cache makes a rebuild of the same task, or a build of a nearby commit of the same repository, about three times
faster. A cold build is about 1% slower. The `pkg` stage prints ccache's counters in the build log, and
`docker buildx du --verbose` shows the size of the `fc-ccache` and `fc-uv` mounts. A cache mount lives in the BuildKit
state directory, never in the image.

### How ccache is installed

`Dockerfile.pr` installs `ccache` into its own `/opt/ccache-env` prefix and puts compiler symlinks in
`/opt/ccache-bin`.

- It is in the Dockerfile, not in `docker_build_base.sh` or `docker_build_env.sh`, because `candidate_containers`
  stores a copy of `build_env_sh` per task, so a template edit there does not reach tasks already synthesized.
  `Dockerfile.pr` is copied from the templates directory on every build
  (`runners/synthesize_images._fill_missing_scripts`, `agents/sandbox.py`), and `local_ci.py` lists it in
  `_IMMUTABLE_FILES`, so every task gets it and no synthesis agent can edit it.
- The setup `RUN` comes before `ARG COMMIT_SHA` and the `git checkout`. It does not depend on the commit, so BuildKit
  builds it once per repo image, not once per task.
- A failed ccache setup does not fail the build, because conda-forge can be unreachable. The build then compiles
  without the cache and prints `FORMULACODE_CCACHE_UNAVAILABLE`. Search a slow build log for it.
- The compiler is replaced through `PATH`, not `CC`. conda's `activate-gcc_linux-64.sh` sets `CC` to the bare name
  `x86_64-conda-linux-gnu-cc`, which `PATH` resolves. Do not use `CC="ccache gcc"`: distutils, meson and cmake each
  split a multi-word `CC` differently.
- `/opt/ccache-bin` is put at the front of `PATH` twice. The Dockerfile `ENV` covers uv's isolated sdist build
  environments, which do not activate a conda env. `micromamba run -n <env>` puts the environment's `bin` first after
  activation, so the setup also writes `activate.d/zz-ccache.sh` into every `asv_*` env. The `zz-` prefix sorts it
  after `activate-gcc_linux-64.sh`.
- `CCACHE_COMPILERCHECK=content`, not the default `mtime`, because a conda-installed compiler has a new mtime in every
  image.

### Limits of these caches

- **They are local to the host.** BuildKit cache mounts cannot be exported or pulled. A rebuild on a different host
  starts cold and also pulls the full base image.
- **BuildKit garbage-collects them.** `docker buildx inspect default` shows the GC rule and keep duration for
  `type==exec.cachemount`, which is shorter than a month. `fc-data` runs monthly, so the cache of one run is gone before the next. A longer time needs
  a `builder.gc` policy in `/etc/docker/daemon.json` and a Docker restart, which stops running builds.
- **`--no-cache` empties them.** No `docker.build()` call passes it; `tests/docker/test_build_caches.py` checks this.
- **`docker builder prune` deletes them.** `utils/docker_prune.py` prunes only when disk use is at or above
  `DATASMITH_DOCKER_PRUNE_MIN_USED_PCT` (default 85).

`tests/docker/test_build_caches.py` also checks that each mount target equals the environment variable that the tool
reads. A mount target that differs from `CCACHE_DIR` or `UV_CACHE_DIR` still builds, but the cache is always cold.

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

`local_ci.py` verifies each image during stage 6. It builds the image, runs the
test suite, runs the measure step and checks the fatal invariants of the build
manifest. See the [Verification guide](verification.md).

To read the manifest of a built image, see [Reading a manifest programmatically](verification.md#reading-a-manifest-programmatically).

### Invariant severity

| Severity | Effect |
|----------|--------|
| `fatal` | Fails the verification step; recorded in `report.fatal` |
| `warn` | Recorded in `candidate_containers.manifest_warnings`; does not fail the step |
| *(skipped)* | The invariant's inputs were absent, so it was not evaluated |

## Implementation notes

- Docker operations use `python-on-whales` (not `docker-py`), which calls the Docker CLI in a subprocess and is safe to use from many threads
- Image tags are lowercased, as Docker registries require
