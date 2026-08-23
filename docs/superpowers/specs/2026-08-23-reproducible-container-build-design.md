# Reproducible container builds

**Status:** design approved in conversation, not implemented
**Date:** 2026-08-23
**Branch:** `spec/build-manifest-verification`
**Supersedes:** the stage-6 synthesis design, for the container-build part only

---

## 1. Goal

Build container images that hold a repository at a base commit, with all
dependencies installed and all extensions compiled, such that a rebuild
installs the same versions.

That is the whole goal of this document. Measurement, scoring, and publishing
are out of scope. See section 9.

### Definition of reproducible

Three definitions were considered. This design adopts the second.

| Option | Meaning | Verdict |
|---|---|---|
| Rebuildable | a rebuild produces a working environment, versions may differ | rejected, does not protect a timing comparison |
| **Version-reproducible** | **every package resolves to an exact version with a recorded hash** | **adopted** |
| Bit-reproducible | two builds produce the same image digest | rejected, large cost and little added value |

Bit-reproducibility needs `SOURCE_DATE_EPOCH`, deterministic archive ordering,
and a pinned base digest. Debian and Nix achieve it. A timing depends on
installed versions and build flags, and not on archive byte order, so the extra
work buys little here.

### Prior art

Debian's `.buildinfo` records the full recursive dependency closure at exact
versions. PEP 751 `pylock.toml` mandates hashes. Both are records of what a
build used, and not declarations of what it should use. This design follows
that split.

---

## 2. Why the current system is replaced

Every item below was verified directly in this repository. File and line
references point at the current working tree.

### 2.1 Defects that change measured numbers

**A missing import corrupted 97% of the corpus.**
`src/datasmith/docker/templates/pytest_runner.py:708` calls `sys.exit()`. The
file imports `argparse`, `json`, `os`, `shlex`, `subprocess`, `time`, and
`glob`. It does not import `sys`. Every test run therefore raises a
`NameError`.

The text `name 'sys' is not defined` appears in 989 `error_logs` rows, across
all 134 repositories, and 765 of those rows belong to sessions recorded as
successes.

The agent may only edit `docker_build_pkg.sh` and `docker_build_run.sh`, so it
cannot correct the file. 130 of 134 repositories added a workaround, and 128
repositories use the literal assignment `builtins.sys = sys`, across 1361
`candidate_containers` rows. That workaround runs in every Python process in
the image, and the measured benchmark process is one of them.

The design that baked the harness into the image caused this. The agent had no
other lever.

**The compiler and the measurement tools are unpinned.**
`docker_build_base.sh:774` installs the conda-forge `compilers` package with no
version, together with `meson-python`, `cmake`, and `ninja`. Line 790 installs
asv from the git default branch. `docker_build_final.sh` installs LSV and
snapshot-tester the same way, and appends `|| true`. The toolchain in an image
is therefore a function of its build date.

**Thread limits are set once and never checked.**
`Dockerfile.base` lines 11 to 13 set `OPENBLAS_NUM_THREADS`,
`MKL_NUM_THREADS`, and `OMP_NUM_THREADS` to 1. Those names appear nowhere else
in `src/`. Nothing seals them, and nothing compares them between measurement
legs.

### 2.2 Gates that report success without checking

**The pin invariant ignores versions.**
`docker/manifest.py::_c_pins` strips the version from both sides and compares
names. A request for `cython==3.0.5` passes against a resolved
`cython==0.29.33`. Cython 0.29 and Cython 3.0 emit different C.

**The resolved pin list is corrupted before the check reads it.**
A shipped image contains:

```
pins_resolved = ['accessible-pygments==0.0.5', 'affine==2.4.0',
                 'alabaster==1.0.0', 'archspec', '@',
                 'file:///home/conda/feeds/...']
```

The entry `archspec @ file:///...` was split on whitespace into three tokens.

**Two manifest fields disagree, and one is never produced.**
The same image seals `rounds = 5` in the `build` block and `measure_rounds = 2`
in the `verify` block. `cpu_cap` is `None`, because it reads two environment
variables that nothing sets.

### 2.3 Defects that waste compute

**The pipeline never reads a repository's own configuration at all.**
`resolution/orchestrator.py:81` parses each `asv.conf.json` with
`json5.loads`, which returns a plain dict. Lines 86 to 99 then read that dict
with `getattr(cfg, "pythons", [])`, `getattr(cfg, "build_command", None)`,
`getattr(cfg, "install_command", None)`, and `getattr(cfg, "matrix", None)`.
`getattr` is attribute access, and a dict has no such attributes, so every one
of those calls returns its default.

The whole read is therefore a no-op. `pythons` is always empty, so line 111
falls back to `SUPPORTED_PYTHON_VERSIONS` and the repository's declared Python
version is never used. `build_command`, `install_command`, and `matrix` are
always empty.

A second defect sits below the first and is currently unreachable.
`orchestrator.py:269` iterates `cfg_items.matrix.values()` and discards the
keys. ASV defines `matrix` as a map from package name to required versions. For
the pandas declaration
`{"numpy": [], "Cython": ["0.29.21"], "matplotlib": [], "sqlalchemy": []}` that
code derives the single string `'0.29.21'` and passes it to the resolver as a
package name. Fixing the first defect exposes the second, so both must be fixed
together.

`apache/arrow` declares `boost-cpp: ["1.68.0"]` and ships its own
`asv-build.sh`. `profile.sh:97` forces `environment_type` to `existing`, which
makes the `build_command` and `install_command` we copy forward inert.

**One agent backend never worked.**
The gemini backend recorded 3763 attempts and zero successes. codex succeeded
on 30.9% of attempts, and claude on 13.8%.

**The pipeline deletes its own build cache, and never had one.**
`utils/docker_prune.py` sets `_DEFAULT_INTERVAL_SEC = 7200`, and
`update/pipeline.py:647` wraps the whole of stage 6 in
`builder_prune_watcher()`. `docker builder prune` removes BuildKit cache.
Separately, all three Dockerfiles declare `# syntax=docker/dockerfile:1.7`, and
none contains a `RUN --mount=type=cache` directive.

Docker's data root is `/mnt/sdd2/docker`, on a filesystem that is 8% full with
6.1 TB free. The prune protects a disk that is not under pressure.

**The agent is told builds cost four times what they cost.**
`AGENTS.md.j2` states that a build takes "15 to 40 minutes, sometimes up to 60",
and instructs the agent that "a single end-to-end run is normal and expected".
Measured `build_duration_s` is 446 seconds at p50 and 1606 seconds at p90.

Section 4 removes this text, because the agent no longer drives `local_ci.py`.
If any instruction text survives the redesign, it must not carry a cost figure
that nobody re-measures.

### 2.4 The harness is writable by the agent

`Dockerfile.pr` copies `run-tests.sh` and `profile.sh` in the `pkg` stage. The
agent's `docker_build_run.sh` executes in the `run` stage, which is later. 36
`candidate_containers` rows write to `/run-tests.sh` or `/profile.sh`, and 284
rows across 74 repositories reference `formulacode_testrunner`, which is the
name `pytest_runner.py` takes once inlined.

`local_ci.py::_check_file_integrity` hashes the host-side sandbox templates, so
it observes none of this.

---

## 3. The build contract

### 3.1 What must be pinned

| Layer | Pin by | Current state |
|---|---|---|
| Base image | OCI digest | `FROM buildpack-deps:jammy`, a mutable tag |
| System packages | version, from a dated snapshot | unpinned `apt-get install` |
| Conda packages | version, build string, channel, sha256 | `compilers`, `cmake`, `ninja` unpinned |
| Python packages | version, sha256 | `pins_resolved` is whitespace-mangled |
| `asv_runner` | version, sha256 | installed from the git default branch |

The compiler is a conda package, so row three covers it.

### 3.2 What the image holds

`asv/runner.py:26` sets `BENCHMARK_RUN_SCRIPT` to `asv/benchmark.py`. ASV runs
that file as a subprocess under the target environment's Python, and that file
imports `asv_runner.check`, `.discovery`, `.run`, `.server`, `.setup_cache`,
and `.timing`.

So `asv_runner` must be importable inside the image. ASV receives an absolute
path, so ASV itself can stay outside.

**Inside the image, and therefore pinned:**

- the repository at the base commit
- the conda environment with all dependencies
- compiled extensions
- the compiler
- `asv_runner`

**Outside the image:**

- `asv`, including `benchmark.py`
- LSV, `parser.py`, `emit_measure.py`, `apply_oracle_patch.py`
- `run-tests.sh`, `pytest_runner.py`, `profile.sh`, `measure.sh`
- `emit_manifest.py`, which no longer runs at build time
- the build manifest, which becomes a database row and not a file

The mounted `asv` and the pinned `asv_runner` must stay compatible. Both are
pinned, in different places, so the pipeline must check the pair.

### 3.3 The artifact

Each image has five lock files. The pipeline stores them in the database,
keyed by the image, and not inside the image. Section 3.2 removes the build
manifest from the image for the same reason.

1. `base_image_digest` — one string
2. `apt.lock` — package and exact version
3. `conda.lock` — explicit list with URL and sha256, accepted directly by
   `micromamba create --file`
4. `pylock.toml` — PEP 751, hashes mandatory
5. `toolchain.json` — compiler version, `asv_runner` version, and the realized
   `CFLAGS`, `CXXFLAGS`, `LDFLAGS`

---

## 4. How a build is produced

```mermaid
flowchart TB
    S1["Step 1. Deterministic pre-pass<br/>read asv.conf.json and CI config"] --> Q1{"build succeeds?"}
    Q1 -->|yes| DONE["emit lock files"]
    Q1 -->|no| S2["Step 2. Agent in a live container<br/>checkpoint per turn"]
    S2 --> S3["Step 3. Agent emits a typed spec"]
    S3 --> S4["Step 4. Renderer compiles spec to Dockerfile<br/>no model in this path"]
    S4 --> Q2{"clean replay reproduces?"}
    Q2 -->|yes| DONE
    Q2 -->|no| S2
```

### Step 1. Deterministic pre-pass, with no agent

1. Read `asv.conf.json`. Take `pythons`, and filter by commit date. The
   filtering code at `orchestrator.py:118` is correct, but it currently
   filters a fallback set, because the read above it returns nothing. See
   section 2.3.
2. Take `matrix`, and read it as ASV defines it. An empty list means "require
   this package". A version means "pin this package to that version".
3. Take `build_command` and `install_command`, and run them.
4. Read the repository's CI configuration as a second source. Check
   `.github/workflows`, `tox.ini`, and `pyproject.toml` extras.
5. Resolve with `uv pip compile --exclude-newer`, set to the commit date. The
   pipeline already does this.
6. Build. If the build succeeds, stop.

The maintainers wrote this answer, and they keep it current.

### Step 2. Agent, only when step 1 fails

- The agent works in a live container started from the `env`-stage image, and
  may run `pip` and the compiler directly.
- `docker commit` takes a checkpoint before each turn, against the `env` stage
  at about 1 GB, and not against the final image at 9.7 GB.
- A five-second preflight confirms the agent writes output before a session
  starts.
- The attempt cap stays at two. Attempt two produced 176 of 1394 successes,
  which is 12.6%.

### Step 3. The typed spec

| Field | Holds | Prior art |
|---|---|---|
| `apt` | system packages with versions | repo2docker `apt.txt` |
| `conda` | conda packages with versions | conda-forge `meta.yaml` |
| `build_requires` | build-time only packages | conda-forge `requirements/build` |
| `build_env` | `CFLAGS`, `CXXFLAGS`, `LDFLAGS` | pyodide `_BuildSpec`, which validates them |
| `no_build_isolation` | boolean | implied at `docker_build_pkg.sh:88` |
| `install_mode` | `editable` or `wheel` | ASV's default is a built wheel |
| `patches` | diffs against repository files | pyodide `meta.yaml` |
| `files` | new files written into the tree | none found |
| `run` | ordered commands, each with `on_failure: abort` or `continue` | the typed escape hatch |

`on_failure` makes `|| true` impossible to express, which closes a failure path
by structure and not by pattern matching.

`files` has no prior art in anything surveyed. That usually means an existing
format was misread. Treat the field as unproven until a real case needs it.

### Step 4. Replay proves the spec

A renderer compiles the spec into a Dockerfile, with no model in that path. The
pipeline then builds once, cleanly, and compares the result against the live
container. A failed replay reports which command behaved differently.

The recorded shell session is evidence. It is not the deliverable.

---

## 5. Layer structure

Three images, as today. The split is correct. The ordering and the caching are
not.

| Image | Contents | Changes when |
|---|---|---|
| `base` | OS, conda, compiler, `asv_runner` | the pinned config changes |
| `repo` | `base` plus a full clone | the repository changes |
| `task` | `repo` plus checkout, dependencies, extensions | the commit changes |

The `task` image builds in two named stages. `env` holds the dependencies but
not the repository build. `build` holds the compiled repository. Step 2 of
section 4 starts the agent from the `env` stage, and checkpoints against it,
because it is about 1 GB where the finished image is 9.7 GB at p50.

### Install dependencies before checking out the commit

`Dockerfile.pr` runs `git checkout "$COMMIT_SHA"` in its first stage. Docker
invalidates every layer below a changed layer, so a new commit invalidates the
dependency install even when the dependencies did not change.

```
FROM <repo image>
COPY conda.lock pylock.toml /opt/locks/     # changes only when pins change
RUN  install dependencies from the locks     # expensive download
RUN  git checkout "$COMMIT_SHA"              # cheap, per commit
RUN  build the repository                    # expensive compile, per commit
```

### Add three cache mounts

| Mount | Target | Saves |
|---|---|---|
| pip | `/root/.cache/pip` | wheel downloads |
| conda | the micromamba package cache | conda downloads |
| ccache | `/ccache`, with `CC="ccache gcc"` | recompiling C and Cython output |

Cache mounts are not part of the image, and they survive layer invalidation.
That also removes the need for the two `micromamba clean --all --yes` calls,
because the packages never enter the image.

### Stop deleting the cache

Set `DATASMITH_DISABLE_DOCKER_PRUNE=1` during synthesis, and reclaim disk
between stages instead.

### Wasted effort

- `--cache-to type=gha`, because we do not use GitHub Actions
- `type=inline` cache, which excludes the intermediate stages that hold the cost
- exporting cache mounts through `--cache-to`, which BuildKit cannot do
  (moby/buildkit issue 1512, open)
- `--squash` and image flattening, which destroy shared layers
- multi-platform builds, because everything targets `linux/amd64`

---

## 6. Verification

### Test 1. Does the build reproduce?

1. Build the image. Emit the five lock files and a hash manifest of every
   installed file.
2. Rebuild in a clean context, from the five lock files and the typed spec. The
   lock files alone are not enough, because the repository build also needs
   `build_env`, `install_mode`, and any `run` steps.
3. Compare the two hash manifests on name, version, and hash.

Conda writes `info/paths.json` per package with a sha256 per file. Pip writes a
`RECORD` file with hashes. The pipeline reads both.

Image digests will differ. Option 2 does not require bit-reproducibility.

### Test 2. Is the environment sound?

1. The package imports.
2. Every compiled extension module imports. A build that silently skipped an
   extension passes check 1 and fails check 2.
3. Benchmarks discover, and the count is greater than zero.

Check 3 exists today. Check 2 does not.

### Test 3. Did anything shim the environment?

1. `command -v python` resolves inside the conda prefix.
2. That path holds an ELF binary, and not a shell script.
3. No `sitecustomize.py` or `usercustomize.py` sits on `sys.path`, unless its
   hash matches a known value.
4. `LD_PRELOAD` is empty.
5. Installed file hashes match the lock files.

Check 5 is the strongest, and Test 1 already computes it. Checks 1 to 4 cover
what file hashes cannot see, which is a shim placed beside the conda prefix.

Check 2 alone catches the `python` wrapper found in `dynamicslab/pysindy#139`.

### Cost

Test 1 needs a second build. Measured sharing is 1.70 commits per lock set,
across 13016 commits and 7580 distinct lock sets. So Test 1 costs about 1.6
times one build, and not 2 times.

Sharing varies. `apache/arrow` has 385 commits across 105 lock sets.
`scipy/scipy` has 548 across 386.

Tests 2 and 3 are cheap. Run them on every image.

---

## 7. Scale

| Tier | Runs | Count in the full corpus |
|---|---|---|
| `base` image | once per pinned config | 1 |
| `repo` image | once per repository | 147 |
| `task` image | once per commit | 13016 |
| Test 1 rebuild | once per distinct lock set | 7580 |

The target is hundreds of pairs, so the pipeline selects a subset. The table is
a ceiling.

### Measured

- one build takes 446 seconds at p50, and 1606 seconds at p90
- one agent attempt takes 4098 seconds at p50, and succeeds 47.7% of the time
- 20.8% of agent attempts reach the four-hour ceiling
- images are 9.7 GB at p50, and 38 GB at maximum

### Not measured

- what fraction of repositories the pre-pass alone can build
- how much the cache mounts reduce build time

Both decide the total cost. Neither can be derived from current data, because
the pre-pass has never run in this form and no build has used a cache mount.

**Recommendation: run a 20-repository pre-pass trial first.** It needs no
agent, and it costs about 20 builds.

### Risk

No published project automates this at 147 repositories. SWE-bench covers 18.
R2E-Gym covers 13, semi-manually. SWE-Gym covers 11. Each hand-wrote a small
number of specs and reused them across many commits. EnvBench measures 6.69%
success for a general environment-setup agent in a live container.

This design assumes the pre-pass carries most repositories and the agent
handles a tail. If the pre-pass fails often, the tail becomes the main cost.

---

## 8. Open questions

1. How many repositories does the pre-pass build without an agent? Unmeasured.
2. How much do the cache mounts reduce build time? Unmeasured.
3. Does the `files` spec field survive contact with a real case, or was an
   existing format misread?
4. How do we stop the agent writing shell into `run` when a typed field exists?
   A check is possible and is not designed.
5. Which subset of 13016 commits do we build first?

---

## 9. Out of scope

Measurement, scoring, and publishing are excluded by decision. The measurement
apparatus moves outside the image, and this document does not specify it.

The following were found during the same investigation and are recorded so they
are not lost. They belong to the measurement design, not this one.

- Measurements of `networkx#8148` across 8 repeats show a per-benchmark spread
  of 14.8 percentage points at p50, and 104 of 140 benchmarks change sign
  between repeats. The sealed manifest claims a geomean of 1.0654. No repeat
  reproduced it.
- `asv._stats.is_different` needs at least 6 samples per leg before its
  Mann-Whitney branch can fire at `p_threshold=0.002`.
- The publish gate `max_speedup >= 1.05` fires on a simulated zero-effect task
  at 43 and at 139 benchmarks.
- `dynamicslab/pysindy#139` shipped a fabricated measurement, produced by three
  interception layers written by the synthesis agent.

The existing 1856 containers are not migrated. They carry the `builtins.sys`
shim in 128 repositories, an unpinned compiler, and an unpinned asv.

---

## 10. Provenance

Every number in sections 2 to 7 was produced in this repository, from
`error_logs`, `candidate_containers`, and `packages` in the local Supabase
instance, or by reading the named source file, or by running a container from a
local image. Numbers taken from published projects are attributed inline.

Three numbers reported earlier in the investigation were wrong and are not used
here. They are recorded so the corrections are not lost.

- "697 containers built with no agent failures" is meaningless, because
  `error_logs` records successes as well, with `failure_stage = None`.
- "63% of failures are loop latency" is wrong. 6016 of 6066 `aborted` rows fall
  in a two-week window in April 2026, with a median duration of 4 seconds, and
  3710 of them are gemini with empty output.
- "Retries never produce a success" is wrong. `synthesizer.py:54` sets
  `max_attempts = 2`, so no third attempt has ever run.
