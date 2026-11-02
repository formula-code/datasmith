# Synthesis

The synthesizer writes Docker build contexts for pull requests. It is a state machine that tries cached, similar and default scripts before it asks an LLM agent.

## How it works

The synthesizer tries existing scripts first:

1. **Check cache**: look up a stored build context for this commit in Supabase.
2. **Find similar**: query stored contexts from other PRs of the same repository. `DATASMITH_SKIP_SIMILAR_CONTEXTS=1` skips this step and the next.
3. **Try similar**: build and check each similar context.
4. **Try default**: build with the stock template scripts in `docker/templates/`. This step is skipped for a repository where the default already succeeded, after too many default failures, or when a stored context exists for the commit.
5. **Produce and verify**: when `DATASMITH_PV_ENABLED` is set, a producer agent edits the context and a verifier agent checks the image (`DATASMITH_PV_PRODUCER_AGENT` and `DATASMITH_PV_VERIFIER_AGENT`, default `codex`).
6. **LLM generate**: an installed coding agent (Claude Code, Codex, Gemini or Qwen Code) writes the scripts in a sandbox.
7. **Fail**: return `None`.

`local_ci.py` checks each attempt inside the sandbox: it builds the image, runs the tests, runs the measure step and checks the fatal invariants of the build manifest. A built context also goes through a tamper audit of its scripts and a scan of the image on the host before it is stored. A success is stored in the `candidate_containers` table. Every attempt is logged to the `error_logs` table, so a failed PR can be retried later.

## Basic usage

```python
from datasmith.agents import Synthesizer

synth = Synthesizer(max_attempts=3)
ctx = synth.run(
    owner="pandas-dev",
    repo="pandas",
    issue_number=16222,
    pr_context="This PR optimizes groupby performance by ...",
    sha="abc123def456",
    base_sha="def456abc123",   # the commit before the optimization, which the image checks out
    env_payload='{"dependencies": ["numpy==1.26.0", "cython==3.0.0"]}',
    python_version="3.10",
    solution_patch=pr_patch,   # the oracle patch, used only to check that the task is measurable
)
# ctx is a DockerContext with the working build scripts, or None if all attempts failed.
```

## Running at scale

```python
from datasmith.runners import SynthesizeImagesRunner

runner = SynthesizeImagesRunner(synth, n_concurrent=8)
await runner.run(pr_items)
```

!!! warning
    Each LLM attempt uses many agent tokens. Use `n_concurrent` to limit parallelism.

## Agent backends

The synthesizer auto-detects which coding agent CLI is installed:

| Agent | CLI Command | Detection |
|-------|-------------|-----------|
| Claude Code | `claude` | Checks `which claude` |
| Codex | `codex` | Checks `which codex` |
| Gemini | `gemini` | Checks `which gemini` |
| Qwen Code | `qwen` | Checks `which qwen` |

The agent runs in a sandboxed workspace with the Docker build context, edits build scripts, runs verification, and iterates until the verifier passes or attempts are exhausted.

## Chronological neighborhood cascade

`SynthesizeImagesRunner` uses a queue of work items. When a PR succeeds, the
runner queries other PRs in the same repository whose `created_at` is within
`±DATASMITH_NEIGHBOR_WINDOW_DAYS` (default 60) and enqueues them, at most
`DATASMITH_NEIGHBOR_CAP` (default 40) per success. A neighbor starts at
`CHECK_CACHE`, and `TRY_SIMILAR` usually reuses the new context. Only a PR
whose environment differs reaches `LLM_GENERATE` and uses agent budget.
A successful neighbor enqueues its own neighbors.

`--tasks-per-repo` limits only the first items. Neighbors are limited by
`DATASMITH_NEIGHBOR_CAP` and by the set of items already enqueued.

## Rate-limit handling

Codex and Claude have usage limits per five hours and per week, which a run can use up. The synthesizer detects these from the
raw agent output in two ways:

- **Codex** — parses the free-text `{"type":"error", ...}` event (the
  `"You've hit your usage limit ... try again at <date>"` message) and
  extracts the reset time.
- **Claude** — parses structured `rate_limit_event` records for any
  `status` outside `{allowed, allowed_warning}` and reads `resetsAt` as a
  unix epoch.

When detected, the attempt is logged to `error_logs` with
`failure_stage='rate_limited'` and `rate_limit_reset_at` set to the parsed
reset time. A `RateLimitError` goes up to
`SynthesizeImagesRunner._process_item`, which sets a pause shared by all
workers. Each worker waits in `_wait_for_rate_limit` until the reset time plus
`DATASMITH_RL_PAUSE_JITTER_S`, so the attempt budget is not spent on failures
that take a few seconds. Each item is retried up to
`DATASMITH_RL_MAX_RETRIES` times across rate-limit pauses before being
marked as failed. If no reset time could be parsed, the pause defaults to
`DATASMITH_RL_DEFAULT_PAUSE_S` seconds.

See [Configuration → Tunable constants](configuration.md#tunable-constants)
for the variables that change these values.

## Dependencies on resolution

The synthesizer needs `env_payload` (pinned dependencies) and `python_version` from stage 4 (resolve packages). Without them, `docker_build_env.sh` cannot install the correct packages. Run stage 4 before synthesis.

## Repositories that often fail synthesis

- **Native extensions** (numpy, scipy, astropy, h5py, MDAnalysis, shapely, cartopy, geopandas, scikit-image): the
  build needs system libraries and compilers (BLAS/LAPACK, GDAL, GEOS, PROJ, HDF5, Fortran) that the generated
  `build_pkg_sh` often does not install.
- **GPU or vendor toolchains** (mujoco_warp, dpctl, dpnp, NVIDIA/warp, NVIDIA/physicsnemo): CUDA or SYCL is not in the
  base image and cannot be installed with pip.
- **Not a plain Python package** (PostHog/posthog, conda/conda-build): not installable with `pip install -e .`.
- **Multi-package ecosystems** (dask, dask/distributed, mars-project/mars): the package needs companion packages at
  matching versions.
- **Old commits**: pins resolved at the commit date can still fail on the code of that time. Reusing a nearby
  successful build context helps only when one exists.
