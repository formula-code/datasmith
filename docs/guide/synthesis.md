# Synthesis

The synthesizer automatically generates Docker build contexts for pull requests. It is a state machine that tries cached and similar scripts before falling back to LLM-based generation.

## How it works

The synthesizer follows a strict try-existing-first strategy:

1. **Check cache** — Look up Supabase for an existing build script for this PR
2. **Find similar** — Query Supabase for similar scripts from the same repository
3. **Try similar** — Run each similar script against the verifier chain
4. **LLM generate** — Fall back to an installed coding agent (Claude Code, Codex, or Gemini)
5. **Fail** — Log all attempts and return `None`

All attempts are logged to the `build_attempts` table so failed PRs can be retried later with improved prompts or models.

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
    env_payload='{"dependencies": ["numpy==1.26.0", "cython==3.0.0"]}',
    python_version="3.10",
)
# ctx is a DockerContext with the working build scripts, or None if all attempts failed
```

The synthesizer handles verification internally — it builds the Docker image
from the generated context and runs the verifier chain as part of each attempt.

## Running at scale

```python
from datasmith.runners import SynthesizeImagesRunner

runner = SynthesizeImagesRunner(synth, n_concurrent=8)
await runner.run(pr_items)
# Returns None entries for PRs where synthesis failed
```

!!! warning
    Running synthesis at scale can be expensive — each LLM attempt may consume significant tokens. Use `n_concurrent` to control parallelism.

## Agent backends

The synthesizer auto-detects which coding agent CLI is installed:

| Agent | CLI Command | Detection |
|-------|-------------|-----------|
| Claude Code | `claude` | Checks `which claude` |
| Codex | `codex` | Checks `which codex` |
| Gemini | `gemini` | Checks `which gemini` |

The agent runs in a sandboxed workspace with the Docker build context, edits build scripts, runs verification, and iterates until the verifier passes or attempts are exhausted.

## Chronological neighborhood cascade

`SynthesizeImagesRunner` uses a queue-based worker pool rather than a fixed
task list. Whenever a PR synthesises successfully, the runner queries for
other PRs in the same repo whose `created_at` is within
`±DATASMITH_NEIGHBOR_WINDOW_DAYS` (default 60) and enqueues them (capped at
`DATASMITH_NEIGHBOR_CAP` per success, default 40). Those neighbor items
re-enter the state machine at `CHECK_CACHE → FIND_SIMILAR`, where
`TRY_SIMILAR` almost always reuses the freshly-cached context for free.
Only PRs whose environment has genuinely drifted fall through to
`LLM_GENERATE` and consume agent budget.

This replaces the old two-pass workflow (run once with `--agent codex`,
then again with `--agent none`) with a single pass that spreads each win
across its chronological neighborhood automatically. A successful
neighbor cascades its own neighbors onto the queue, so a single codex
session can hydrate an entire repo's worth of adjacent PRs.

Seed items still respect `--tasks-per-repo`; neighbors are additive and
only bounded by `DATASMITH_NEIGHBOR_CAP` and the `_enqueued` dedupe set.

## Rate-limit handling

Codex and Claude both enforce periodic usage limits (five-hour and weekly
buckets) that can exhaust mid-run. The synthesizer detects these from the
raw agent output in two ways:

- **Codex** — parses the free-text `{"type":"error", ...}` event (the
  `"You've hit your usage limit ... try again at <date>"` message) and
  extracts the reset time.
- **Claude** — parses structured `rate_limit_event` records for any
  `status` outside `{allowed, allowed_warning}` and reads `resetsAt` as a
  unix epoch.

When detected, the attempt is logged to `error_logs` with
`failure_stage='rate_limited'` and `rate_limit_reset_at` set to the parsed
reset time. A `RateLimitError` bubbles up into
`SynthesizeImagesRunner._process_item`, which installs a shared pause on
the runner — every worker blocks in `_wait_for_rate_limit` until the clock
passes the reset time (plus a small jitter) instead of continuing to burn
the attempt budget on sub-3-second failures. Each item is retried up to
`DATASMITH_RL_MAX_RETRIES` times across rate-limit pauses before being
marked as failed. If no reset time could be parsed, the pause defaults to
`DATASMITH_RL_DEFAULT_PAUSE_S` seconds.

See [Configuration → Tunable constants](configuration.md#tunable-constants)
for the full list of overridable knobs.

## Dependencies on resolution

The synthesizer requires `env_payload` (pinned dependencies) and `python_version` from pipeline Stage 4 (Resolve Packages). Without this data, `docker_build_env.sh` cannot install the correct packages. Always run resolution before synthesis.
