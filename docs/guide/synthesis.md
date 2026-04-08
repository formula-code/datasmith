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

## Dependencies on resolution

The synthesizer requires `env_payload` (pinned dependencies) and `python_version` from pipeline Stage 4 (Resolve Packages). Without this data, `docker_build_env.sh` cannot install the correct packages. Always run resolution before synthesis.
