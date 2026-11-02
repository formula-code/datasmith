# Configuration

fc-data reads its configuration from a `tokens.env` file in the repository root. `datasmith/__init__.py` loads the file on import, and the `Settings` class (`pydantic-settings`) reads it too.

## Environment variables

### Required

| Variable | Description |
|----------|-------------|
| `SUPABASE_URL` | Supabase instance URL (e.g., `http://127.0.0.1:54321`) |
| `SUPABASE_KEY` | Supabase service role key |
| `GH_TOKENS` | Comma-separated GitHub personal access tokens |

### LLM backends

| Variable | Description | Default |
|----------|-------------|---------|
| `DSPY_MODEL` | Model identifier | `openai/gpt-oss-120b` |
| `DSPY_API_BASE` | API base URL | `http://localhost:30001/v1` |
| `DSPY_API_KEY` | API key for the LLM provider | `local` |
| `DSPY_MAX_TOKENS` | Maximum tokens per request | `16000` |
| `DSPY_TEMPERATURE` | Sampling temperature | `0` |
| `PORTKEY_API_KEY` | Portkey AI gateway key (alternative backend) | — |
| `PORTKEY_MODEL_NAME` | Model for Portkey | `@anthropic/claude-3-5-sonnet-latest` |

### Publishing

| Variable | Description |
|----------|-------------|
| `DOCKERHUB_USERNAME` | DockerHub username |
| `DOCKERHUB_TOKEN` | DockerHub access token |
| `HF_TOKEN_PATH` | Path to HuggingFace token file (default `~/.cache/huggingface/token`; `HF_TOKEN` is used if the file is missing) |

### Tunable constants

Each tunable module constant (timeouts, retries, caps, windows,
concurrency limits) reads an environment variable with the `DATASMITH_`
prefix. Set it in `tokens.env` to change it without a code change.
The tables below list the stage 6 variables.

#### Stage 6: synthesize_images

Rate-limit pause behavior (see [Synthesis → Rate-limit handling](synthesis.md#rate-limit-handling)):

| Variable | Description | Default |
|----------|-------------|---------|
| `DATASMITH_RL_DEFAULT_PAUSE_S` | Fallback pause (seconds) when the agent signals a rate limit but no reset time could be parsed | `3600` |
| `DATASMITH_RL_PAUSE_JITTER_S` | Grace seconds added to the parsed reset time before workers resume, to ride out clock skew | `30` |
| `DATASMITH_RL_MAX_RETRIES` | Maximum consecutive rate-limit pauses for a single item before it is marked failed | `3` |

Chronological neighborhood cascade (see [Synthesis → Chronological neighborhood cascade](synthesis.md#chronological-neighborhood-cascade)):

| Variable | Description | Default |
|----------|-------------|---------|
| `DATASMITH_NEIGHBOR_WINDOW_DAYS` | ± window, in days of PR `created_at`, for enqueuing neighbor PRs after a successful synthesis | `60` |
| `DATASMITH_NEIGHBOR_CAP` | Maximum neighbor PRs enqueued per successful item | `40` |

`DATASMITH_NEIGHBOR_CAP=0` turns off the cascade. The runner then processes
only the items it was given, which keeps progress counts exact in small reruns.

## Agent backend resolution

`agents/config.py` configures DSPy for stages 3 and 5 in this order:

1. **DSPy endpoint**: if `DSPY_API_KEY` and `DSPY_MODEL` are not empty, it uses `DSPY_MODEL` at `DSPY_API_BASE`. Both have defaults, so this is the usual case.
2. **Portkey**: if one of them is set to an empty value and `PORTKEY_API_KEY` is set, it uses the Portkey gateway with `PORTKEY_MODEL_NAME`.
3. Otherwise no LLM is configured and a warning is logged.

Stage 6 also uses DSPy when it renders a missing problem statement. Its build agent is a CLI agent that uses its own login.

## tokens.env template

```bash
# Supabase (required)
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_KEY=your-service-role-key

# GitHub (required — comma-separated for multiple tokens)
GH_TOKENS=github_pat_xxx,github_pat_yyy

# LLM backend (for classification and problem rendering)
DSPY_MODEL=openai/gpt-oss-120b
DSPY_API_BASE=http://localhost:30000/v1
DSPY_API_KEY=local
DSPY_MAX_TOKENS=16000

# DockerHub (for publishing)
DOCKERHUB_USERNAME=formulacode
DOCKERHUB_TOKEN=dckr_pat_xxxxx

# HuggingFace (for dataset publishing)
HF_TOKEN_PATH=/path/to/huggingface/token
```
