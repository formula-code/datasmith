# Configuration

fc-data is configured primarily through a `tokens.env` file in the repository root. The `Settings` class (powered by `pydantic-settings`) loads these automatically.

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
| `DSPY_MODEL` | Model identifier (e.g., `openai/gpt-oss-120b`) | — |
| `DSPY_API_BASE` | API base URL | — |
| `DSPY_API_KEY` | API key for the LLM provider | — |
| `DSPY_MAX_TOKENS` | Maximum tokens per request | `16000` |
| `DSPY_TEMPERATURE` | Sampling temperature | `0.0` |
| `PORTKEY_API_KEY` | Portkey AI gateway key (alternative backend) | — |
| `ANTHROPIC_API_KEY` | Anthropic API key (alternative backend) | — |

### Publishing

| Variable | Description |
|----------|-------------|
| `DOCKERHUB_USERNAME` | DockerHub username |
| `DOCKERHUB_TOKEN` | DockerHub access token |
| `HF_TOKEN_PATH` | Path to HuggingFace token file |

### Tunable constants

Any module-level constant that is a knob — timeouts, retries, caps,
windows, concurrency limits — is overridable from `tokens.env` without a
code change. Every such variable is prefixed `DATASMITH_` so it is
globally greppable in both Python and shell env. `tokens.env` is loaded
at import time by `datasmith/__init__.py`, so setting one of these in the
file is enough; no export needed.

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
| `DATASMITH_NEIGHBOR_CAP` | Hard ceiling on neighbor PRs enqueued per successful item | `40` |

Setting `DATASMITH_NEIGHBOR_CAP=0` disables the cascade entirely — the
runner then behaves like a pre-cascade fixed-item pool, useful for tight
unit-style reruns where extra enqueues would confuse progress tracking.

## Agent backend resolution

The agent configuration (`agents/config.py`) checks environment variables in priority order:

1. **Portkey** — `PORTKEY_API_KEY` present → uses Portkey AI gateway
2. **Anthropic** — `ANTHROPIC_API_KEY` present → uses `anthropic/claude-3-opus-20240229`
3. **vLLM/Local** — `DSPY_API_KEY` present → uses `DSPY_MODEL` + `DSPY_API_BASE`
4. **Fallback** — Local defaults

## tokens.env template

```bash
# Supabase (required)
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_KEY=your-service-role-key

# GitHub (required — comma-separated for multiple tokens)
GH_TOKENS=github_pat_xxx,github_pat_yyy

# LLM backends (for classification and synthesis)
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
