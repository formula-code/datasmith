# Configuration

DataSmith is configured primarily through a `tokens.env` file in the repository root. The `Settings` class (powered by `pydantic-settings`) loads these automatically.

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
