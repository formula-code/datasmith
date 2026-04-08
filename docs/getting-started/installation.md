# Installation

## Prerequisites

- **Python 3.9–3.12**
- **[uv](https://astral.sh/uv/)** — Fast Python package manager
- **[Node.js](https://nodejs.org/)** — For Supabase CLI
- **Docker** — For image building and verification

## 1. Install system dependencies

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Node.js via nvm (for Supabase CLI)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
nvm install --lts
nvm use --lts
```

## 2. Clone and install

```bash
git clone https://github.com/formula-code/datasmith.git
cd datasmith

# Install dev environment and pre-commit hooks
make install
```

This creates a virtual environment with `uv`, installs all dependencies, and sets up pre-commit hooks.

## 3. Configure `tokens.env`

DataSmith reads all configuration from a `tokens.env` file in the repo root. The `Settings` class (powered by `pydantic-settings`) loads it automatically — no manual `source` or `export` needed.

Create the file:

```bash
touch tokens.env
```

### Required variables

These are needed for any pipeline run:

```bash
# === Supabase (required) ===
# Local Supabase instance — started in the next step.
# SUPABASE_URL points to the PostgREST API (not the Postgres port).
# SUPABASE_KEY is the service-role key printed by `npx supabase status`.
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_KEY=<paste service-role key here>

# === GitHub (required) ===
# One or more GitHub personal access tokens, comma-separated.
# DataSmith rotates tokens automatically when one hits the rate limit.
# Create tokens at https://github.com/settings/tokens with `repo` scope.
GH_TOKENS=github_pat_xxx
```

### LLM backend variables

Required for stages 3 (classification) and 6 (synthesis):

```bash
# === LLM backends ===
# DSPy-compatible endpoint (vLLM, OpenAI, etc.)
DSPY_MODEL=openai/gpt-oss-120b
DSPY_API_BASE=http://localhost:30000/v1
DSPY_API_KEY=local
DSPY_MAX_TOKENS=16000
```

Alternative backends (checked in priority order — first match wins):

| Variable | Backend |
|----------|---------|
| `PORTKEY_API_KEY` | Portkey AI gateway |
| `ANTHROPIC_API_KEY` | Anthropic (Claude) |
| `DSPY_API_KEY` + `DSPY_API_BASE` | vLLM / OpenAI-compatible |

### Publishing variables

Required only for stage 7 (publish):

```bash
# === DockerHub ===
DOCKERHUB_USERNAME=formulacode
DOCKERHUB_TOKEN=dckr_pat_xxxxx

# === HuggingFace ===
HF_TOKEN_PATH=/path/to/huggingface/token
```

See [Configuration](../guide/configuration.md) for a complete reference of all environment variables.

## 4. Set up Supabase

DataSmith uses a local Supabase instance for all persistent state (no cloud account needed).

### Start the instance

```bash
npx supabase start
```

This pulls and starts Postgres, PostgREST, Auth, Storage, and Studio containers. The first run takes a few minutes to download images.

### Get your service-role key

After startup, run:

```bash
npx supabase status
```

This prints connection details. Copy the **service_role key** (not the anon key) and paste it as `SUPABASE_KEY` in your `tokens.env`:

```
         API URL: http://127.0.0.1:54321
     GraphQL URL: http://127.0.0.1:54321/graphql/v1
          DB URL: postgresql://postgres:postgres@127.0.0.1:54322/postgres
      Studio URL: http://127.0.0.1:54323
        ...
   service_role key: eyJhbGciOiJIUzI1NiIs...   <-- copy this
```

### Apply migrations

DataSmith's schema is defined in numbered SQL migrations:

```bash
npx supabase migration up --local
```

This creates all required tables (`pull_requests`, `packages`, `candidate_containers`, `error_logs`, `runner_progress`, `runner_failures`, `candidate_prs`, `hook_cache`, etc.).

### Common Supabase commands

```bash
npx supabase status               # Show URLs, ports, and service health
npx supabase migration list --local # List applied / pending migrations
npx supabase db reset             # Wipe and recreate from migrations (destructive!)
npx supabase stop                 # Stop all containers
```

### Supabase Studio

A web UI for browsing tables and running queries is available at the Studio URL printed by `supabase status` (default `http://127.0.0.1:54323`).

### Direct Postgres access

For ad-hoc queries or debugging, connect directly to Postgres:

```bash
psql postgresql://postgres:postgres@127.0.0.1:54322/postgres
```

## 5. Verify your setup

Run the preflight check to confirm everything is configured:

```bash
python -m datasmith.preflight
```

This validates:

| Check | What it verifies |
|-------|-----------------|
| Environment | `SUPABASE_URL`, `SUPABASE_KEY`, `GH_TOKENS` are set |
| Supabase | Database connection succeeds |
| Docker | Docker daemon is running |
| GitHub | API access works and rate limit is available |

Then run the test suite:

```bash
make check    # Ruff lint + mypy type check
make test     # pytest
```

## Next steps

You're ready to run the pipeline:

```bash
fc-data --start-date 2026-03-01 --end-date 2026-04-01
```

See the **[Pipeline guide](../guide/pipeline.md)** for the full CLI reference and stage descriptions.
