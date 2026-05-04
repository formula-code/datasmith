# Model Proxy

`model.formulacode.org` is an OpenAI-compatible HTTPS endpoint that fronts the
local vLLM servers. A LiteLLM proxy multiplexes a single URL across multiple
vLLM backends — clients pick which model they want by setting the `model`
field in the request body, exactly like the OpenAI API.

| Hostname | Auth | Backend |
|---|---|---|
| `model.formulacode.org` | Bearer `LITELLM_MASTER_KEY` (or any virtual key minted from it) | LiteLLM proxy → local vLLM servers |

LiteLLM runs in **DB-backed mode** against the local Supabase Postgres
(database `litellm`, separate from the datasmith schema). This unlocks the
admin UI, virtual keys, spend logs, and live config edits — everything the
master key alone can't do.

## Architecture

```mermaid
flowchart LR
    Client["OpenAI SDK client<br/>(Bearer master_key or virtual key)"]
    CFE["model.formulacode.org"]
    subgraph Host["Host machine"]
        CFD["cloudflared<br/>(datasmith-model tunnel)"]
        LL["LiteLLM proxy :4100"]
        PG[("Postgres :54322<br/>litellm db")]
        VA["vllm serve :8123<br/>model A"]
        VB["vllm serve :8124<br/>model B"]
    end
    Client --> CFE --> CFD --> LL
    LL <--> PG
    LL -- "model: A" --> VA
    LL -- "model: B" --> VB
```

vLLM bound to localhost only; LiteLLM is the single internet-facing
process. Auth is enforced by LiteLLM's `master_key` (or scoped virtual keys
issued from it) — vLLM has none.

---

## For users

### Calling the proxy

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://model.formulacode.org/v1",
    api_key="sk-fc-<master-key-or-virtual-key>",
)

resp = client.chat.completions.create(
    model="gemma-4-31b",                   # alias from infra/litellm.config.yaml
    messages=[{"role": "user", "content": "hello"}],
)
```

`/v1/models` lists the configured aliases:

```bash
curl -H "Authorization: Bearer sk-fc-..." \
    https://model.formulacode.org/v1/models
```

### Admin UI

Visit `https://model.formulacode.org/ui/` and log in with username `admin`,
password = `LITELLM_MASTER_KEY`. The UI is convenience over the same
REST API; everything you can do there has a `curl` equivalent.

---

## For operators

### Prerequisites

- vLLM servers already running on `127.0.0.1:8123` and `127.0.0.1:8124`.
  Bind to localhost — they have no auth and must not be exposed.
- Local Supabase Postgres reachable on `127.0.0.1:54322` (it already is for
  the datasmith pipeline; LiteLLM uses a separate `litellm` database in the
  same instance).
- `cloudflared` installed and authenticated (`cert.pem` present in
  `~/.cloudflared/`); see [Remote Access](remote-access.md) for the install
  steps.
- `uv` available on PATH.

### `tokens.env`

```bash
# Bearer master key. Generate with: openssl rand -hex 32
LITELLM_MASTER_KEY=sk-fc-<random-hex>

# DB-backed mode (UI + virtual keys + spend logs).
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/litellm
LITELLM_SALT_KEY=sk-salt-<random-hex>          # encrypts virtual keys at rest
STORE_MODEL_IN_DB=True
```

The model list — aliases, vLLM ports, upstream model ids — lives in
`infra/litellm.config.yaml` and is checked in. The
`litellm_params.model` value must be prefixed `openai/` so LiteLLM speaks
the OpenAI-compatible protocol vLLM exposes; the suffix after the prefix is
forwarded to vLLM in the request body and should match what `vllm serve`
loaded.

### One-time setup

```bash
# 1. Create the tunnel and copy its UUID from the output
cloudflared tunnel create datasmith-model

# 2. Patch the placeholder in ~/.cloudflared/config-model.yml
TUNNEL_ID=<paste-uuid>
sed -i "s/REPLACE_WITH_MODEL_TUNNEL_ID/$TUNNEL_ID/g" ~/.cloudflared/config-model.yml
cloudflared --config ~/.cloudflared/config-model.yml tunnel ingress validate

# 3. Route DNS. --config + --overwrite-dns are required when multiple
#    tunnels coexist; without them cloudflared resolves the name against
#    ~/.cloudflared/config.yml and CNAMEs to the wrong tunnel.
cloudflared --config ~/.cloudflared/config-model.yml tunnel route dns \
    --overwrite-dns datasmith-model model.formulacode.org

# 4. Create the LiteLLM database in the local Supabase Postgres.
docker exec supabase_db_datasmith_new psql -U postgres -c "CREATE DATABASE litellm;"

# 5. Build the persistent venv used by `make model-tunnel` (this also runs
#    `prisma generate` to fetch the query-engine binary). LiteLLM applies
#    Prisma migrations into the `litellm` DB on first boot.
make model-proxy-install
```

### Day-to-day

```bash
make model-tunnel
```

This single target:

1. Loads `tokens.env` into the environment.
2. Launches the LiteLLM binary from `.venv-litellm/` on `127.0.0.1:4100`,
   reading `infra/litellm.config.yaml`. The child runs from inside the
   venv directory to dodge Prisma's pyproject.toml lookup, which trips over
   the repo's `[[tool.mypy.overrides]]` array-of-tables.
3. Polls `:4100/health/liveliness` until LiteLLM is ready.
4. Starts `cloudflared tunnel run datasmith-model` in the foreground.
5. On Ctrl-C, kills LiteLLM and exits.

Run it under `tmux`/`screen` or as a systemd service for persistence; the
target itself is foreground only.

### Adding or swapping a model

Edit `infra/litellm.config.yaml` and restart `make model-tunnel`.
Each entry under `model_list` is `model_name` (the alias clients pass) +
`litellm_params.model` (`openai/<vllm-loaded-id>`) + `api_base`
(`http://127.0.0.1:<port>/v1`). With `STORE_MODEL_IN_DB=True` you can also
add models live from the UI — they persist in `LiteLLM_ProxyModelTable` and
survive restarts without editing the YAML.

### Issuing virtual keys

Mint a scoped key (different from the master key, individually revocable):

```bash
curl -sS -X POST -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
    -H "Content-Type: application/json" \
    https://model.formulacode.org/key/generate \
    -d '{"models":["gemma-4-31b"],"key_alias":"alice","duration":"30d","max_budget":50}'
```

The response includes `"key": "sk-..."` — that's what you hand to the user.
Revoke with `POST /key/delete` body `{"keys":["sk-..."]}`. Browse / search
keys in the UI under `/ui/key-management`.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `401 Unauthorized` | Bearer token does not match the master key or any active virtual key |
| `400 model not found` | The `model` field does not match any `model_name` in `infra/litellm.config.yaml` (or any DB-stored model) |
| `502 Bad Gateway` | LiteLLM up, vLLM backend down — check `curl http://127.0.0.1:8123/v1/models` |
| UI shows `Authentication Error: Not connected to DB!` | `DATABASE_URL` not set, or the `litellm` database doesn't exist yet — re-run step 4 of one-time setup |
| `Unable to find Prisma binaries. Please run 'prisma generate' first.` | The `.venv-litellm/` venv is missing or stale — `rm -rf .venv-litellm && make model-proxy-install` |
| `tomlkit.exceptions.ParseError: Key "overrides" already exists` during prisma generate | Prisma is reading the repo's `pyproject.toml` from cwd. The Makefile already cd's into `.venv-litellm` to dodge this; if you're invoking prisma manually, do the same. |
| `litellm exited before becoming ready` | Read the LiteLLM stderr above the failure line — usually a missing env var or a Postgres connectivity issue |
| Responses look like a different LiteLLM (e.g. `no_db_connection` from the chat path) | Port 4100 is taken by another process; LiteLLM falls back to an ephemeral port and the tunnel/health-check ends up hitting the squatter. Check `ss -tlnp \| grep :4100` and pick an unused port in `Makefile`, `~/.cloudflared/config-model.yml`, and this doc. |
| Hangs on long generations | Increase `request_timeout` in `infra/litellm.config.yaml` |

### Security notes

- vLLM has no auth. Bind it to `127.0.0.1`, never `0.0.0.0`.
- The proxy hostname has **no Cloudflare Access gate**; auth is the
  LiteLLM master/virtual key only. Rotate the master by changing
  `LITELLM_MASTER_KEY` in `tokens.env` and restarting `make model-tunnel`.
  Rotate a virtual key by deleting and re-issuing it — only that user is
  affected.
- `LITELLM_SALT_KEY` encrypts virtual key secrets at rest in Postgres.
  Changing it after keys are issued will make existing keys unreadable, so
  treat it as immutable post-bootstrap.
- The `litellm` database is in the same Postgres instance as Supabase but
  is a separate logical database; the LiteLLM tables cannot reach datasmith
  data and vice-versa.
