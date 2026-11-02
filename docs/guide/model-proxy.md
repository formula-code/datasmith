# Model Proxy

`model.formulacode.org` is an OpenAI-compatible HTTPS endpoint that fronts the
local vLLM servers. One LiteLLM proxy URL serves many vLLM backends. A client
selects the model with the `model` field of the request body, as in the OpenAI API.

| Hostname | Auth | Backend |
|---|---|---|
| `model.formulacode.org` | Bearer `LITELLM_MASTER_KEY` (or any virtual key minted from it) | LiteLLM proxy → local vLLM servers |

LiteLLM runs in **DB-backed mode** against the local Supabase Postgres
(database `litellm`, separate from the datasmith schema). This mode gives the
admin UI, virtual keys, spend logs and live config edits.

Nobody edits the model list by hand. vLLM servers start and stop on any port, so `infra/refresh_models.py` discovers
every live `vllm serve` process, probes its `/v1/models`, and reconciles the
proxy's registry through the admin API (`POST /model/new` / `/model/delete`).
`make model-tunnel` runs this reconciler as a background loop; `make
model-refresh` runs a single pass. The `model_name` clients call is the vLLM
*served id* (`--served-model-name`, or the loaded model path). List the
available models with `/v1/models`; there is no fixed alias.

## Architecture

```mermaid
flowchart LR
    Client["OpenAI SDK client<br/>(Bearer master_key or virtual key)"]
    CFE["model.formulacode.org"]
    subgraph Host["Host machine"]
        CFD["cloudflared<br/>(datasmith-model tunnel)"]
        LL["LiteLLM proxy :4100"]
        RF["refresh_models.py<br/>(reconcile loop)"]
        PG[("Postgres :54322<br/>litellm db")]
        VA["vllm serve :PORT_A<br/>served id A"]
        VB["vllm serve :PORT_B<br/>served id B"]
    end
    Client --> CFE --> CFD --> LL
    LL <--> PG
    RF -- "ps + /v1/models<br/>discover" --> VA & VB
    RF -- "/model/new /delete" --> LL
    LL -- "model: A" --> VA
    LL -- "model: B" --> VB
```

vLLM listens on localhost only, and LiteLLM is the only process reachable from
the internet. LiteLLM checks the `master_key` or a virtual key made from it;
vLLM has no authentication.

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
    model="qwen3.6-35b",                   # a vLLM served id — see /v1/models
    messages=[{"role": "user", "content": "hello"}],
)
```

`/v1/models` lists the running models. The list changes as servers start and stop:

```bash
curl -H "Authorization: Bearer sk-fc-..." \
    https://model.formulacode.org/v1/models
```

### Admin UI

Visit `https://model.formulacode.org/ui/` and log in with username `admin`,
password = `LITELLM_MASTER_KEY`. The UI uses the same REST API, so each UI
action can also be done with `curl`.

---

## For operators

### Prerequisites

- One or more vLLM servers running locally (any ports). The reconciler finds
  them by parsing `ps` for `vllm serve` and probing each `/v1/models`. They
  have no auth, so keep them on loopback / behind the firewall.
- Local Supabase Postgres reachable on `127.0.0.1:54322`, as for the datasmith
  pipeline. LiteLLM uses a separate `litellm` database in the same instance.
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

`infra/litellm.config.yaml` holds only `general_settings` / `litellm_settings`
and an empty `model_list: []` — the model list is reconciled at runtime by
`infra/refresh_models.py`. The reconciler prefixes each `litellm_params.model`
with `openai/` so LiteLLM speaks vLLM's OpenAI-compatible protocol; the suffix
is the served id forwarded to vLLM verbatim. Pin an entry in the YAML only if
it must stay registered regardless of what's running.

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
   venv directory, because Prisma otherwise reads the repo's `pyproject.toml`
   and fails on its `[[tool.mypy.overrides]]` array of tables.
3. Polls `:4100/health/liveliness` until LiteLLM is ready.
4. Starts `infra/refresh_models.py --watch` in the background. It reconciles
   the model registry against live vLLM servers every
   `DATASMITH_MODEL_REFRESH_INTERVAL_S` seconds (default 90).
5. Starts `cloudflared tunnel run datasmith-model` in the foreground.
6. On Ctrl-C, kills the reconciler and LiteLLM and exits.

Run it under `tmux`/`screen` or as a systemd service for persistence; the
target itself is foreground only.

### Adding or swapping a model

Start or stop the `vllm serve` process. Within one reconcile interval the model
is added to or removed from the proxy, with no YAML edit and no restart. To
apply the change at once:

```bash
make model-refresh                 # reconcile once
make model-refresh ARGS="--dry-run"  # preview the plan, change nothing
```

The reconciler only deletes models it added (`db_model`) whose upstream is
gone; anything you pin in `infra/litellm.config.yaml`, or add by hand from the
UI, stays. Settings (override in `tokens.env`):
`DATASMITH_MODEL_REFRESH_INTERVAL_S` (loop period, default 90),
`DATASMITH_MODEL_PROBE_TIMEOUT_S` (per-server probe timeout, default 3),
`DATASMITH_LITELLM_BASE` (admin API base, default `http://127.0.0.1:4100`).

### Issuing virtual keys

Mint a scoped key (different from the master key, individually revocable):

```bash
curl -sS -X POST -H "Authorization: Bearer $LITELLM_MASTER_KEY" \
    -H "Content-Type: application/json" \
    https://model.formulacode.org/key/generate \
    -d '{"models":["gemma-4-31b"],"key_alias":"alice","duration":"30d","max_budget":50}'
```

Give the user the `"key": "sk-..."` value from the response.
Revoke with `POST /key/delete` body `{"keys":["sk-..."]}`. Browse / search
keys in the UI under `/ui/key-management`.

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| `401 Unauthorized` | Bearer token does not match the master key or any active virtual key |
| `400 model not found` | The `model` field does not match any `model_name` in `infra/litellm.config.yaml` (or any DB-stored model) |
| `502 Bad Gateway` | LiteLLM up, vLLM backend down — check `curl http://127.0.0.1:<port>/v1/models`, then `make model-refresh` to prune the dead entry |
| `400 model not found` for a server you just started | The reconciler has not run yet; wait one interval or run `make model-refresh` |
| UI shows `Authentication Error: Not connected to DB!` | `DATABASE_URL` not set, or the `litellm` database doesn't exist yet — re-run step 4 of one-time setup |
| `Unable to find Prisma binaries. Please run 'prisma generate' first.` | The `.venv-litellm/` venv is missing or stale — `rm -rf .venv-litellm && make model-proxy-install` |
| `tomlkit.exceptions.ParseError: Key "overrides" already exists` during prisma generate | Prisma reads the repo's `pyproject.toml` from the working directory. The Makefile runs it from `.venv-litellm`; do the same when you run prisma by hand. |
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
  If you change it after keys are issued, the existing keys become unreadable,
  so do not change it after setup.
- The `litellm` database is in the same Postgres instance as Supabase but
  is a separate logical database; the LiteLLM tables cannot reach datasmith
  data and vice-versa.
