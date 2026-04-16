# Remote Access via Cloudflare Tunnel

fc-data stores all persistent state in a local Supabase instance. By
default this is only reachable from the host machine (`127.0.0.1:54321`).
A **Cloudflare Tunnel** lets a remote machine run the same fc-data
pipeline against the same database — no VPN, no open ports, no firewall
rules.

## Architecture

```
Remote machine                       Host machine
┌──────────────┐                     ┌──────────────────────┐
│  fc-data     │── HTTPS ──▶ Cloudflare Edge ──▶ cloudflared │──▶ Supabase
│  tokens.env: │            (db.formulacode.org)  (tunnel)   │    :54321
│  SUPABASE_URL│                                             │
│  CF headers  │  Cloudflare Access                          │
└──────────────┘  (service-token auth)                       └──────────────────────┘
```

**Two layers of auth protect the database:**

1. **Cloudflare Access** — a service token (`CF-Access-Client-Id` /
   `CF-Access-Client-Secret` headers) must be present on every request
   or Cloudflare rejects it at the edge before it ever reaches the tunnel.
2. **Supabase service-role key** — the standard `apikey` header required
   by PostgREST, unchanged from local usage.

## Prerequisites

- A **Cloudflare account** (free plan is sufficient)
- A **domain managed by Cloudflare** (e.g., `formulacode.org`)
- `cloudflared` CLI installed on the **host machine** (the one running Supabase)

## Host machine setup

### 1. Install cloudflared

```bash
# Debian / Ubuntu
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb \
  -o cloudflared.deb
sudo dpkg -i cloudflared.deb

# Verify
cloudflared --version
```

### 2. Authenticate

```bash
cloudflared login
```

This opens a browser to authorize `cloudflared` with your Cloudflare
account. Select the domain you want to use (e.g., `formulacode.org`).

### 3. Create a tunnel

```bash
cloudflared tunnel create datasmith-db
```

Note the **Tunnel ID** printed (e.g., `a1b2c3d4-...`). A credentials
file is saved to `~/.cloudflared/<TUNNEL_ID>.json`.

### 4. Configure the tunnel

Create `~/.cloudflared/config.yml`:

```yaml
tunnel: <TUNNEL_ID>
credentials-file: /home/<user>/.cloudflared/<TUNNEL_ID>.json

ingress:
  - hostname: db.formulacode.org
    service: http://localhost:54321
  - service: http_status:404
```

### 5. Create the DNS record

```bash
cloudflared tunnel route dns datasmith-db db.formulacode.org
```

This creates a CNAME record pointing `db.formulacode.org` to the tunnel.

### 6. Run the tunnel

```bash
# Using the Makefile target (recommended)
make db-tunnel

# Or directly
cloudflared tunnel run datasmith-db

# As a systemd service (persistent, survives reboots)
sudo cloudflared service install
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
```

At this point, `https://db.formulacode.org` proxies to your local
Supabase PostgREST API — but **Cloudflare Access blocks all requests**
until you create an access policy.

## Cloudflare Access setup

### 1. Create an application

1. Go to [Cloudflare Zero Trust](https://one.dash.cloudflare.com/) →
   **Access** → **Applications**
2. Click **Add an application** → **Self-hosted**
3. Set:
   - **Application name**: `datasmith-db`
   - **Session duration**: `24 hours`
   - **Application domain**: `db.formulacode.org`
4. Under **Policies**, create a policy:
   - **Policy name**: `Service Token`
   - **Action**: Service Auth
   - **Include**: Service Token (select the token you'll create next)
5. Save the application

### 2. Create a service token

1. Go to **Access** → **Service Auth** → **Service Tokens**
2. Click **Create Service Token**
3. Name it (e.g., `datasmith-remote`)
4. **Copy both values immediately** — the Client Secret is only shown once:
   - `CF-Access-Client-Id` (e.g., `abc123.access`)
   - `CF-Access-Client-Secret` (e.g., `long-secret-string`)

## Remote machine setup

On the remote machine, edit `tokens.env`:

```bash
# Point at the tunnel instead of localhost
SUPABASE_URL=https://db.formulacode.org
SUPABASE_KEY=your-service-role-key          # Same key as the host machine

# Cloudflare Access service token
DATASMITH_CF_ACCESS_CLIENT_ID=abc123.access
DATASMITH_CF_ACCESS_CLIENT_SECRET=long-secret-string
```

The `SUPABASE_KEY` is the same service-role key used on the host — it is
not a Cloudflare credential.

### Verify connectivity

```bash
fc-data --preflight
```

The Supabase connection check should show `[OK]`. If it fails:

- **403 Forbidden** — the CF Access headers are missing or the service
  token is invalid. Double-check `DATASMITH_CF_ACCESS_CLIENT_ID` and
  `DATASMITH_CF_ACCESS_CLIENT_SECRET`.
- **502 Bad Gateway** — `cloudflared` is not running on the host machine
  or Supabase is down. SSH into the host and check
  `systemctl status cloudflared` and `supabase status`.
- **Connection refused** — DNS is not resolving. Verify
  `cloudflared tunnel route dns` was run and the CNAME exists in your
  Cloudflare DNS dashboard.

## How it works in the code

When `DATASMITH_CF_ACCESS_CLIENT_ID` and `DATASMITH_CF_ACCESS_CLIENT_SECRET`
are both set, `datasmith.utils.db` automatically injects the
`CF-Access-Client-Id` and `CF-Access-Client-Secret` headers into every
Supabase client request via `ClientOptions`. No other code changes are
needed — every call to `get_client()` or `get_async_client()` picks up
the headers transparently.

When neither variable is set (the default for local development), no
extra headers are added and behavior is identical to before.

## Public read-only access

Four tables are exposed for **unauthenticated read-only** access via
the Supabase **anon key** (called "Publishable" key in `supabase status`).
This is intended for public-facing websites that need to display dataset
statistics without any server-side credentials.

### Tables with public SELECT policies

| Table | What's exposed |
|-------|---------------|
| `repositories` | Repository metadata (owner, repo, stars, description) |
| `pull_requests` | PR metadata, classification, difficulty, patches |
| `candidate_containers` | Successful build scripts per SHA |
| `harbor_runs` | Benchmark speedup results per container |

All other tables remain locked down — the anon key cannot read them.

### How it works

Row Level Security (RLS) is enabled on these tables with a `public_read`
policy that allows `SELECT` for the `anon` role. The service-role key
(used by the pipeline) bypasses RLS entirely, so active processes are
unaffected.

The anon key is safe to embed in client-side code — it can only read
the 4 tables above. Writes are rejected by RLS:

```
HTTP 403: new row violates row-level security policy
```

### Frontend usage

```js
const SUPABASE_URL = "https://api.formulacode.org";
const ANON_KEY = "sb_publishable_...";  // from `supabase status`

const res = await fetch(
  `${SUPABASE_URL}/rest/v1/repositories?select=owner,repo,stars&order=stars.desc&limit=20`,
  {
    headers: {
      "apikey": ANON_KEY,
      "Authorization": `Bearer ${ANON_KEY}`
    }
  }
);
```

### Migration

The RLS policies are defined in `supabase/migrations/00012_public_read_rls.sql`.
To apply on a fresh instance:

```bash
docker exec supabase_db_<project> psql -U postgres -d postgres \
  -c "$(cat supabase/migrations/00012_public_read_rls.sql)"
```

## Makefile Targets

```bash
make db-tunnel        # Expose Supabase PostgREST API via Cloudflare Tunnel
```
