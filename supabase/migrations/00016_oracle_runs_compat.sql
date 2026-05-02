-- 00016_oracle_runs_compat.sql
--
-- Folds the legacy harbor-fc-adapter `tasks` / `runs` schema into datasmith.
-- Adds:
--   * pull_requests.baseline_run_id        — UUID FK to the oracle harbor_runs row
--   * pull_requests.snapshot_storage_url   — Supabase Storage URL of the oracle snapshot
--   * harbor_runs.model_name               — agent's underlying model identifier
--   * harbor_runs.model_agent_signature    — "<agent>:<model>" composite identifier
--   * harbor_runs.pytest_success_ratio     — (passed) / (passed + failed + error)
--
-- Plus two cache tables for LSV `initialize_diffcheck` short-circuit:
--   * lsv_dep_cache       — per-PR deps DB Storage URL (resource-independent)
--   * lsv_baseline_cache  — per-(PR, resource_signature) baseline timings JSONB
--

ALTER TABLE pull_requests
  ADD COLUMN IF NOT EXISTS baseline_run_id UUID REFERENCES harbor_runs(run_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS snapshot_storage_url TEXT;

ALTER TABLE harbor_runs
  ADD COLUMN IF NOT EXISTS model_name TEXT,
  ADD COLUMN IF NOT EXISTS model_agent_signature TEXT,
  ADD COLUMN IF NOT EXISTS pytest_success_ratio DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS lsv_dep_cache (
  owner          TEXT NOT NULL,
  repo           TEXT NOT NULL,
  issue_number   INT  NOT NULL,
  deps_db_url    TEXT NOT NULL,
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (owner, repo, issue_number)
);

CREATE TABLE IF NOT EXISTS lsv_baseline_cache (
  owner               TEXT  NOT NULL,
  repo                TEXT  NOT NULL,
  issue_number        INT   NOT NULL,
  resource_signature  TEXT  NOT NULL,
  baselines           JSONB NOT NULL,
  signature_payload   JSONB NOT NULL,
  created_at          TIMESTAMPTZ DEFAULT NOW(),
  updated_at          TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (owner, repo, issue_number, resource_signature)
);

-- grafana_ro already has SELECT on all public tables via the default-privileges grant in 00009,
-- but those defaults only apply to tables created *after* the grant ran.  Re-grant explicitly so
-- the dashboards can read the new cache tables.
GRANT SELECT ON lsv_dep_cache, lsv_baseline_cache TO grafana_ro;

-- Anon role stays restricted per 00015: lsv_* are NOT in the public-anon allowlist.  No grants
-- to anon here on purpose; pipeline writes happen via service_role which bypasses RLS.
