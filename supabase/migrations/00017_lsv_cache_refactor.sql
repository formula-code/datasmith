-- Tighten the LSV cache layout introduced in 00016:
--
--   * `lsv_dep_cache` was a one-row-per-PR pointer table; collapse it into a
--     `pull_requests.lsv_deps_db_url` column. Mirrors the existing
--     `baseline_run_id` / `snapshot_storage_url` columns there.
--   * `lsv_baseline_cache.resource_signature` (a hash) becomes raw resource
--     columns so cache lookups are explicit, granularity is tunable later
--     without re-hashing, and joins/inspection don't require unhashing.
--   * `harbor_runs.artifacts_storage_url` records the per-trial run-artifacts
--     tarball path (replaces what the legacy adapter wrote into
--     `runs.payload.patch_storage_url` against a separate Supabase project).

BEGIN;

-- (1) Move per-PR deps DB URL onto pull_requests
ALTER TABLE pull_requests ADD COLUMN lsv_deps_db_url TEXT;

UPDATE pull_requests pr
SET lsv_deps_db_url = ldc.deps_db_url
FROM lsv_dep_cache ldc
WHERE pr.owner = ldc.owner
  AND pr.repo = ldc.repo
  AND pr.issue_number = ldc.issue_number;

DROP TABLE lsv_dep_cache;

-- (2) Replace resource_signature/signature_payload with raw resource columns.
-- DEFAULT '' on text columns and DEFAULT 0 on numeric columns lets the new
-- PK include every resource dimension without requiring callers to provide
-- a value when a particular dimension is meaningless for their environment
-- (e.g. machine_class=='' for docker, cpu_count==0 for daytona).
ALTER TABLE lsv_baseline_cache
  ADD COLUMN env             TEXT   NOT NULL DEFAULT '',
  ADD COLUMN container_name  TEXT   NOT NULL DEFAULT '',
  ADD COLUMN image_digest    TEXT   NOT NULL DEFAULT '',
  ADD COLUMN machine_class   TEXT   NOT NULL DEFAULT '',
  ADD COLUMN docker_host_id  TEXT   NOT NULL DEFAULT '',
  ADD COLUMN cpu_model       TEXT   NOT NULL DEFAULT '',
  ADD COLUMN cpu_count       INT    NOT NULL DEFAULT 0,
  ADD COLUMN mem_bytes       BIGINT NOT NULL DEFAULT 0;

UPDATE lsv_baseline_cache SET
  env             = COALESCE(signature_payload->>'env',           ''),
  container_name  = COALESCE(signature_payload->>'container_name',''),
  image_digest    = COALESCE(signature_payload->>'image_digest',  ''),
  machine_class   = COALESCE(signature_payload->>'machine_class', ''),
  docker_host_id  = COALESCE(signature_payload->>'docker_host_id',''),
  cpu_model       = COALESCE(signature_payload->>'cpu_model',     ''),
  cpu_count       = COALESCE((signature_payload->>'cpu_count')::INT,    0),
  mem_bytes       = COALESCE((signature_payload->>'mem_bytes')::BIGINT, 0);

ALTER TABLE lsv_baseline_cache DROP CONSTRAINT lsv_baseline_cache_pkey;
ALTER TABLE lsv_baseline_cache DROP COLUMN resource_signature;
ALTER TABLE lsv_baseline_cache DROP COLUMN signature_payload;

ALTER TABLE lsv_baseline_cache
  ADD CONSTRAINT lsv_baseline_cache_pkey PRIMARY KEY
  (owner, repo, issue_number, env, container_name, image_digest,
   machine_class, docker_host_id, cpu_model, cpu_count, mem_bytes);

-- (3) Per-trial artifact tarball URL on harbor_runs
ALTER TABLE harbor_runs ADD COLUMN artifacts_storage_url TEXT;

-- Grafana SELECT survives ALTERs but make explicit since 00009 default
-- privileges only apply to objects created after the GRANT.
GRANT SELECT ON lsv_baseline_cache TO grafana_ro;
GRANT SELECT ON harbor_runs TO grafana_ro;

COMMIT;
