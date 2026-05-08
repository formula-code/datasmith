-- Add detected_cpu_model to lsv_baseline_cache PK.
--
-- Background: 00018 dropped cpu_model because it was sourced from the
-- runner's /proc/cpuinfo (the orchestrator host), which over-keyed the
-- cache and tanked hit rate. This migration re-introduces a CPU-model
-- column, but it's now populated from inside the *sandbox* by lsv_init.py
-- reading /proc/cpuinfo at trial start. That value reflects the physical
-- machine Daytona scheduled the sandbox onto.
--
-- Why we need this: scripts/probe_daytona_cpu.py confirmed that within
-- Daytona's `default` machine_class a single (cpu_count, mem_bytes)
-- request lands on multiple distinct EPYC SKUs (9254 / 9334 / 9354P).
-- Without keying on the detected CPU, baselines captured on a 9354P get
-- replayed on a 9254 and pollute the speedup signal.
--
-- Existing rows backfill to '' (the column NOT NULL default). The first
-- post-migration writeback per (env, host, detected_cpu) populates the
-- real value and starts a correctly-keyed row; the legacy '' rows decay
-- naturally as their PRs get re-run.

BEGIN;

ALTER TABLE lsv_baseline_cache DROP CONSTRAINT lsv_baseline_cache_pkey;

ALTER TABLE lsv_baseline_cache
  ADD COLUMN detected_cpu_model TEXT NOT NULL DEFAULT '';

ALTER TABLE lsv_baseline_cache
  ADD CONSTRAINT lsv_baseline_cache_pkey PRIMARY KEY
  (owner, repo, issue_number, env, container_name, image_digest,
   machine_class, docker_host_id, cpu_count, mem_bytes, detected_cpu_model);

GRANT SELECT ON lsv_baseline_cache TO grafana_ro;

COMMIT;
