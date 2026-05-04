-- Drop cpu_model from lsv_baseline_cache.
--
-- Background: 00017 introduced raw resource columns on lsv_baseline_cache as
-- the cache PK. cpu_model came from /proc/cpuinfo on the runner host, which
-- (a) describes the orchestrator machine, not the Daytona sandbox that
-- actually ran the trial, and (b) varies across Daytona's `default`
-- machine_class because the scheduler picks a physical host per sandbox.
-- Including it in the PK over-keyed the cache and tanked hit rate.
--
-- Resolution: drop cpu_model entirely. The cache key is now keyed on
-- cpu_count + mem_bytes (which Harbor pins uniformly within a machine
-- class), plus docker_host_id for local docker. That gives a stable cache
-- hit pattern within a machine_class and per-developer-host separation
-- for docker. cpu_count + mem_bytes will be read inside the container at
-- lsv_init time (next session's refactor) so they describe the actual
-- sandbox, not the runner host.

BEGIN;

ALTER TABLE lsv_baseline_cache DROP CONSTRAINT lsv_baseline_cache_pkey;
ALTER TABLE lsv_baseline_cache DROP COLUMN cpu_model;

ALTER TABLE lsv_baseline_cache
  ADD CONSTRAINT lsv_baseline_cache_pkey PRIMARY KEY
  (owner, repo, issue_number, env, container_name, image_digest,
   machine_class, docker_host_id, cpu_count, mem_bytes);

GRANT SELECT ON lsv_baseline_cache TO grafana_ro;

COMMIT;
