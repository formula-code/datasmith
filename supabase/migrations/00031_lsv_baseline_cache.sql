-- Resource-keyed cache of LSV baseline timings, so stage 7 (harbor_healthcheck)
-- stops re-measuring the base-commit baseline on every trial.
--
-- WHY THIS EXISTS
--
-- Stage 7 measures a speedup with LSV (`asv.contrib.lightspeed`). Every trial
-- calls `initialize_diffcheck`, which runs two passes: a coverage SURVEY (which
-- source files each benchmark touches) and a BASELINE TIMING pass (how fast each
-- impacted benchmark runs at the base commit). Today `lsv_init.py` passes
-- `force=True`, so both passes run from scratch on every trial -- and a task is
-- benchmarked many times (oracle re-runs x agents x models x concurrency). The
-- timing pass dominates that cost.
--
-- `initialize_diffcheck(force=False)` short-circuits both passes when the deps DB
-- already holds the survey AND a baseline row for every benchmark (verified in
-- the LSV fork, asv/contrib/lightspeed/session.py). This table is the durable,
-- cross-trial home for those baseline rows. The oracle trial populates it; every
-- trial reads it. `session.export_baselines()` produces the exact JSON stored in
-- `baselines` -> `{benchmark_id: {median, ci_99_a, ci_99_b, q_25, q_75, repeat,
-- number}}`.
--
-- WHY THE KEY IS THIS WIDE
--
-- A baseline timing is only valid on the hardware it was measured on: replaying a
-- fast-CPU baseline against a slow-CPU patched run fabricates a speedup and
-- silently corrupts the signal. The primary key therefore pins every resource
-- fact that moves a timing:
--   * (owner, repo, issue_number)          -- the task
--   * env                                  -- 'docker' | 'daytona'
--   * container_name, image_digest         -- the exact image (a rebuild changes deps -> timings)
--   * machine_class, docker_host_id        -- where it ran (daytona class / docker host)
--   * cpu_count, mem_bytes                 -- the cgroup pins the runner REQUESTED (not /proc)
--   * detected_cpu_model                   -- the CPU model read from /proc/cpuinfo INSIDE
--                                             the sandbox; daytona's 'default' class fans one
--                                             (cpu,mem) request across several EPYC SKUs, so
--                                             this is the only field that distinguishes them.
-- Matching is exact equality on all 11 columns, so an over-coarse key is
-- impossible; the only failure mode is under-hitting, which is safe (the trial
-- falls back to a fresh `force=True` measure). Every key column is NOT NULL with
-- an empty-string / zero default so docker rows (machine_class='') and daytona
-- rows (docker_host_id='') coexist without NULL-PK gaps, and lookup vs upsert can
-- never disagree on '' vs NULL.
--
-- No FK to pull_requests: this cache is advisory and is also written from the
-- stage-6 measure.sh context, where no PR row is guaranteed to exist. A missing
-- task simply never gets a cache hit.

CREATE TABLE IF NOT EXISTS lsv_baseline_cache (
    owner               TEXT   NOT NULL,
    repo                TEXT   NOT NULL,
    issue_number        INT    NOT NULL,
    env                 TEXT   NOT NULL DEFAULT '',   -- 'docker' | 'daytona'
    container_name      TEXT   NOT NULL DEFAULT '',
    image_digest        TEXT   NOT NULL DEFAULT '',
    machine_class       TEXT   NOT NULL DEFAULT '',
    docker_host_id      TEXT   NOT NULL DEFAULT '',
    cpu_count           INT    NOT NULL DEFAULT 0,
    mem_bytes           BIGINT NOT NULL DEFAULT 0,
    detected_cpu_model  TEXT   NOT NULL DEFAULT '',
    baselines           JSONB  NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (owner, repo, issue_number, env, container_name, image_digest,
                 machine_class, docker_host_id, cpu_count, mem_bytes, detected_cpu_model)
);

-- The read path filters by the task first, so index the task prefix. The full PK
-- already backs exact-match lookups on every column.
CREATE INDEX IF NOT EXISTS idx_lsv_baseline_cache_task
    ON lsv_baseline_cache (owner, repo, issue_number);

-- Read-only role for Grafana, following 00009. No anon grant: this is internal
-- pipeline state, private by default per 00015. Pipeline writes use the
-- service-role key, which bypasses grants and RLS.
GRANT SELECT ON lsv_baseline_cache TO grafana_ro;
