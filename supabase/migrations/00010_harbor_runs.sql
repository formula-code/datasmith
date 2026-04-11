-- Stores per-execution results from running synthesized containers through
-- the Harbor eval framework (stage 7: harbor_healthcheck). Each row captures
-- one Harbor oracle trial: speedup metrics parsed from reward.json and the
-- coarse run state.
--
-- One-to-many with candidate_containers: a given (owner, repo, sha) can be
-- benchmarked many times (re-runs across harbor revisions, environment
-- changes, etc.). Publish stage filters pull_requests by joining here and
-- dropping anything whose best max_speedup is below 1.05.

CREATE TABLE IF NOT EXISTS harbor_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner           TEXT NOT NULL,
    repo            TEXT NOT NULL,
    sha             TEXT NOT NULL,
    issue_number    INT,
    container_name  TEXT,
    environment     TEXT NOT NULL,          -- 'docker' | 'daytona'
    agent_name      TEXT NOT NULL DEFAULT 'oracle',
    status          TEXT NOT NULL,          -- 'success' | 'failed' | 'timeout' | 'no_benchmarks'
    max_speedup     DOUBLE PRECISION,       -- max over per_benchmark_speedups
    geomean_speedup DOUBLE PRECISION,       -- lsv_mean_speedup from harbor parser.py
    n_benchmarks    INT,
    wallclock_sec   DOUBLE PRECISION,
    reward_payload  JSONB,                  -- full reward.json verbatim
    error_message   TEXT,
    ran_at          TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (owner, repo, sha)
        REFERENCES candidate_containers (owner, repo, sha)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_harbor_runs_container
    ON harbor_runs (owner, repo, sha);

CREATE INDEX IF NOT EXISTS idx_harbor_runs_max_speedup
    ON harbor_runs (max_speedup);

CREATE INDEX IF NOT EXISTS idx_harbor_runs_status
    ON harbor_runs (status);
