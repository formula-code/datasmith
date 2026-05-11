-- Per-benchmark speedup measurements from terminal-bench eval runs.
-- Each row is one (benchmark, agent, run) triple: how an LLM agent performed
-- on a single ASV benchmark within a given task (owner/repo/issue), relative
-- to the human-expert oracle.
--
-- Source: terminal-bench `runs/<run_id>/results.json`, field
-- `parser_extra_metrics.per_benchmark_speedups_by_agent[agent:model][benchmark]`.
-- The human expert is not stored as its own agent row; its baseline lives in
-- `oracle_speedup_vs_nop` on every agent row.

CREATE TABLE IF NOT EXISTS benchmark_information (
    id                      BIGSERIAL PRIMARY KEY,
    measured_at             TIMESTAMPTZ NOT NULL,    -- run_metadata.start_time
    run_id                  TEXT NOT NULL,           -- e.g. "2026-01-05__09-47-55"
    owner                   TEXT NOT NULL,
    repo                    TEXT NOT NULL,
    issue_number            INT  NOT NULL,
    benchmark_name          TEXT NOT NULL,           -- "benchmarks.ConstructorsSuite.time_point"
    agent_name              TEXT NOT NULL,           -- "openhands", "terminus-2", "oracle", ...
    model_name              TEXT,                    -- full model id; NULL for oracle/human
    speedup                 DOUBLE PRECISION NOT NULL,  -- human_time / agent_time (1.0 = parity)
    agent_speedup_vs_nop    DOUBLE PRECISION,        -- raw agent/nop from parser
    oracle_speedup_vs_nop   DOUBLE PRECISION,        -- raw oracle/nop from parser
    advantage               DOUBLE PRECISION,        -- parser `advantage` field
    significant             BOOLEAN,                 -- parser `significant` field
    commit_hash             TEXT,
    trial_started_at        TIMESTAMPTZ,
    trial_ended_at          TIMESTAMPTZ,
    raw_payload             JSONB,                   -- full per-benchmark dict verbatim
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT benchmark_information_unique
        UNIQUE (run_id, owner, repo, issue_number, benchmark_name, agent_name, model_name)
);

CREATE INDEX IF NOT EXISTS idx_benchmark_information_repo
    ON benchmark_information (owner, repo, issue_number);

CREATE INDEX IF NOT EXISTS idx_benchmark_information_agent
    ON benchmark_information (agent_name, model_name);

CREATE INDEX IF NOT EXISTS idx_benchmark_information_speedup
    ON benchmark_information (speedup);

CREATE INDEX IF NOT EXISTS idx_benchmark_information_run
    ON benchmark_information (run_id);

CREATE INDEX IF NOT EXISTS idx_benchmark_information_measured_at
    ON benchmark_information (measured_at);

GRANT ALL ON benchmark_information TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON SEQUENCE benchmark_information_id_seq TO anon, authenticated, service_role;

-- Public read access (mirrors 00012_public_read_rls.sql / 00015_revoke_anon_select.sql).
-- Anon role can SELECT but not write; service-role bypasses RLS.
ALTER TABLE benchmark_information ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_read ON benchmark_information;
CREATE POLICY public_read ON benchmark_information
    FOR SELECT
    TO anon
    USING (true);

REVOKE INSERT, UPDATE, DELETE ON benchmark_information FROM anon;
