-- 00023_findings_workload_tradeoff_per_config.sql
-- Replace findings_workload_tradeoff with a per-config schema.
--
-- Context: the original schema in 00021 stored one row per
-- (agent, model, owner, repo, issue_number). Paper Figure 5 is 8 dots, one
-- per (agent, model), and the arithmetic-mean aggregation the website would
-- compute from per-task rows drifted ~0.04 above the paper values.
--
-- The new schema mirrors the `mean_speedup` semantics of
-- analysis.task.compute_leaderboard (per-task gmean -> arithmetic mean across
-- tasks, with failed non-baseline tasks imputed at speedup=1.0), which matches
-- the paper Table 1 / Figure 5 numbers.

DROP TABLE IF EXISTS findings_workload_tradeoff;

CREATE TABLE findings_workload_tradeoff (
    agent                   TEXT NOT NULL,
    model                   TEXT NOT NULL,
    global_speedup          DOUBLE PRECISION,
    worst_workload_speedup  DOUBLE PRECISION,
    n_tasks                 INT,
    is_expert               BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (agent, model)
);

ALTER TABLE findings_workload_tradeoff ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON findings_workload_tradeoff
    FOR SELECT TO anon USING (true);

GRANT SELECT ON findings_workload_tradeoff TO anon;
