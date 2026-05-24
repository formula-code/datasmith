-- Per-finding tables backing the paper-figure summaries on
-- api.formulacode.org. See formula-code/fc-eval#19.
--
-- Rows are refreshed by analysis/export_website_findings.py in the
-- formula-code/fc-eval repo (which reads cached Task objects, runs the
-- existing nb_utils helpers, and upserts here via the service-role key).
--
-- Each table is anon-readable through PostgREST at:
--   GET /rest/v1/findings_<name>
-- and pairs with findings_metadata which carries the envelope fields the
-- website expects: {_source, _generated_at, axis_metadata, row_count}.
--
-- Schema names align with the canonical scaffolds in
-- formula-code/formula-code.github.io:src/data/findings/f{1..7}_*.json
-- (e.g. rp_rank not rp_rank_adv; advantage_norm not normalized_advantage;
-- speedup_geomean not mean_speedup; worst_workload_speedup not
-- worst_workload_spd; advantage_weighted not cost_weighted_adv). Per-task
-- rows use (owner, repo, issue_number) to match pull_requests / harbor_runs.

-- Envelope sidecar — one row per finding.
CREATE TABLE IF NOT EXISTS findings_metadata (
    finding_name    TEXT PRIMARY KEY,         -- 'f1_leaderboard' | ...
    source          TEXT NOT NULL,            -- notebook/script reference
    paper_artifact  TEXT,                     -- 'Table 1', 'Figure 3', ...
    arxiv_url       TEXT,
    generated_at    TIMESTAMPTZ NOT NULL,
    axis_metadata   JSONB,                    -- levels / tags / quintiles / bins / cutoffs
    row_count       INT NOT NULL,
    notes           TEXT
);

-- f1: Global Leaderboard (Table 1).
CREATE TABLE IF NOT EXISTS findings_global_leaderboard (
    agent             TEXT NOT NULL,          -- display label, e.g. 'Terminus 2'
    model             TEXT NOT NULL,          -- display label, e.g. 'Claude 4.0 Sonnet'
    rp_rank           INT,                    -- Ranked-Pairs rank; 0 reserved for oracle baseline
    advantage         DOUBLE PRECISION,
    advantage_norm    DOUBLE PRECISION,
    speedup_geomean   DOUBLE PRECISION,
    is_baseline       BOOLEAN DEFAULT FALSE,  -- TRUE for the Human Expert / oracle row
    PRIMARY KEY (agent, model)
);

-- f2: Stratified Advantage (Figure 3). Note: scaffold exposes overall + level2/3/4.
-- We keep level1 too so downstream consumers that want full hierarchy can use it,
-- but the website's slope chart reads from overall/level2/level3/level4 only.
CREATE TABLE IF NOT EXISTS findings_stratified_advantage (
    agent             TEXT NOT NULL,
    model             TEXT NOT NULL,
    overall           DOUBLE PRECISION,       -- agent_advantage (level 4, all benchmarks)
    level1            DOUBLE PRECISION,       -- agent_advantage_level1 (module)
    level2            DOUBLE PRECISION,       -- agent_advantage_level2 (class)
    level3            DOUBLE PRECISION,       -- agent_advantage_level3 (function)
    level4            DOUBLE PRECISION,       -- agent_advantage_level4 (params; same as overall)
    PRIMARY KEY (agent, model)
);

-- f3: Per-Tag Advantage (Table 2). Tag keys match scaffold taxonomy
-- (parallelization, batching, io, caching, algorithmic, data_structure,
-- reduce_work, approximation, scale, db, higher_level, micro, lower_level,
-- uncategorized).
CREATE TABLE IF NOT EXISTS findings_tag_advantage (
    agent      TEXT NOT NULL,
    model      TEXT NOT NULL,
    tag        TEXT NOT NULL,
    advantage  DOUBLE PRECISION,
    speedup    DOUBLE PRECISION,
    n_tasks    INT,
    PRIMARY KEY (agent, model, tag)
);

-- f4: Repo Quintiles by stars (Table 3). Quintile bin edges live in
-- findings_metadata.axis_metadata.quintile_ranges so the website doesn't have
-- to inspect rows to render axis labels.
CREATE TABLE IF NOT EXISTS findings_repo_quintiles (
    agent      TEXT NOT NULL,
    model      TEXT NOT NULL,
    quintile   TEXT NOT NULL,                 -- 'Q1' | 'Q2' | 'Q3' | 'Q4' | 'Q5'
    advantage  DOUBLE PRECISION,
    speedup    DOUBLE PRECISION,
    n_tasks    INT,
    PRIMARY KEY (agent, model, quintile)
);

-- f5: Cost-Performance Pareto (Figure 4 / Table 10).
CREATE TABLE IF NOT EXISTS findings_cost_pareto (
    agent               TEXT NOT NULL,
    model               TEXT NOT NULL,
    cost_usd_per_task   DOUBLE PRECISION,
    advantage           DOUBLE PRECISION,
    advantage_weighted  DOUBLE PRECISION,     -- advantage / cost_usd_per_task
    is_pareto           BOOLEAN,
    PRIMARY KEY (agent, model)
);

-- f6: Workload Tradeoff (Figure 5). One row per (agent, model, task).
-- The expert (oracle) cluster is flagged via is_expert=TRUE.
CREATE TABLE IF NOT EXISTS findings_workload_tradeoff (
    agent                     TEXT NOT NULL,
    model                     TEXT NOT NULL,
    owner                     TEXT NOT NULL,
    repo                      TEXT NOT NULL,
    issue_number              INT NOT NULL,
    global_speedup            DOUBLE PRECISION,
    worst_workload_speedup    DOUBLE PRECISION,
    is_expert                 BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (agent, model, owner, repo, issue_number)
);

-- f7: Temporal Generalization (Table 4 / Figure 1). 5 bins around each model's
-- knowledge cutoff: pre6 | pre3 | cutoff | post3 | post6.
CREATE TABLE IF NOT EXISTS findings_temporal_generalization (
    model             TEXT NOT NULL,
    bin               TEXT NOT NULL,
    knowledge_cutoff  DATE,
    speedup           DOUBLE PRECISION,
    advantage         DOUBLE PRECISION,
    n_tasks           INT,
    PRIMARY KEY (model, bin)
);

-- ---- RLS + grants (mirrors 00012_public_read_rls + 00015_revoke_anon_select) ----
-- Pattern: enable RLS, add a public_read SELECT policy for the anon role, then
-- explicitly GRANT SELECT since 00015 revoked the schema-wide anon grant.

ALTER TABLE findings_metadata                ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_global_leaderboard      ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_stratified_advantage    ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_tag_advantage           ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_repo_quintiles          ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_cost_pareto             ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_workload_tradeoff       ENABLE ROW LEVEL SECURITY;
ALTER TABLE findings_temporal_generalization ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public_read" ON findings_metadata                FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_global_leaderboard      FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_stratified_advantage    FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_tag_advantage           FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_repo_quintiles          FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_cost_pareto             FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_workload_tradeoff       FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON findings_temporal_generalization FOR SELECT TO anon USING (true);

GRANT SELECT ON
    findings_metadata,
    findings_global_leaderboard,
    findings_stratified_advantage,
    findings_tag_advantage,
    findings_repo_quintiles,
    findings_cost_pareto,
    findings_workload_tradeoff,
    findings_temporal_generalization
TO anon;

-- Helpful index for the workload-tradeoff page if it ever filters by repo.
CREATE INDEX IF NOT EXISTS idx_findings_workload_tradeoff_repo
    ON findings_workload_tradeoff (owner, repo);
