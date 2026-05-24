-- Map FormulaCode's legacy on-disk task identifiers (the strings under
-- `analysis/tasks.txt` in formula-code/fc-eval, e.g. `pandas_dev-pandas_3`)
-- to the canonical (owner, repo, issue_number) identity used everywhere
-- else in the public API (`pull_requests`, `harbor_runs`,
-- `findings_workload_tradeoff`, etc.).
--
-- Why this exists: the legacy task-id format is `{owner_sanitized}_{repo_
-- sanitized}_{seq_num}` where `seq_num` is a per-run sequence number (NOT
-- the GitHub issue/PR number) and the sanitization rule for `_` vs `-` is
-- inconsistent across rows in `tasks.txt`. Anyone consuming an
-- `analysis/tasks.txt`-style id needs this table to recover the PR's
-- actual identity and commit SHA.
--
-- See formula-code/fc-eval#19 for context; rows are populated by the
-- `task_id_map` builder in `analysis/export_website_findings.py`.

CREATE TABLE IF NOT EXISTS task_id_map (
    legacy_task_id          TEXT PRIMARY KEY,        -- e.g. 'pandas_dev-pandas_3'
    canonical_task_id       TEXT NOT NULL,           -- '{owner}_{repo}_{issue_number}'
    owner                   TEXT NOT NULL,
    repo                    TEXT NOT NULL,
    issue_number            INT  NOT NULL,
    pr_merge_commit_sha     TEXT,
    pr_base_sha             TEXT,
    FOREIGN KEY (owner, repo, issue_number)
        REFERENCES pull_requests (owner, repo, issue_number)
);

CREATE INDEX IF NOT EXISTS idx_task_id_map_canonical
    ON task_id_map (canonical_task_id);
CREATE INDEX IF NOT EXISTS idx_task_id_map_pr
    ON task_id_map (owner, repo, issue_number);

-- RLS + anon grant (same pattern as 00021_findings_tables.sql).
ALTER TABLE task_id_map ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public_read" ON task_id_map FOR SELECT TO anon USING (true);
GRANT SELECT ON task_id_map TO anon;
