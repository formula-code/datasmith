-- Add `task_id` to candidate_containers so the FormulaCode website can join
-- its rows to the pipeline by a single column instead of the (owner, repo,
-- issue_number) tuple. Per the project-wide convention we adopted alongside
-- this change, `task_id = issue_number` — the (owner, repo) qualifier is
-- always carried in adjacent columns. A generated column avoids any backfill
-- or write-path changes.
--
-- candidate_containers is already public-read (00012, 00015) so no grant
-- changes are needed.

ALTER TABLE candidate_containers
    ADD COLUMN IF NOT EXISTS task_id INT
    GENERATED ALWAYS AS (issue_number) STORED;

CREATE INDEX IF NOT EXISTS idx_candidate_containers_task_id
    ON candidate_containers (task_id);
