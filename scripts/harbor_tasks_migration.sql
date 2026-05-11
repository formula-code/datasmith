-- ============================================================================
-- Harbor `tasks` table migration: composite (owner, repo, issue_number) PK
-- ============================================================================
--
-- Target: Harbor's Supabase project (see HARBOR_SUPABASE_URL in tokens.env),
-- NOT datasmith's local Supabase. Apply via:
--
--   psql "$HARBOR_DATABASE_URL" -f scripts/harbor_tasks_migration.sql
--
-- This script realigns Harbor's tasks table with the canonical FormulaCode
-- identity tuple `(owner, repo, issue_number)`. The legacy `task_id` column
-- was constructed inconsistently across the codebase (`owner__repo-N` in
-- the publish path, `owner_repo_N` in harbor_adapter), so we extract its
-- components and key the table on the tuple instead. The `task_id` column
-- is retained for one release as an integer mirror of `issue_number`.
--
-- Companion change: scripts/migrate_snapshot_keys.py walks Supabase Storage
-- and renames `snapshots/{old_task_id}/oracle.tar.gz` →
-- `snapshots/{owner}/{repo}/{issue_number}/oracle.tar.gz` once this script
-- has run successfully (so the new tasks rows already carry the triple).
--
-- Re-runnable: every statement is idempotent.

BEGIN;

-- 1. Add the three target columns (nullable initially so the backfill can
--    run before we add NOT NULL constraints).
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS owner        TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS repo         TEXT;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS issue_number INT;

-- 2. Backfill from the legacy task_id string.
--    Two patterns observed in the wild:
--      Format A:  owner__repo-N    (publish/HuggingFace path)
--      Format B:  owner_repo_N     (harbor_adapter path)
--    Format A uses a double-underscore between owner and repo and a dash
--    before the PR number; Format B uses single underscores throughout.
--
--    We try Format A first (more specific), then fall back to Format B.
--    Rows that match neither stay NULL and will be flagged below.
UPDATE tasks
   SET owner        = split_part(task_id, '__', 1),
       repo         = split_part(split_part(task_id, '__', 2), '-', 1),
       issue_number = NULLIF(
                        regexp_replace(
                          split_part(task_id, '__', 2),
                          '^[^-]*-(\d+)$',
                          '\1'
                        ),
                        split_part(task_id, '__', 2)
                      )::INT
 WHERE owner IS NULL
   AND task_id ~ '^[^_]+__[^_-]+-\d+$';

-- Fallback: Format B (owner_repo_N) — but only for rows still unbackfilled.
-- This is brittle because owner and repo can both contain underscores, so
-- we assume the last underscore-separated segment is the integer issue.
UPDATE tasks
   SET owner        = regexp_replace(task_id, '^(.+)_([^_]+)_(\d+)$', '\1'),
       repo         = regexp_replace(task_id, '^(.+)_([^_]+)_(\d+)$', '\2'),
       issue_number = regexp_replace(task_id, '^(.+)_([^_]+)_(\d+)$', '\3')::INT
 WHERE owner IS NULL
   AND task_id ~ '^.+_[^_]+_\d+$';

-- Surface anything that didn't match either pattern.
DO $$
DECLARE n_orphaned INT;
BEGIN
    SELECT count(*) INTO n_orphaned FROM tasks WHERE owner IS NULL;
    IF n_orphaned > 0 THEN
        RAISE WARNING 'tasks: % rows have task_id values that do not parse — leaving them with NULL owner/repo/issue_number; review manually before enforcing NOT NULL', n_orphaned;
    END IF;
END $$;

-- 3. Once the backfill is verified clean, enforce NOT NULL + swap the PK.
--    Skip this block if any orphans remain — operator handles them first.
DO $$
DECLARE n_orphaned INT;
BEGIN
    SELECT count(*) INTO n_orphaned FROM tasks WHERE owner IS NULL OR repo IS NULL OR issue_number IS NULL;
    IF n_orphaned = 0 THEN
        ALTER TABLE tasks ALTER COLUMN owner        SET NOT NULL;
        ALTER TABLE tasks ALTER COLUMN repo         SET NOT NULL;
        ALTER TABLE tasks ALTER COLUMN issue_number SET NOT NULL;

        -- Replace the PK only if it isn't already on the triple.
        IF NOT EXISTS (
            SELECT 1
              FROM pg_constraint
             WHERE conrelid = 'tasks'::regclass
               AND contype = 'p'
               AND pg_get_constraintdef(oid) ILIKE '%(owner, repo, issue_number)%'
        ) THEN
            ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_pkey;
            ALTER TABLE tasks ADD CONSTRAINT tasks_pkey PRIMARY KEY (owner, repo, issue_number);
        END IF;
    ELSE
        RAISE WARNING 'tasks: skipping PK swap — % rows still have NULL owner/repo/issue_number', n_orphaned;
    END IF;
END $$;

-- 4. Reduce the legacy task_id column to an integer mirror of issue_number.
--    Keep the column for one release so any code we missed still reads
--    something sensible. The column is no longer a unique key.
ALTER TABLE tasks ALTER COLUMN task_id DROP NOT NULL;
UPDATE tasks SET task_id = issue_number::TEXT WHERE issue_number IS NOT NULL;
COMMENT ON COLUMN tasks.task_id IS
  'deprecated: equal to issue_number::text — use (owner, repo, issue_number) instead';

COMMIT;
