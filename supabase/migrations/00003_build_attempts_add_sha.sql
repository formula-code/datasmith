-- Add sha column to build_attempts so each row tracks which commit
-- the build script was synthesized for.
ALTER TABLE build_attempts ADD COLUMN IF NOT EXISTS sha TEXT;

-- Index for _find_similar() lookups: WHERE owner=? AND repo=? AND ok=True
CREATE INDEX IF NOT EXISTS idx_build_attempts_repo_ok
    ON build_attempts (owner, repo) WHERE ok = TRUE;
