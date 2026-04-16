-- Ensure runner_progress.updated_at is bumped on every UPDATE.
-- Without this, upserts from BaseRunner only refresh total/completed/failed
-- and updated_at stays pinned at the INSERT default, making it impossible
-- to distinguish live runs from zombies via timestamp.

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS runner_progress_set_updated_at ON runner_progress;
CREATE TRIGGER runner_progress_set_updated_at
    BEFORE UPDATE ON runner_progress
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
