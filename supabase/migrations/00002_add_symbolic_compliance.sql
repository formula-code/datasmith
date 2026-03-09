-- Add symbolic attribute-compliance column to pull_requests.
-- True = PR passes all attribute compliance filters (message, file-level, patch size).
-- False = PR fails at least one filter.
-- NULL = not yet evaluated.

ALTER TABLE pull_requests
    ADD COLUMN IF NOT EXISTS is_performance_commit_symbolic BOOLEAN;
