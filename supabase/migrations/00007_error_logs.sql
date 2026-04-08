-- Stores per-attempt synthesis results (success and failure).
-- Populated by Synthesizer._log_attempt() during stage 6 (synthesize_images).
-- Captures agent output, failure stage/return code, and error messages
-- so synthesis failures can be diagnosed without re-running.

CREATE TABLE IF NOT EXISTS error_logs (
    id                  SERIAL PRIMARY KEY,
    owner               TEXT NOT NULL,
    repo                TEXT NOT NULL,
    sha                 TEXT NOT NULL,
    issue_number        INT  NOT NULL,
    attempt_index       INT  NOT NULL DEFAULT 0,
    agent_name          TEXT,
    success             BOOLEAN NOT NULL DEFAULT FALSE,
    duration_s          FLOAT,
    failure_stage       TEXT,
    failure_return_code INT,
    error_message       TEXT,
    agent_output        TEXT,
    files_changed       JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_error_logs_repo
    ON error_logs (owner, repo);

CREATE INDEX IF NOT EXISTS idx_error_logs_success
    ON error_logs (success);

CREATE INDEX IF NOT EXISTS idx_error_logs_failure_stage
    ON error_logs (failure_stage)
    WHERE NOT success;
