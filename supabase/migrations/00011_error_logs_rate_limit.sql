-- Add rate-limit reset tracking to error_logs.
--
-- Stage 6 (synthesize_images) can burn through the weekly Codex/Claude budget
-- and start hitting usage-limit errors. We now detect those errors in the
-- raw agent_output, stamp failure_stage='rate_limited', and record the
-- reset timestamp here so the runner can pause until the budget resets.

ALTER TABLE error_logs
    ADD COLUMN IF NOT EXISTS rate_limit_reset_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_error_logs_rate_limit_reset_at
    ON error_logs (rate_limit_reset_at)
    WHERE rate_limit_reset_at IS NOT NULL;
