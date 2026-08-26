-- Task-keyed cache of the LSV coverage SURVEY (the `.lightspeed_deps.db`
-- SQLite file, baselines stripped), so stage 7 (harbor_healthcheck) stops
-- re-running the survey pass on every trial.
--
-- WHY THIS EXISTS, AND WHY IT IS SEPARATE FROM lsv_baseline_cache (00031)
--
-- `initialize_diffcheck` runs two passes: a coverage SURVEY (which source file
-- each benchmark touches) and a BASELINE TIMING pass. It short-circuits BOTH
-- only when the deps DB already holds the survey AND a baseline row for every
-- benchmark (verified in the LSV fork, asv/contrib/lightspeed/session.py) --
-- and `load_baselines` REQUIRES the surveyed deps DB to already exist on disk,
-- so the baseline cache (00031) is useless without the survey staged first.
--
-- The two facts have different lifetimes and different keys, hence two tables:
--   * The SURVEY is resource-INDEPENDENT -- it depends only on the code and the
--     benchmark suite at a given commit, not on the CPU. So it is keyed by the
--     task alone, (owner, repo, issue_number), and one row serves docker and
--     every daytona SKU alike.
--   * The BASELINE timings are resource-DEPENDENT and live in lsv_baseline_cache
--     under an 11-column hardware key.
-- Keeping the multi-MB survey blob in its own table also keeps it off
-- pull_requests and clear of the DATASMITH_LARGE_* unfiltered-read guard.
--
-- TRANSPORT. The blob is a binary SQLite file, stored as BYTEA. Both the
-- in-container writeback (stdlib urllib) and the host-side runner (supabase-py)
-- move it through PostgREST as PostgreSQL's hex form (`\x...`): the writeback
-- POSTs `"\\x" + data.hex()`, the runner reads the `\x`-prefixed string back and
-- `bytes.fromhex`es it. No Storage bucket -- datasmith host-side uses none, and a
-- bucket would add client.storage plumbing and bucket RLS for no gain here.
--
-- The oracle trial writes the survey (baselines DELETEd + `PRAGMA
-- wal_checkpoint(TRUNCATE)` so no per-host timing rows leak across hardware);
-- the runner reads it and bakes it into the next build. A miss anywhere degrades
-- to today's `force=True` full measure. No FK to pull_requests: advisory cache,
-- also written from the stage-6 measure.sh context where no PR row is
-- guaranteed. A missing task simply never gets a hit.

CREATE TABLE IF NOT EXISTS lsv_deps_cache (
    owner         TEXT  NOT NULL,
    repo          TEXT  NOT NULL,
    issue_number  INT   NOT NULL,
    deps_db       BYTEA NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (owner, repo, issue_number)
);

-- Read-only role for Grafana, following 00009/00031. No anon grant: internal
-- pipeline state, private by default per 00015. Pipeline writes use the
-- service-role key, which bypasses grants and RLS.
GRANT SELECT ON lsv_deps_cache TO grafana_ro;
