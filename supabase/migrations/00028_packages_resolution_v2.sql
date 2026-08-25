-- Stage 4 resolution redesign: provenance, advisory probe, dropped requirements.
--
-- Number chosen after checking every branch. Taken elsewhere: 00018
-- (origin/lsv-cache-integration), 00024 (a separate working tree), and
-- 00025-00027 (spec/ingestion-window). 00028 is the first free number here.
--
-- Adds no anon privileges whatsoever: `packages` is private and stays private.

-- dropped_requirements is JSON-encoded text, exactly like env_payload beside it,
-- and deliberately not JSONB. The runner hands PostgREST the output of
-- json.dumps, and PostgREST stores a JSON string as a jsonb *scalar string*:
-- jsonb_typeof said 'string' and dropped_requirements->0->>'reason' came back
-- null, so the column would have claimed a structure it does not hold. As text
-- the cast is one step away -- dropped_requirements::jsonb->0->>'reason' -- and
-- the declared type matches what is stored.
ALTER TABLE packages
    ADD COLUMN IF NOT EXISTS dropped_requirements TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS probe_status         TEXT,
    ADD COLUMN IF NOT EXISTS probe_log            TEXT,
    ADD COLUMN IF NOT EXISTS interpreter_source   TEXT,
    ADD COLUMN IF NOT EXISTS cutoff_used          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolver_version     TEXT,
    ADD COLUMN IF NOT EXISTS uv_version           TEXT,
    ADD COLUMN IF NOT EXISTS resolved_at          TIMESTAMPTZ;

-- can_install was declared NOT NULL with no default, so a row that omits it is
-- rejected outright. The redesigned runner omits it deliberately -- probe_status
-- carries the verdict now -- and NULL is the honest value for "this resolver
-- never evaluated the question". A DEFAULT false would forge a verdict instead.
-- The relaxation leaves every existing row, and every writer still setting the
-- column, exactly as they were.
ALTER TABLE packages ALTER COLUMN can_install DROP NOT NULL;

-- Every existing row came from the resolver this redesign replaces, and carries
-- no provenance. Stamp rather than delete, so a re-resolve is a choice and not a
-- prerequisite.
UPDATE packages SET resolver_version = 'legacy' WHERE resolver_version IS NULL;

-- probe_status orders the stage 5 queue, so it is read with a sort, not a filter.
CREATE INDEX IF NOT EXISTS packages_probe_status_idx ON packages (probe_status);

COMMENT ON COLUMN packages.probe_status IS
    'Advisory only. installable | unresolved | failed | empty. Orders the stage 5 queue; excludes nobody.';
COMMENT ON COLUMN packages.dropped_requirements IS
    'JSON-encoded [{req, reason}], like env_payload. Requirements that could not be parsed or resolved, with reasons. Makes a failure diagnosable without a re-run: dropped_requirements::jsonb.';
COMMENT ON COLUMN packages.can_install IS
    'Deprecated. Retained for compatibility; no longer read. Use probe_status.';
