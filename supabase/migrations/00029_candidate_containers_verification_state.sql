-- Mark every existing container UNVERIFIED.
--
-- 00028 is claimed by `00028_packages_resolution_v2.sql` on another branch, so
-- this takes 00029. Per CLAUDE.md, the skip is recorded rather than silently
-- stepped over.
--
-- WHY THIS EXISTS
--
-- `candidate_containers` holds 1858 rows and none of them has been through the
-- gate that now decides whether a container is honest. Three separate facts,
-- each established on 2026-08-24:
--
--   1. 130 of the 134 repositories in the corpus carry a `builtins.sys`
--      sitecustomize shim injected into site-packages as a workaround for a
--      defect since fixed. It runs inside the measured benchmark process.
--   2. The host image scan (`agents/reflexive/image_integrity.py`) found a
--      previously unknown tamper in that corpus: `apache/arrow` carries a
--      `/usr/local/bin/grep` wrapper that exits 1 rather than matching when
--      the build's secret scan greps for `sb_secret_` or `SUPABASE_`, plus a
--      `sitecustomize.py` in both site-packages and the repository root.
--   3. The scan runs ONLY on the PRODUCE_VERIFY path. A repository the stock
--      template builds returns from TRY_DEFAULT before the gate is reached
--      (`agents/synthesizer.py`: "a repo the stock template already builds
--      never reaches here"), so even a container rebuilt today through the
--      current pipeline has not been image-scanned.
--
-- Point 3 is why this migration marks EVERYTHING, including rows written
-- today. "Built by the current pipeline" and "passed the current gate" are
-- different claims, and only the second one is worth anything.
--
-- `verified_at` stays NULL until a container passes the whole gate: it builds,
-- the host image scan returns no findings, the verifier accepts it, and a
-- manifest is sealed. Nothing sets it yet -- wiring the host scan into the
-- TRY_DEFAULT path is the change that makes 'verified' reachable.
--
-- A NULL `build_manifest` already distinguishes rows built before manifests
-- existed. This column answers a different question: not "was a manifest
-- sealed" but "did anything check the image the manifest describes".

ALTER TABLE candidate_containers
  ADD COLUMN IF NOT EXISTS verification_state text NOT NULL DEFAULT 'unverified',
  ADD COLUMN IF NOT EXISTS verified_at timestamptz;

-- Fail closed: an unrecognised value is a bug, not a new state to be guessed
-- at. Widen this constraint deliberately if a third state is ever needed.
ALTER TABLE candidate_containers
  DROP CONSTRAINT IF EXISTS candidate_containers_verification_state_check;
ALTER TABLE candidate_containers
  ADD CONSTRAINT candidate_containers_verification_state_check
  CHECK (verification_state IN ('unverified', 'verified'));

-- Explicit, not merely defaulted. The DEFAULT covers rows inserted after this
-- runs; this statement covers the 1858 that already exist, and says so in the
-- migration rather than relying on a column default to have been applied.
UPDATE candidate_containers
   SET verification_state = 'unverified',
       verified_at = NULL;

-- The scale run's progress query is "how many are verified", so it is worth an
-- index even at 1858 rows: the corpus is meant to grow by two orders.
CREATE INDEX IF NOT EXISTS idx_candidate_containers_verification_state
    ON candidate_containers (verification_state);

-- No new grant. `candidate_containers` is already anon-readable via 00012 and
-- 00015, and `GRANT SELECT ON <table>` covers columns added later, so the
-- public set is unchanged by design rather than by omission.
