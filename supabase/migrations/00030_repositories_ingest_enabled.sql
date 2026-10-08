-- supabase/migrations/00030_repositories_ingest_enabled.sql
--
-- Let a repository be taken out of ingestion without deleting its history.
--
-- Six repositories have produced ZERO rows in candidate_containers across 430
-- stage-6 attempts, measured 2026-08-25:
--
--   PostHog/posthog            35,870 PRs   4,582 perf   73 attempts   0 containers
--   man-group/ArcticDB          1,783 PRs     112 perf   88 attempts   0 containers
--   IntelPython/dpctl           1,679 PRs      77 perf   74 attempts   0 containers
--   tardis-sn/tardis            1,452 PRs      32 perf   86 attempts   0 containers
--   not522/ac-library-python       50 PRs       6 perf   57 attempts   0 containers
--   mongodb-labs/mongo-arrow      301 PRs       4 perf   52 attempts   0 containers
--
-- Together they are 15.2% of the corpus and 28.6% of everything ever marked
-- is_performance_commit, and they consume stage 3's REST budget, stage 3's LLM
-- calls, stage 4 resolution and stage 5 rendering on the way to producing
-- nothing.  The other 134 repositories turned 9,330 attempts into 1,875
-- containers, so the per-attempt success rate is about 19%.  Zero from 430 at
-- that rate is not bad luck.
--
-- What this is NOT evidence of: that agent aborts are their problem.  62% of
-- stage-6 failures at repositories that DO produce containers are aborts, and
-- 66% at these six -- the rate is the same either way.  Aborts are a
-- pipeline-wide defect worth more than this exclusion; see the run report at
-- logs/ingestion-2026-08/RUN-REPORT.md.
--
-- The flag is a column rather than an env knob because it carries a reason and
-- a date, an operator on another machine sees the same state, and re-enabling a
-- repository after the stage-6 abort rate is fixed is one UPDATE rather than a
-- code change.  Default TRUE, so an unknown or newly discovered repository is
-- ingested exactly as before.
--
-- Numbered 00030: 00029 is the highest present on any branch of this
-- repository.  00024 remains absent everywhere -- as 00025's header records, it
-- was authored in a separate working tree and never landed here.
--
-- GRANTS NOTHING TO anon.  repositories is anon-readable through 00012/00015
-- already; this adds columns and changes no privilege.

alter table repositories
  add column if not exists ingest_enabled boolean not null default true;

alter table repositories
  add column if not exists ingest_excluded_reason text;

comment on column repositories.ingest_enabled is
  'False takes the repository out of stages 2 and 3. Existing rows are kept.';
comment on column repositories.ingest_excluded_reason is
  'Why ingestion is disabled, so the decision can be re-examined rather than rediscovered.';

-- Partial index: the predicate is "the enabled ones", which is nearly every row
-- today, so index the exclusions instead -- that set is small and stays small.
create index if not exists idx_repositories_ingest_disabled
  on repositories (owner, repo)
  where not ingest_enabled;

update repositories
   set ingest_enabled = false,
       ingest_excluded_reason =
         '0 containers from 430 stage-6 attempts (measured 2026-08-25); '
         'per-attempt success elsewhere is ~19%. Re-enable once the stage-6 '
         'agent-abort rate (62% pipeline-wide) is addressed.'
 where (owner, repo) in (
   ('PostHog', 'posthog'),
   ('man-group', 'ArcticDB'),
   ('IntelPython', 'dpctl'),
   ('tardis-sn', 'tardis'),
   ('not522', 'ac-library-python'),
   ('mongodb-labs', 'mongo-arrow')
 );
