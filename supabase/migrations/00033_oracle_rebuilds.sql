-- supabase/migrations/00033_oracle_rebuilds.sql
--
-- Oracle performance as rows: multi-pass re-measurement campaigns, their raw
-- per-pass measurements, the canonical per-benchmark aggregate, and a canonical
-- per-task summary attached to the task row that already exists.
--
-- WHY.  The FormulaCode RL reward divides every agent measurement by a per-task
-- oracle baseline.  That baseline has lived in a single committed file
-- (`initial_survey/oracle/gold_oracle_benchmarks.json`, 1.7 MB, 59 tasks) with
-- no history and no provenance.  Two captures four hours apart on the same box
-- disagreed on every task that was actually re-measured: across the 2484
-- benchmark pairs involved, 66% moved further than the whole 1.05 noise
-- deadband, the impacted set was ~26% reproducible (median Jaccard), and 11.7%
-- of doubly-impacted benchmarks flipped sign.  The reward is built on the sign
-- of those numbers, so the baseline has to carry its own uncertainty and its own
-- provenance, and it has to be queryable next to the task it belongs to.
--
-- Numbered 00033.  00030 is the highest on this branch, but 00031
-- (`00031_lsv_baseline_cache.sql`) and 00032 (`00032_lsv_deps_cache.sql`) are
-- both claimed on `lsv-cache` / `origin/lsv-cache`, so taking either here would
-- collide when that branch lands.  00024 remains absent everywhere, as 00025's
-- header records.
--
-- ── TASK IDENTITY ───────────────────────────────────────────────────────────
-- The task row of record on this database is `pull_requests`, PK
-- (owner, repo, issue_number) -- the tuple CLAUDE.md names as "the canonical
-- identifier for one PR / one task".  There is deliberately NO reference to a
-- `tasks` table: none exists on this database.  The `tasks` table the trial
-- containers PATCH lives in Harbor's separate Supabase project, reached through
-- HARBOR_SUPABASE_URL (see scripts/harbor_tasks_migration.sql, whose header
-- says "Target: Harbor's Supabase project ... NOT datasmith's local Supabase",
-- and runners/harbor_healthcheck.py::_build_verifier_env, which maps the
-- HARBOR_SUPABASE_* variables into the container as its SUPABASE_*).
--
-- NOTHING HERE EVER INSERTS A TASK.  The FKs to `pull_requests` are the point:
-- publishing a rebuild for a task that has no row MUST fail loudly rather than
-- quietly building a parallel task universe.  The publishing tool preflights
-- the task list with GETs and refuses to write if any key is missing; these FKs
-- are the backstop for the case where it does not.
--
-- ── PRIVATE, AND ENFORCED (00026_formulacode_task_overrides pattern) ────────
-- These four tables are private.  That is a deliberate choice, not an omission:
-- `harbor_runs` and `benchmark_information` -- the existing measurement tables
-- -- are anon-readable and served on api.formulacode.org, and oracle rebuild
-- measurements are not public data.
--
-- 00015 revoked the default broad `anon` SELECT, so not granting SELECT keeps
-- these unreadable -- but that is only half of it.  Postgres/Supabase default
-- privileges still hand `anon` INSERT/UPDATE/DELETE/TRUNCATE on a newly created
-- table, and with RLS disabled nothing blocks them.  So the grants are revoked
-- explicitly and RLS is enabled with no policy, which denies every anon row.
-- The service-role key used by the publishing tool bypasses RLS entirely.

-- ── 1. One row per rebuild campaign ──────────────────────────────────────────
create table if not exists oracle_rebuilds (
  rebuild_id        uuid        primary key default gen_random_uuid(),

  -- Human-facing handle and natural upsert key: 'rebuild-20260930'.  Matches
  -- the tool's --out directory name and its storage prefix.
  label             text        not null unique,
  generated_at      timestamptz not null,

  -- ── provenance: what measured this, on what, from which source ──────────
  host              text,
  nproc             int,
  kernel            text,
  repo_sha          text,       -- formulacode-verified-rl commit
  datasmith_sha     text,       -- this repo's commit (template + renderer)
  lsv_commit        text,       -- LSV as reported from INSIDE a task container
  render_digest     text,       -- harbor_adapter render digest (stamp check)

  -- ── measurement protocol ────────────────────────────────────────────────
  k_passes          int         not null,
  arms              text[]      not null default array['oracle'],
  concurrency       int,
  gate_slots        int,
  lsv_rounds        int         not null,
  noise_floor       double precision not null default 1.05,

  -- How task images were pinned.  'baked-pinned' = every task.toml carried a
  -- docker_image whose id was recorded per task, the only state in which a
  -- rebuild is reproducible; 'mixed' = some tasks re-measured their baseline
  -- per trial; 'unpinned' = none were pinned.
  image_policy      text        check (image_policy in ('baked-pinned', 'mixed', 'unpinned')),

  -- ── rollup, so "how did this campaign go" is one row ────────────────────
  n_tasks           int,
  n_trusted         int,
  n_marginal        int,
  n_untrusted       int,

  -- Host load across the campaign (per-pass p50/p95, foreign load, co-tenants).
  -- The contention record is part of the measurement, not a log artifact.
  load_stats        jsonb,

  -- Full manifest verbatim, including per-task image ids.  The columns above
  -- are the queryable subset, not a summary of something lost.
  manifest          jsonb,

  -- Optional pointer to the raw per-pass tarball in Storage.  NULL is normal:
  -- the trial directories live on /overflow and are only archived on request.
  raw_archive_uri   text,

  -- At most one campaign is the live denominator.  Enforced below.
  is_canonical      boolean     not null default false,

  created_at        timestamptz not null default now()
);

-- At most one canonical rebuild, ever.  Unique on a column that is only ever
-- `true` in the indexed subset means a second canonical row cannot be inserted
-- without first demoting the current one -- so there is never a moment when two
-- campaigns both claim to be the live denominator.
create unique index if not exists uq_oracle_rebuilds_canonical
  on oracle_rebuilds (is_canonical)
  where is_canonical;

comment on table oracle_rebuilds is
  'One row per oracle re-measurement campaign: protocol, provenance, contention record and rollup. Private: RLS on, no policy, no anon grant.';
comment on column oracle_rebuilds.label is
  'Upsert key and storage prefix, e.g. rebuild-20260930. Matches the tool''s --out directory name.';
comment on column oracle_rebuilds.lsv_commit is
  'LSV commit as reported from inside a task container, not from a host checkout -- the host clone can lag what is baked into the images.';
comment on column oracle_rebuilds.image_policy is
  'baked-pinned means every task image id was recorded; only such a rebuild is reproducible.';
comment on column oracle_rebuilds.is_canonical is
  'The campaign the live reward divides by. At most one (uq_oracle_rebuilds_canonical); formulacode_task_overrides.oracle_rebuild_id points at it.';

-- ── 2. One row per task per pass per arm (the raw measurement) ───────────────
-- Deliberately NOT `harbor_runs`: that table is anon-readable and its FK is to
-- candidate_containers (owner, repo, sha), which our rendered RL tasks need not
-- have.  This is the same shape, private, and keyed to the task instead.
create table if not exists oracle_rebuild_passes (
  pass_id           uuid        primary key default gen_random_uuid(),
  rebuild_id        uuid        not null
                      references oracle_rebuilds (rebuild_id) on delete cascade,

  owner             text        not null,
  repo              text        not null,
  issue_number      int         not null,

  pass_idx          int         not null,

  -- 'oracle' applies the real solution patch and is the signal.  'placebo'
  -- applies a behaviour-preserving edit to the same files, so its true speedup
  -- is 1.0 everywhere; it measures the baked-baseline bias that repeated
  -- oracle passes structurally cannot see (every pass divides by the same
  -- baked baseline, so its error is invisible to repetition).
  arm               text        not null check (arm in ('oracle', 'placebo')),

  status            text,
  max_speedup       double precision,
  geomean_speedup   double precision,
  n_benchmarks      int,
  wallclock_sec     double precision,

  -- Host load sampled around this pass's measurement window.  A pass is judged
  -- contaminated against these, and a contaminated pass is excluded from the
  -- aggregate but kept here as the evidence for that exclusion.
  load_before       jsonb,
  load_after        jsonb,
  contaminated      boolean     not null default false,

  reward_payload    jsonb,      -- the pass's reward.json verbatim
  ran_at            timestamptz not null default now(),

  unique (rebuild_id, owner, repo, issue_number, pass_idx, arm),

  foreign key (owner, repo, issue_number)
    references pull_requests (owner, repo, issue_number)
);

create index if not exists idx_oracle_rebuild_passes_rebuild
  on oracle_rebuild_passes (rebuild_id);
create index if not exists idx_oracle_rebuild_passes_task
  on oracle_rebuild_passes (owner, repo, issue_number);
create index if not exists idx_oracle_rebuild_passes_contaminated
  on oracle_rebuild_passes (rebuild_id)
  where contaminated;

comment on table oracle_rebuild_passes is
  'One row per task per pass per arm: the raw oracle measurement and the host load it was taken under. Private by design -- harbor_runs and benchmark_information are anon-readable and this data is not public.';
comment on column oracle_rebuild_passes.arm is
  'oracle = the real solution patch (signal). placebo = a behaviour-preserving edit to the same files, whose true speedup is 1.0, measuring the baked-baseline bias repeated oracle passes cannot see.';
comment on column oracle_rebuild_passes.contaminated is
  'Pass excluded from the aggregate (host load, timing outlier, dropped benchmarks). Kept, never deleted: it is the evidence for the exclusion.';

-- ── 3. Raw per-benchmark values, one row per benchmark per pass ─────────────
create table if not exists oracle_rebuild_benchmarks (
  pass_id           uuid        not null
                      references oracle_rebuild_passes (pass_id) on delete cascade,
  benchmark_name    text        not null,
  log_speedup       double precision,
  raw               jsonb,

  primary key (pass_id, benchmark_name)
);

comment on table oracle_rebuild_benchmarks is
  'Raw per-benchmark log-speedup for one pass. The population the canonical median in task_oracle_benchmarks is taken over. Private: RLS on, no policy, no anon grant.';
comment on column oracle_rebuild_benchmarks.log_speedup is
  'ln(baseline/current) for this benchmark in this pass. NULL when the benchmark produced no usable result.';

-- ── 4. The canonical per-benchmark aggregate, versioned by rebuild ──────────
create table if not exists task_oracle_benchmarks (
  rebuild_id                   uuid    not null
                                 references oracle_rebuilds (rebuild_id) on delete cascade,
  owner                        text    not null,
  repo                         text    not null,
  issue_number                 int     not null,
  benchmark_name               text    not null,

  -- MEDIAN over accepted passes, not mean.  Contention noise is one-sided --
  -- interference only makes a benchmark slower -- so the mean of ratios is
  -- biased, and the median at K=5 tolerates two contaminated passes.
  median_log_speedup           double precision not null,
  log_iqr                      double precision,
  log_mad                      double precision,
  passes_present               int     not null,
  sign_consistency             double precision,  -- fraction of passes agreeing on the sign

  -- Legacy rule, kept verbatim so the derived JSON export and the current
  -- reward/dashboard agree: |median_log_speedup| > ln(noise_floor).
  impacted_beyond_noise_floor  boolean not null,

  -- New rule: the effect must exceed this benchmark's OWN measured noise, be
  -- present in most passes, and not flip sign between them.
  reliably_impacted            boolean not null,

  -- Per-benchmark noise floor, max(ln(noise_floor), |bias| + 2 sigma) from the
  -- placebo arm.  Replaces the single global constant with something measured.
  noise_theta                  double precision,
  placebo_log_bias             double precision,  -- null when the placebo arm was unusable
  placebo_log_mad              double precision,

  primary key (rebuild_id, owner, repo, issue_number, benchmark_name),

  foreign key (owner, repo, issue_number)
    references pull_requests (owner, repo, issue_number)
);

create index if not exists idx_task_oracle_benchmarks_task
  on task_oracle_benchmarks (owner, repo, issue_number);
create index if not exists idx_task_oracle_benchmarks_reliable
  on task_oracle_benchmarks (rebuild_id, owner, repo, issue_number)
  where reliably_impacted;

comment on table task_oracle_benchmarks is
  'Canonical per-benchmark oracle aggregate for one rebuild: the median log-speedup and its measured noise. Versioned by rebuild_id. Private: RLS on, no policy, no anon grant.';
comment on column task_oracle_benchmarks.median_log_speedup is
  'Median of ln(speedup) over accepted passes. Median, not mean: contention noise is one-sided so the mean of ratios is biased.';
comment on column task_oracle_benchmarks.noise_theta is
  'Per-benchmark noise floor: max(ln(noise_floor), |placebo_log_bias| + 2*1.4826*placebo_log_mad). Published for a later reward change; nothing reads it yet.';
comment on column task_oracle_benchmarks.impacted_beyond_noise_floor is
  'Legacy global-floor rule, kept verbatim so the derived JSON export stays byte-compatible with the current reward and dashboard.';

-- ── 5. Canonical per-task summary on the EXISTING per-task row ──────────────
-- formulacode_task_overrides is already the private, per-task, operator-facing
-- row keyed by the canonical triple, and it already carries `oracle_h` ("the
-- human-authored speedup this task is scored against").  The canonical oracle
-- summary belongs next to it, not on the anon-readable `pull_requests`.
--
-- ADD COLUMN IF NOT EXISTS is idempotent and takes no table rewrite for a
-- nullable column with no default.  Every pre-existing row gets NULL, which
-- reads correctly as "no rebuild has claimed this task yet" rather than as a
-- speedup of zero.
alter table formulacode_task_overrides
  add column if not exists oracle_rebuild_id          uuid,
  add column if not exists oracle_lsv_mean_speedup    double precision,
  add column if not exists oracle_n_impacted          int,
  add column if not exists oracle_n_reliably_impacted int,
  add column if not exists oracle_geomean_log_iqr     double precision,
  add column if not exists oracle_trust               text,
  add column if not exists oracle_updated_at          timestamptz;

comment on column formulacode_task_overrides.oracle_rebuild_id is
  'Which oracle_rebuilds campaign produced the canonical numbers on this row. NULL = no rebuild has claimed this task.';
comment on column formulacode_task_overrides.oracle_lsv_mean_speedup is
  'Canonical geometric-mean oracle speedup: the denominator the RL reward divides by. Measured, unlike the hand-declared oracle_h above.';
comment on column formulacode_task_overrides.oracle_geomean_log_iqr is
  'IQR of ln(lsv_mean_speedup) across accepted passes. A task whose own spread exceeds ln(noise_floor) is measuring the box, not the patch.';
comment on column formulacode_task_overrides.oracle_trust is
  'trusted | marginal | untrusted. untrusted means the oracle is too noisy to reward against and the task should be dropped or masked.';

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'fto_oracle_rebuild_id_fkey'
      and conrelid = 'formulacode_task_overrides'::regclass
  ) then
    alter table formulacode_task_overrides
      add constraint fto_oracle_rebuild_id_fkey
      foreign key (oracle_rebuild_id) references oracle_rebuilds (rebuild_id)
      on delete set null;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'fto_oracle_trust_check'
      and conrelid = 'formulacode_task_overrides'::regclass
  ) then
    alter table formulacode_task_overrides
      add constraint fto_oracle_trust_check
      check (oracle_trust is null
             or oracle_trust in ('trusted', 'marginal', 'untrusted'));
  end if;
end
$$;

create index if not exists idx_fto_oracle_trust
  on formulacode_task_overrides (oracle_trust);

-- ── Lock the four new tables down ───────────────────────────────────────────
-- Deliberately NO policy: RLS with zero policies denies every row to every
-- non-superuser role, and the service-role key used by the publishing tool
-- bypasses RLS entirely, so operator tooling is unaffected.  Per CLAUDE.md a
-- new table is private by default and stays that way.  formulacode_task_overrides
-- is already locked down by 00026 and only gains nullable columns here, so its
-- privileges are untouched.
revoke all on oracle_rebuilds           from anon;
revoke all on oracle_rebuilds           from authenticated;
revoke all on oracle_rebuild_passes     from anon;
revoke all on oracle_rebuild_passes     from authenticated;
revoke all on oracle_rebuild_benchmarks from anon;
revoke all on oracle_rebuild_benchmarks from authenticated;
revoke all on task_oracle_benchmarks    from anon;
revoke all on task_oracle_benchmarks    from authenticated;

alter table oracle_rebuilds           enable row level security;
alter table oracle_rebuild_passes     enable row level security;
alter table oracle_rebuild_benchmarks enable row level security;
alter table task_oracle_benchmarks    enable row level security;
