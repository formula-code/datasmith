-- Oracle rebuild campaigns, their raw and aggregated measurements, and a per-task
-- summary. Private; FKs to pull_requests so publishing for an unknown task fails.
create table if not exists oracle_rebuilds (
  rebuild_id        uuid        primary key default gen_random_uuid(),
  label             text        not null unique,
  generated_at      timestamptz not null,
  host              text,
  nproc             int,
  kernel            text,
  repo_sha          text,
  datasmith_sha     text,
  lsv_commit        text,
  render_digest     text,
  k_passes          int         not null,
  arms              text[]      not null default array['oracle'],
  concurrency       int,
  gate_slots        int,
  lsv_rounds        int         not null,
  noise_floor       double precision not null default 1.05,
  image_policy      text        check (image_policy in ('baked-pinned', 'mixed', 'unpinned')),
  n_tasks           int,
  n_trusted         int,
  n_marginal        int,
  n_untrusted       int,
  load_stats        jsonb,
  manifest          jsonb,
  raw_archive_uri   text,
  is_canonical      boolean     not null default false,
  created_at        timestamptz not null default now()
);

-- Partial unique index on a true-only subset: at most one canonical rebuild.
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

create table if not exists oracle_rebuild_passes (
  pass_id           uuid        primary key default gen_random_uuid(),
  rebuild_id        uuid        not null
                      references oracle_rebuilds (rebuild_id) on delete cascade,
  owner             text        not null,
  repo              text        not null,
  issue_number      int         not null,
  pass_idx          int         not null,
  arm               text        not null check (arm in ('oracle', 'placebo')),
  status            text,
  max_speedup       double precision,
  geomean_speedup   double precision,
  n_benchmarks      int,
  wallclock_sec     double precision,
  load_before       jsonb,
  load_after        jsonb,
  contaminated      boolean     not null default false,
  reward_payload    jsonb,
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

create table if not exists task_oracle_benchmarks (
  rebuild_id                   uuid    not null
                                 references oracle_rebuilds (rebuild_id) on delete cascade,
  owner                        text    not null,
  repo                         text    not null,
  issue_number                 int     not null,
  benchmark_name               text    not null,
  median_log_speedup           double precision not null,
  log_iqr                      double precision,
  log_mad                      double precision,
  passes_present               int     not null,
  sign_consistency             double precision,
  impacted_beyond_noise_floor  boolean not null,
  reliably_impacted            boolean not null,
  noise_theta                  double precision,
  placebo_log_bias             double precision,
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

-- Default privileges grant anon writes on new tables; RLS with no policy denies all rows.
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
