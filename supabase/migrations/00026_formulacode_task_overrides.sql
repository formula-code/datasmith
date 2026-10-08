-- supabase/migrations/00026_formulacode_task_overrides.sql
--
-- Per-task overrides: the facts about a task that cannot be derived from the
-- repo or the PR, and must be declared by an operator.
--
-- Numbered 00026, not 00024.  00025's own header records that
-- 00024_formulacode_task_overrides is authored in a separate working tree and
-- unapplied here; taking 00026 avoids colliding with it if it ever lands.
-- This table is the same concept under a number that is free in this tree.
--
-- Schema follows the REAL override record shipped in
-- rl-prep/local-overrides/formulacode_task_overrides.json (5 tasks), not the
-- shape the 2026-07-31 spec assumed.  That spec asserted `expected_n` "is a
-- declared field on the override row"; it is not -- the real record has ten
-- keys and none of them is expected_n.  See
-- docs/superpowers/specs/2026-08-13-followon-plans-design.md.
--
-- PRIVATE BY DESIGN, AND ENFORCED.  Migration 00015 revoked the default broad
-- `anon` SELECT, so simply not granting SELECT keeps this table unreadable.
-- That is NOT sufficient on its own: Postgres/Supabase default privileges
-- still hand `anon` INSERT/UPDATE/DELETE/TRUNCATE on a newly created table,
-- and with RLS disabled nothing blocks them.  Verified empirically against
-- this database at creation time -- the table came up with six anon write
-- privileges.  So the grants are revoked explicitly below and RLS is enabled
-- with no policy, which denies every anon row.
--
-- NOTE (pre-existing, NOT introduced here): `packages` and `error_logs` carry
-- the same default anon write grants with RLS disabled. They are out of scope
-- for this migration -- fixing them is a separate, deliberate change with its
-- own blast radius -- but they are recorded here because the pattern is
-- systemic rather than a one-off.

create table if not exists formulacode_task_overrides (
  owner                     text        not null,
  repo                      text        not null,
  issue_number              int         not null,

  -- ── from the real rl-prep record ──────────────────────────────────────
  -- The benchmark file this task's measurement depends on.  This is the
  -- producer for the FATAL `benchmark_dest_missing` invariant, which has
  -- been inert since it shipped because nothing in the tree set
  -- $BENCHMARK_DEST.
  benchmark_dest            text,
  benchmark_storage_key     text,
  extra_dockerfile_commands text,
  extra_entrypoint_commands text,
  pip_pins                  text[],
  restore_regex             text,
  -- The human-authored speedup this task is scored against.
  oracle_h                  numeric,

  -- ── hand-declared, not derived ────────────────────────────────────────
  -- How many benchmarks this PR SHOULD impact.  Input to the dilution
  -- invariant (#18), which compares impacted_n against it.
  --
  -- NULL means "no one has judged this task yet", and the invariant then
  -- SKIPS rather than failing or guessing.  It is deliberately not derived:
  -- deriving it from the oracle's own impacted set would make the check
  -- compare the oracle against itself, which cannot catch the networkx case
  -- (140 measured when 10 were expected) that motivates the invariant --
  -- the oracle would have selected 140 too.
  expected_n                int,
  notes                     text,

  created_at                timestamptz not null default now(),
  updated_at                timestamptz not null default now(),

  primary key (owner, repo, issue_number)
);

comment on table formulacode_task_overrides is
  'Per-task operator declarations that cannot be derived from the repo or PR. Private: no RLS policy, no anon grant.';
comment on column formulacode_task_overrides.expected_n is
  'Hand-declared count of benchmarks this PR should impact. NULL = not yet judged; the dilution invariant skips rather than guessing.';
comment on column formulacode_task_overrides.benchmark_dest is
  'Benchmark file the measurement depends on. Producer for the benchmark_dest_missing invariant.';

-- Lock the table down.  Not granting SELECT is only half of it: the default
-- privileges above must be revoked, and RLS must be ON so that even a future
-- accidental grant cannot expose rows without an explicit policy.
revoke all on formulacode_task_overrides from anon;
revoke all on formulacode_task_overrides from authenticated;

alter table formulacode_task_overrides enable row level security;

-- Deliberately NO policy.  RLS with zero policies denies every row to every
-- non-superuser role; the service-role key used by the pipeline bypasses RLS
-- entirely, so operator tooling is unaffected.  Do not add a policy or a
-- grant without a specific reason: per CLAUDE.md a new table is private by
-- default and should stay that way.
