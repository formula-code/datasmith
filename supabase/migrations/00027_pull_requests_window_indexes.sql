-- supabase/migrations/00027_pull_requests_window_indexes.sql
--
-- Indexes for the ingestion window predicates on pull_requests.
--
-- pull_requests carries ONLY its primary-key index (owner, repo,
-- issue_number).  Every window query therefore sequentially scans all
-- 265,181 rows in roughly 300 ms -- measured on the live database on
-- 2026-08-23 -- for both the merged_at and the created_at filters.  Stages
-- 2-5 now agree on one window definition, half-open [start, end) over
-- merged_at (see docs/superpowers/specs/
-- 2026-08-23-ingestion-window-and-async-pipeline-design.md, sections 4.1
-- and 6.3), so that filter is about to run on every stage of every monthly
-- run instead of once.
--
-- Two indexes, one per predicate shape the new code issues:
--
--   1. merged_at alone            -- the stage-wide window scan.
--   2. (owner, repo, merged_at)   -- the per-repository, window-scoped skip
--                                    set.  Resume reads only the rows in the
--                                    window for one repo, replacing the old
--                                    full-table skip-set read that pulled
--                                    25,802 rows for pandas to decide 35
--                                    upserts.  The leading (owner, repo)
--                                    columns must come first so the composite
--                                    index can serve that lookup; the
--                                    primary key cannot, because merged_at is
--                                    not part of it.
--
-- Numbered 00027, not 00024.  00026 is the highest migration number present
-- on any branch of this repository.  00024 is absent from all of them: as
-- 00025's own header records, 00024_formulacode_task_overrides is authored in
-- a separate working tree and has never landed here, so that number stays
-- reserved rather than reused.
--
-- GRANTS NOTHING TO anon, DELIBERATELY.  pull_requests is already
-- anon-readable through 00012 (RLS policy) plus 00015 (the narrow re-grant
-- after the broad anon SELECT was revoked).  This migration adds indexes and
-- changes no privilege, so it must not restate a grant -- restating one here
-- would silently re-widen access if 00012/00015 are ever tightened.
--
-- Plain CREATE INDEX, not CONCURRENTLY: the Supabase migration runner wraps
-- each file in a transaction and CREATE INDEX CONCURRENTLY cannot run inside
-- one.  This takes a brief write lock on pull_requests; the pipeline is a
-- monthly batch, so apply it between runs.

create index if not exists idx_pull_requests_merged_at
  on pull_requests (merged_at);

create index if not exists idx_pull_requests_repo_merged_at
  on pull_requests (owner, repo, merged_at);

comment on index idx_pull_requests_merged_at is
  'Half-open [start, end) window scan over merged_at for ingestion stages 2-5.';
comment on index idx_pull_requests_repo_merged_at is
  'Per-repository window-scoped skip set; replaces the full-table skip-set read.';
