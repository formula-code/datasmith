-- supabase/migrations/00025_candidate_containers_build_manifest.sql
--
-- Build manifest: the facts a build recorded about itself, plus the
-- outcome of the invariants evaluated over them.
--
-- Kept separate from resource_metrics deliberately.  resource_metrics is
-- observed cost (timings, sizes, memory) and always advisory; build_manifest
-- is declared facts that gate behaviour.  The decisive reason is triage:
-- `build_manifest IS NULL` cleanly identifies rows built before this existed.
--
-- Numbered 00025: 00024_formulacode_task_overrides is authored in a separate
-- working tree and not yet applied here.

alter table candidate_containers
  add column if not exists build_manifest    jsonb,
  add column if not exists manifest_warnings text[];

create index if not exists idx_cc_manifest_warnings
  on candidate_containers using gin (manifest_warnings);

comment on column candidate_containers.build_manifest is
  'Sealed build facts + merged verify observations. NULL = built before manifests existed.';
comment on column candidate_containers.manifest_warnings is
  'Non-fatal invariant ids raised for this build, plus triage tags.';

grant select on candidate_containers to anon, authenticated;
