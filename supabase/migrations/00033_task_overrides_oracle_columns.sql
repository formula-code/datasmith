-- Measured oracle results from the rebuild tool, stored beside the hand-declared oracle_h.
-- One row per task; raw per-run data stays as files, not in the database.
alter table formulacode_task_overrides
  add column if not exists oracle_speedup     double precision,
  add column if not exists oracle_trust       text,
  add column if not exists oracle_benchmarks  jsonb,
  add column if not exists oracle_provenance  jsonb,
  add column if not exists oracle_measured_at timestamptz;

do $$
begin
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
