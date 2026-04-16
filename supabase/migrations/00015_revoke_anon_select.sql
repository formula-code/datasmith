-- Supabase's default grants give the anon role SELECT on every table in the
-- public schema. Migration 00012 only enabled RLS (and added a public_read
-- policy) on four tables, leaving the rest unprotected: with RLS disabled,
-- the default grant wins and anon can read them via PostgREST.
--
-- Lock this down: revoke the broad anon SELECT, re-grant only the four
-- intentionally-public tables, and change the default privilege so any
-- table added later is anon-invisible unless explicitly granted.
--
-- The service-role key bypasses grants (superuser-equivalent), so the
-- pipeline is unaffected. The grafana_ro role is a separate principal.

REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM anon;

GRANT SELECT ON repositories, pull_requests, candidate_containers, harbor_runs TO anon;

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM anon;
