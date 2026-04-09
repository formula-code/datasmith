-- Read-only role for Grafana datasource.
-- Safe to expose via ngrok — can only SELECT, never mutate.

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'grafana_ro') THEN
    CREATE ROLE grafana_ro WITH LOGIN PASSWORD 'grafana_readonly';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE postgres TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;
