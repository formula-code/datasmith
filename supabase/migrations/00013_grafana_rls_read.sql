-- Allow the grafana_ro role to SELECT through RLS on tables that have it enabled.
-- The public_read policies (migration 00012) only cover the anon role;
-- grafana_ro connects via direct Postgres and needs its own policies.

CREATE POLICY "grafana_read" ON repositories FOR SELECT TO grafana_ro USING (true);
CREATE POLICY "grafana_read" ON pull_requests FOR SELECT TO grafana_ro USING (true);
CREATE POLICY "grafana_read" ON candidate_containers FOR SELECT TO grafana_ro USING (true);
CREATE POLICY "grafana_read" ON harbor_runs FOR SELECT TO grafana_ro USING (true);
