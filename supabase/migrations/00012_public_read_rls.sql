-- Enable Row Level Security on tables exposed to the public API.
-- The service-role key bypasses RLS, so active pipeline processes are unaffected.

ALTER TABLE repositories ENABLE ROW LEVEL SECURITY;
ALTER TABLE pull_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE candidate_containers ENABLE ROW LEVEL SECURITY;
ALTER TABLE harbor_runs ENABLE ROW LEVEL SECURITY;

-- Allow the anon role to SELECT all rows from these tables.
CREATE POLICY "public_read" ON repositories FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON pull_requests FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON candidate_containers FOR SELECT TO anon USING (true);
CREATE POLICY "public_read" ON harbor_runs FOR SELECT TO anon USING (true);
