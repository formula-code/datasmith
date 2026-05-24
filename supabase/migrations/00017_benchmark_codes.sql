-- Per-benchmark source code, keyed by (owner, repo, benchmark_without_params).
-- Populated by pipeline stage 9 (scrape_benchmark_source), which checks out
-- each repo at its candidate-container SHA and AST-parses every ASV-style
-- function under `benchmarks/`. The FormulaCode website consumes this via
-- api.formulacode.org to render the player page's "Benchmark code" tabs
-- (~1.3 MB of the website CSV historically).
--
-- The `benchmark_without_params` value matches the param-stripped form
-- stored in `benchmark_information.benchmark_name`
-- (e.g. "benchmarks.ConstructorsSuite.time_point"), so the website can join
-- benchmark_codes ⋈ benchmark_information on
-- (owner, repo, benchmark_without_params = benchmark_name).
--
-- If a benchmark moves or is deleted upstream the row is kept; the website
-- falls back gracefully and `last_scraped_sha` is the commit the source
-- was read from.

CREATE TABLE IF NOT EXISTS benchmark_codes (
    owner                      TEXT NOT NULL,
    repo                       TEXT NOT NULL,
    benchmark_without_params   TEXT NOT NULL,
    source                     TEXT NOT NULL,
    setup_source               TEXT,
    last_scraped               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scraped_sha           TEXT,
    PRIMARY KEY (owner, repo, benchmark_without_params)
);

CREATE INDEX IF NOT EXISTS idx_benchmark_codes_repo
    ON benchmark_codes (owner, repo);

GRANT ALL ON benchmark_codes TO anon, authenticated, service_role;

-- Public read access (mirrors 00016_benchmark_information.sql).
-- Anon role can SELECT but not write; service-role bypasses RLS.
ALTER TABLE benchmark_codes ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS public_read ON benchmark_codes;
CREATE POLICY public_read ON benchmark_codes
    FOR SELECT
    TO anon
    USING (true);

REVOKE INSERT, UPDATE, DELETE ON benchmark_codes FROM anon;
