-- Expose `benchmark_type` (time / mem / peakmem / track) on
-- benchmark_information. Derived from the ASV naming convention in
-- `benchmark_name`: classes are `Time*` / `Mem*` / `Peakmem*` and methods
-- are `time_*` / `mem_*` / `peakmem_*` / `track_*`. A STORED generated
-- column backfills automatically; the website can filter without parsing.
--
-- See https://asv.readthedocs.io/en/stable/writing_benchmarks.html for the
-- convention. The patterns are case-sensitive to match ASV's discovery.

ALTER TABLE benchmark_information
    ADD COLUMN IF NOT EXISTS benchmark_type TEXT
    GENERATED ALWAYS AS (
        CASE
            WHEN benchmark_name ~ '(^|\.)Peakmem[A-Z]' OR benchmark_name ~ '\.peakmem_'      THEN 'peakmem'
            WHEN benchmark_name ~ '(^|\.)Mem[A-Z]'     OR benchmark_name ~ '\.mem_'          THEN 'mem'
            -- ASV defines `time_*` (in-process) and `timeraw_*` (fresh subprocess); both are time benchmarks.
            WHEN benchmark_name ~ '(^|\.)Time[A-Z]'    OR benchmark_name ~ '\.time(raw)?_'   THEN 'time'
            WHEN benchmark_name ~ '\.track_'                                                 THEN 'track'
            ELSE NULL
        END
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_benchmark_information_type
    ON benchmark_information (benchmark_type);
