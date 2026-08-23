# Dataset Composition — Grafana dashboard row

**Date:** 2026-07-31
**Status:** Approved (design), pending implementation plan
**Owner:** dashboard / monitoring

## Summary

Add one new collapsible row, **Dataset Composition**, to the DataSmith
overview Grafana dashboard. The row surfaces *how large the human performance
fixes are* and *whether the difficulty labels are meaningful*, using data
already resident in `pull_requests`. This is purely additive: new SQL panels
against the existing Postgres datasource, no schema changes, no new tables, no
Python.

### Why

The current dashboard covers pipeline **construction** (scraping, resolution,
synthesis, Harbor) thoroughly but says almost nothing about the **composition
and quality of the corpus itself**. Two whole-corpus, well-populated facets
were selected during brainstorming:

- **Human fix size** — how big is the human change we ask models to match?
- **Difficulty validation** — do the `difficulty` labels actually track
  anything real?

A third candidate, *human-speedup headroom* (`benchmark_information` oracle
rows), was explicitly **dropped**: it covers only 37 evaluated tasks and is
100% `time`-type today — too thin to feature. Revisit when the eval set is
larger and multi-type.

## Data grounding (verified against the live DB, 2026-07-31)

- `pull_requests.file_changes` is a JSONB **array** of
  `{filename, additions, deletions}`. 13,004 perf PRs have it populated.
- Lines changed per perf PR (`sum(additions+deletions)`): median **82**,
  p75 269, p90 749, p95 1296, max 13,421.
- Files touched per perf PR (`array length`): median **3**, p90 13, p95 21,
  max 64.
- Line-count buckets `<10 / 10–50 / 50–200 / 200–1000 / 1000+` split the
  corpus as 1643 / 3469 / 3858 / 3117 / 917.
- Difficulty × fix size is monotone and strong:

  | difficulty | n | median lines | median files |
  |------------|-----|--------------|--------------|
  | easy       | 5099 | 20   | 2  |
  | medium     | 5867 | 199  | 5  |
  | hard       | 465  | 1406 | 18 |
  | unknown    | 1573 | 91   | 3  |

- 7,088 perf PRs carry a non-null `difficulty` label; `classification` is the
  optimization-type field already used by the existing "Optimization Type
  Distribution" panel.

## Panels

All three use `datasource: { uid: "supabase-pg", type: "postgres" }`,
`format: "table"`, and source from
`pull_requests WHERE is_performance_commit`.

### Panel 1 — Fix Size Distribution (barchart histogram)

Lines-changed per perf PR, bucketed. Corpus-wide shape.

```sql
WITH pr_size AS (
  SELECT (
    SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int), 0)
    FROM jsonb_array_elements(file_changes) e
  ) AS lines_changed
  FROM pull_requests
  WHERE is_performance_commit AND file_changes IS NOT NULL
)
SELECT bucket, COUNT(*) AS prs
FROM (
  SELECT CASE
    WHEN lines_changed < 10   THEN '1. <10'
    WHEN lines_changed < 50   THEN '2. 10-50'
    WHEN lines_changed < 200  THEN '3. 50-200'
    WHEN lines_changed < 1000 THEN '4. 200-1000'
    ELSE '5. 1000+'
  END AS bucket
  FROM pr_size
) t
GROUP BY bucket
ORDER BY bucket;
```

Bucket labels are numeric-prefixed so barchart x-ordering is stable; the
prefix is cosmetic and may be stripped with a Grafana value mapping or field
override if desired (optional, not required).

Expected counts: 1643 / 3469 / 3858 / 3117 / 917.

### Panel 2 — Fix Size by Difficulty (barchart or table)

Per difficulty bucket: median lines changed, median files touched, PR count.
The validation money-panel.

```sql
WITH pr_size AS (
  SELECT COALESCE(difficulty, 'unset') AS difficulty,
    (SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int), 0)
     FROM jsonb_array_elements(file_changes) e) AS lines_changed,
    jsonb_array_length(file_changes) AS files_touched
  FROM pull_requests
  WHERE is_performance_commit AND file_changes IS NOT NULL
)
SELECT difficulty,
  COUNT(*) AS prs,
  ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY lines_changed)) AS median_lines,
  ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY files_touched)) AS median_files
FROM pr_size
GROUP BY difficulty
ORDER BY CASE difficulty
  WHEN 'easy' THEN 1 WHEN 'medium' THEN 2 WHEN 'hard' THEN 3
  WHEN 'unknown' THEN 4 ELSE 5 END;
```

Expected medians: easy 20/2, medium 199/5, hard 1406/18, unknown 91/3.

Render as a **barchart** (x = difficulty, series = median_lines + median_files)
per the approved design; a table is an acceptable equivalent if the barchart's
dual-axis scaling (lines in hundreds vs. files in single digits) reads poorly —
implementer's call, note which was chosen.

### Panel 3 — Optimization Type by Difficulty (stacked horizontal barchart)

`classification` distribution within each difficulty bucket.

```sql
SELECT COALESCE(difficulty, 'unset') AS difficulty,
  REPLACE(COALESCE(classification, 'unclassified'), '_', ' ') AS optimization_type,
  COUNT(*) AS count
FROM pull_requests
WHERE is_performance_commit AND classification IS DISTINCT FROM 'legacy-verified'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

Rendered as a stacked barchart with `orientation: horizontal`,
`stacking: normal`, difficulty on the category axis and `optimization_type`
as the stacked series. (Grafana table→series shaping: the query returns long
format; use the barchart panel's default field mapping, or a "Rows to fields"
transformation keyed on `difficulty` if the long form does not stack cleanly —
implementer verifies visually.)

## Layout

- Append a `row` panel titled **Dataset Composition** plus the three panels to
  the **end** of the `panels` array in
  `grafana/provisioning/dashboards-json/datasmith-overview.json`.
- Assign `gridPos.y` **above the current maximum** (current max is the
  Pipeline Funnel at y=163, h=8 → use y ≥ 172 for the row, panels below it).
  Grafana orders by `gridPos.y`, not array order, so appending avoids
  renumbering every existing panel.
- New panel `id`s: **71**, **72**, **73** (the row panel needs no `id`,
  matching every other row in the file). Existing ids in use include 1–66 and
  100–101; 71–73 are free.
- Suggested widths (24-col grid): Panel 1 `w=8`, Panel 2 `w=8`, Panel 3 `w=8`,
  each `h=8`, on one visual row.

Rejected alternative: inserting the row after "Pull Request Insights" (its
natural thematic home) would require incrementing `gridPos.y` on every
later panel — higher risk for no functional gain, since the row is named and
collapsible and thus discoverable wherever it sits.

## Data flow

Grafana → `supabase-pg` Postgres datasource → `public.pull_requests`. The
Grafana read-only role already reads `pull_requests` (migrations
`00009_grafana_readonly.sql`, `00013_grafana_rls_read.sql`; the existing
panels query it). **No new grants required.**

## Known simplifications

- **"Fix size" = whole-PR size.** `file_changes` counts every changed file,
  including tests, docs, and config — not source-only. Accepted for a first
  cut; a source-only refinement is out of scope.
- **`unknown` / `unset` difficulty** are shown as their own buckets, never
  merged into or hidden from the labeled ones.
- Panel 3 excludes `classification = 'legacy-verified'`, matching the existing
  "Optimization Type Distribution" panel's convention.
- Bucket boundaries are fixed literals chosen from the current distribution;
  they are not auto-scaling. If the corpus distribution shifts substantially,
  the buckets may need a manual revisit.

## Validation / acceptance

1. Each panel's SQL runs without error against the live DB and returns counts
   matching this spec (Panel 1: 1643/3469/3858/3117/917; Panel 2 medians
   20/199/1406; Panel 3: non-empty, sums to the labeled-PR population minus
   `legacy-verified`).
2. The edited `datasmith-overview.json` parses as valid JSON (`jq . <file>`).
3. After a Grafana provisioning reload (`grafana/docker-compose.yml`), the
   **Dataset Composition** row appears with all three panels rendering
   non-empty, and **no existing panel has moved or broken**.
4. A short subsection added to `docs/guide/monitoring.md` describing the new
   row.

## Out of scope

- Human-speedup headroom / any `benchmark_information` panel (dropped; too thin
  today).
- Benchmark-coverage-per-task panels.
- Any schema change, migration, grant, or Python code.
- Source-only fix-size accounting.
