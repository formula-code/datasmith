# Dataset Composition Dashboard Row — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Dataset Composition" row (3 SQL panels) to the DataSmith Grafana dashboard surfacing human fix size and difficulty-label validation.

**Architecture:** Purely additive edit to one provisioned dashboard JSON file plus one doc note. Three Postgres panels query the existing `pull_requests` table through the existing `supabase-pg` datasource. No schema changes, no migrations, no new grants, no Python code.

**Tech Stack:** Grafana 11.6.0 (provisioned dashboard JSON), PostgreSQL (Supabase local, `127.0.0.1:54322`), `jq` for JSON validation, `uv run --with pg8000` for ad-hoc SQL verification.

## Global Constraints

- Target file: `grafana/provisioning/dashboards-json/datasmith-overview.json`. Edit only this file (Task 1) and `docs/guide/monitoring.md` (Task 2).
- Every panel uses `"datasource": { "uid": "supabase-pg", "type": "postgres" }` and `"format": "table"`.
- All panels source from `pull_requests WHERE is_performance_commit`.
- New panel `id`s are exactly **71, 72, 73**. The row panel carries no `id` (matches every other row in the file). Do not reuse ids 1–66 or 100–101.
- Append the new row + panels at the **end** of the `panels` array with `gridPos.y` **≥ 172** (current max panel bottom is y=171). Do not modify any existing panel's `gridPos`, `id`, or SQL.
- Panel 3 excludes `classification = 'legacy-verified'` (matches the existing "Optimization Type Distribution" panel).
- `unknown` / `unset` difficulty are shown as their own buckets, never merged or hidden.
- No schema changes, migrations, grants, or Python. Do not touch `benchmark_information` (dropped from scope).
- The dashboard JSON is machine-formatted by the `pretty format json` pre-commit hook; let the hook reformat rather than fighting indentation by hand.

---

### Task 1: Add "Dataset Composition" row and three panels to the dashboard JSON

**Files:**
- Modify: `grafana/provisioning/dashboards-json/datasmith-overview.json` (append to the `panels` array, just before its closing `]`)

**Interfaces:**
- Consumes: existing `supabase-pg` Postgres datasource; `public.pull_requests` columns `is_performance_commit`, `file_changes` (JSONB array of `{filename, additions, deletions}`), `difficulty`, `classification`.
- Produces: three new panels (`id` 71/72/73) under a `row` titled "Dataset Composition". Task 2 references this row title verbatim.

**Context:** The file is a single large Grafana dashboard object with a top-level `panels` array (~1740 lines). Panels render by `gridPos.y`, not array order, so appending at the end places the row at the bottom of the dashboard without disturbing existing panels. The last existing panel is `id: 101` ("Live Runners"); its object closes at the end of the array. You will add a comma after that closing `}` and insert the four new objects (1 row + 3 panels) before the array's closing `]`.

The three SQL queries below have already been run against the live database and return sensible results. Exact row counts drift as the pipeline runs; the acceptance check is **shape + approximate magnitude**, not exact equality.

- [ ] **Step 1: Verify Panel 1 SQL runs and returns 5 ordered buckets**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
uv run --with pg8000 python - <<'PY'
import pg8000.native
con = pg8000.native.Connection(user="postgres", password="postgres", host="127.0.0.1", port=54322, database="postgres")
sql = "WITH pr_size AS (SELECT (SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int),0) FROM jsonb_array_elements(file_changes) e) AS lines_changed FROM pull_requests WHERE is_performance_commit AND file_changes IS NOT NULL) SELECT bucket, COUNT(*) AS prs FROM (SELECT CASE WHEN lines_changed < 10 THEN '1. <10' WHEN lines_changed < 50 THEN '2. 10-50' WHEN lines_changed < 200 THEN '3. 50-200' WHEN lines_changed < 1000 THEN '4. 200-1000' ELSE '5. 1000+' END AS bucket FROM pr_size) t GROUP BY bucket ORDER BY bucket;"
for r in con.run(sql): print(r)
con.close()
PY
```
Expected: five rows `1. <10 … 5. 1000+`, each with a count in the hundreds-to-thousands, totalling ~13,000. (Reference run: 1644 / 3569 / 3922 / 3086 / 783.)

- [ ] **Step 2: Verify Panel 2 SQL shows the monotone easy→hard signal**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
uv run --with pg8000 python - <<'PY'
import pg8000.native
con = pg8000.native.Connection(user="postgres", password="postgres", host="127.0.0.1", port=54322, database="postgres")
sql = "WITH pr_size AS (SELECT COALESCE(difficulty,'unset') AS difficulty, (SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int),0) FROM jsonb_array_elements(file_changes) e) AS lines_changed, jsonb_array_length(file_changes) AS files_touched FROM pull_requests WHERE is_performance_commit AND file_changes IS NOT NULL) SELECT difficulty, COUNT(*) AS prs, ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY lines_changed)) AS median_lines, ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY files_touched)) AS median_files FROM pr_size GROUP BY difficulty ORDER BY CASE difficulty WHEN 'easy' THEN 1 WHEN 'medium' THEN 2 WHEN 'hard' THEN 3 WHEN 'unknown' THEN 4 ELSE 5 END;"
for r in con.run(sql): print(r)
con.close()
PY
```
Expected: rows ordered easy, medium, hard, unknown; `median_lines` increases sharply easy→hard (reference: ~20 → ~193 → ~1339), `median_files` likewise (~2 → ~5 → ~18).

- [ ] **Step 3: Verify Panel 3 pivot SQL returns wide format (easy/medium/hard)**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
uv run --with pg8000 python - <<'PY'
import pg8000.native
con = pg8000.native.Connection(user="postgres", password="postgres", host="127.0.0.1", port=54322, database="postgres")
sql = "WITH base AS (SELECT COALESCE(difficulty,'unset') AS difficulty, REPLACE(COALESCE(classification,'unclassified'),'_',' ') AS opt FROM pull_requests WHERE is_performance_commit AND classification IS DISTINCT FROM 'legacy-verified') SELECT difficulty, COUNT(*) FILTER (WHERE opt='remove or reduce work') AS \"remove/reduce work\", COUNT(*) FILTER (WHERE opt='use better algorithm') AS \"better algorithm\", COUNT(*) FILTER (WHERE opt='use lower level system') AS \"lower level system\", COUNT(*) FILTER (WHERE opt='cache and reuse') AS \"cache & reuse\", COUNT(*) FILTER (WHERE opt='use better data structure and layout') AS \"data structure/layout\", COUNT(*) FILTER (WHERE opt='micro optimizations') AS \"micro optimizations\", COUNT(*) FILTER (WHERE opt='use parallelization') AS \"parallelization\", COUNT(*) FILTER (WHERE opt='do it earlier batch throttle') AS \"earlier/batch/throttle\", COUNT(*) FILTER (WHERE opt NOT IN ('remove or reduce work','use better algorithm','use lower level system','cache and reuse','use better data structure and layout','micro optimizations','use parallelization','do it earlier batch throttle')) AS \"other\" FROM base GROUP BY difficulty ORDER BY CASE difficulty WHEN 'easy' THEN 1 WHEN 'medium' THEN 2 WHEN 'hard' THEN 3 WHEN 'unknown' THEN 4 ELSE 5 END;"
for r in con.run(sql): print(r)
con.close()
PY
```
Expected: exactly three rows (easy, medium, hard — `legacy-verified` PRs carry `difficulty='unknown'` and are excluded, so no unknown/unset row appears; this is intended), each with 9 integer columns after `difficulty`. (Reference easy row: `2786, 309, 232, 312, 191, 480, 128, 117, 310`.)

- [ ] **Step 4: Locate the insertion point in the JSON**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
grep -n '"Live Runners"' grafana/provisioning/dashboards-json/datasmith-overview.json
tail -n 8 grafana/provisioning/dashboards-json/datasmith-overview.json
```
Expected: the file ends with the `id: 101` panel object, then `    }` (panel close), `  ]` (panels-array close), `}` (root close). You will insert a comma after the panel-close `}` and add the four new objects before the `]`.

- [ ] **Step 5: Insert the row and three panel objects**

Add a comma after the final panel object's closing brace, then insert the following four objects (row first) as the last elements of the `panels` array. Keep the SQL strings on single lines exactly as written (double quotes inside Panel 3's SQL are escaped as `\"`):

```json
    {
      "type": "row",
      "title": "Dataset Composition",
      "collapsed": false,
      "gridPos": { "x": 0, "y": 172, "w": 24, "h": 1 }
    },
    {
      "id": 71,
      "title": "Fix Size Distribution (lines changed)",
      "type": "barchart",
      "gridPos": { "x": 0, "y": 173, "w": 8, "h": 8 },
      "datasource": { "uid": "supabase-pg", "type": "postgres" },
      "targets": [
        {
          "rawSql": "WITH pr_size AS (SELECT (SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int),0) FROM jsonb_array_elements(file_changes) e) AS lines_changed FROM pull_requests WHERE is_performance_commit AND file_changes IS NOT NULL) SELECT bucket, COUNT(*) AS prs FROM (SELECT CASE WHEN lines_changed < 10 THEN '1. <10' WHEN lines_changed < 50 THEN '2. 10-50' WHEN lines_changed < 200 THEN '3. 50-200' WHEN lines_changed < 1000 THEN '4. 200-1000' ELSE '5. 1000+' END AS bucket FROM pr_size) t GROUP BY bucket ORDER BY bucket;",
          "format": "table",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": { "color": { "mode": "fixed", "fixedColor": "blue" } },
        "overrides": []
      },
      "options": {
        "xTickLabelMaxLength": 0,
        "xTickLabelSpacing": 0,
        "axisCenteredZero": false,
        "axisBorderShow": false
      }
    },
    {
      "id": 72,
      "title": "Fix Size by Difficulty (median)",
      "type": "table",
      "gridPos": { "x": 8, "y": 173, "w": 8, "h": 8 },
      "datasource": { "uid": "supabase-pg", "type": "postgres" },
      "targets": [
        {
          "rawSql": "WITH pr_size AS (SELECT COALESCE(difficulty,'unset') AS difficulty, (SELECT COALESCE(SUM((e->>'additions')::int + (e->>'deletions')::int),0) FROM jsonb_array_elements(file_changes) e) AS lines_changed, jsonb_array_length(file_changes) AS files_touched FROM pull_requests WHERE is_performance_commit AND file_changes IS NOT NULL) SELECT difficulty, COUNT(*) AS prs, ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY lines_changed)) AS median_lines, ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY files_touched)) AS median_files FROM pr_size GROUP BY difficulty ORDER BY CASE difficulty WHEN 'easy' THEN 1 WHEN 'medium' THEN 2 WHEN 'hard' THEN 3 WHEN 'unknown' THEN 4 ELSE 5 END;",
          "format": "table",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          {
            "matcher": { "id": "byName", "options": "median_lines" },
            "properties": [
              { "id": "custom.cellOptions", "value": { "type": "color-background", "mode": "gradient" } },
              {
                "id": "thresholds",
                "value": {
                  "mode": "absolute",
                  "steps": [
                    { "color": "green", "value": null },
                    { "color": "yellow", "value": 100 },
                    { "color": "red", "value": 500 }
                  ]
                }
              }
            ]
          }
        ]
      }
    },
    {
      "id": 73,
      "title": "Optimization Type by Difficulty",
      "type": "barchart",
      "gridPos": { "x": 16, "y": 173, "w": 8, "h": 8 },
      "datasource": { "uid": "supabase-pg", "type": "postgres" },
      "targets": [
        {
          "rawSql": "WITH base AS (SELECT COALESCE(difficulty,'unset') AS difficulty, REPLACE(COALESCE(classification,'unclassified'),'_',' ') AS opt FROM pull_requests WHERE is_performance_commit AND classification IS DISTINCT FROM 'legacy-verified') SELECT difficulty, COUNT(*) FILTER (WHERE opt='remove or reduce work') AS \"remove/reduce work\", COUNT(*) FILTER (WHERE opt='use better algorithm') AS \"better algorithm\", COUNT(*) FILTER (WHERE opt='use lower level system') AS \"lower level system\", COUNT(*) FILTER (WHERE opt='cache and reuse') AS \"cache & reuse\", COUNT(*) FILTER (WHERE opt='use better data structure and layout') AS \"data structure/layout\", COUNT(*) FILTER (WHERE opt='micro optimizations') AS \"micro optimizations\", COUNT(*) FILTER (WHERE opt='use parallelization') AS \"parallelization\", COUNT(*) FILTER (WHERE opt='do it earlier batch throttle') AS \"earlier/batch/throttle\", COUNT(*) FILTER (WHERE opt NOT IN ('remove or reduce work','use better algorithm','use lower level system','cache and reuse','use better data structure and layout','micro optimizations','use parallelization','do it earlier batch throttle')) AS \"other\" FROM base GROUP BY difficulty ORDER BY CASE difficulty WHEN 'easy' THEN 1 WHEN 'medium' THEN 2 WHEN 'hard' THEN 3 WHEN 'unknown' THEN 4 ELSE 5 END;",
          "format": "table",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": { "color": { "mode": "palette-classic" } },
        "overrides": []
      },
      "options": {
        "orientation": "horizontal",
        "stacking": "normal",
        "xField": "difficulty"
      }
    }
```

- [ ] **Step 6: Validate the JSON parses and the new panels are present**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
jq -e '.panels | map(select(.id==71 or .id==72 or .id==73)) | length == 3' grafana/provisioning/dashboards-json/datasmith-overview.json
jq -r '.panels[] | select(.type=="row") | .title' grafana/provisioning/dashboards-json/datasmith-overview.json
```
Expected: first command prints `true` (and exits 0). Second lists row titles ending with `Dataset Composition`. If `jq` errors, the JSON is malformed — fix the comma/brace placement from Step 5.

- [ ] **Step 7: Confirm no existing panel was altered**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
git diff --unified=0 grafana/provisioning/dashboards-json/datasmith-overview.json | grep '^-' | grep -v '^---'
```
Expected: **no output** (an empty result) — the change is purely additive, so no existing line is removed. Any removed line other than a lone `}` → `},` comma flip is a mistake; investigate before committing.

- [ ] **Step 8: (If Grafana is running) reload provisioning and eyeball the row**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
docker ps --format '{{.Names}}' | grep -q datasmith_grafana && docker restart datasmith_grafana && echo "restarted — open http://localhost:3001 and scroll to 'Dataset Composition'" || echo "Grafana not running; skipping visual check (provisioning loads the JSON on next start)"
```
Expected: either a restart + a reminder to visually confirm the three panels render non-empty (Panel 1 five bars; Panel 2 table easy/medium/hard rows; Panel 3 stacked horizontal bars for easy/medium/hard), or a clean skip message. A skipped visual check is acceptable — the SQL and JSON are already verified in Steps 1–7.

- [ ] **Step 9: Commit**

```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
git add grafana/provisioning/dashboards-json/datasmith-overview.json
git commit -m "feat(grafana): add Dataset Composition row (fix size + difficulty validation)"
```
The `pretty format json` pre-commit hook may reformat the file and abort the first commit; if so, re-run `git add` on the file and commit again.

---

### Task 2: Document the new row in the monitoring guide

**Files:**
- Modify: `docs/guide/monitoring.md` (the "Dashboard Panels" table, around line 51–67)

**Interfaces:**
- Consumes: the "Dataset Composition" row title and panel titles created in Task 1.
- Produces: nothing downstream depends on this.

**Context:** `docs/guide/monitoring.md` has a "Dashboard Panels" markdown table with one row per dashboard section (`| **Section** | Panels |`). Add one row for the new section, matching the existing style (bold section name, comma-separated panel descriptions). The existing "Pull Request Insights" row is the closest sibling.

- [ ] **Step 1: Add the table row**

In the "Dashboard Panels" table, after the existing `**Data Explorer**` row (the last row of the table), add:

```markdown
| **Dataset Composition** | Fix size distribution (lines changed), fix size by difficulty (median lines/files), optimization type by difficulty |
```

- [ ] **Step 2: Verify the table still renders and the row is present**

Run:
```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
grep -n 'Dataset Composition' docs/guide/monitoring.md
```
Expected: one match inside the "Dashboard Panels" table. Confirm the line has a leading `|` and three `|`-delimited cells (section, panels, trailing pipe) like its neighbours.

- [ ] **Step 3: Commit**

```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
git add docs/guide/monitoring.md
git commit -m "docs(monitoring): document Dataset Composition dashboard row"
```

---

## Self-Review

**1. Spec coverage:**
- Panel 1 "Fix Size Distribution" → Task 1 Step 5 (id 71). ✓
- Panel 2 "Fix Size by Difficulty" → Task 1 Step 5 (id 72), rendered as a table per the spec's scale-mismatch note. ✓
- Panel 3 "Optimization Type by Difficulty" → Task 1 Step 5 (id 73), stacked horizontal barchart via SQL pivot (replaces the spec's "implementer verifies visually" transform with a deterministic wide pivot). ✓
- Layout append-at-bottom, y≥172, ids 71–73, row carries no id → Global Constraints + Task 1 Steps 4–7. ✓
- Datasource `supabase-pg`, no new grants → Global Constraints; verified by the SQL steps running through the same Postgres. ✓
- Known simplifications (whole-PR size, unknown/unset own buckets, exclude legacy-verified) → Global Constraints + Panel 3 expected-output note. ✓
- Validation (SQL runs, jq parse, provisioning reload) → Task 1 Steps 1–3, 6, 8. ✓
- Docs note in monitoring.md → Task 2. ✓
- Out-of-scope (benchmark_information/headroom, schema changes) → Global Constraints forbids them. ✓

**2. Placeholder scan:** No TBD/TODO; every step has concrete commands, SQL, or JSON. The one deliberate non-exact assertion (counts "drift") is spelled out with reference numbers, not left vague. ✓

**3. Type consistency:** Panel ids 71/72/73 and the row-with-no-id are stated identically in Global Constraints, Task 1 interfaces, and Step 5. The row title "Dataset Composition" matches between Task 1 (Step 5 JSON) and Task 2 (Step 1 doc row) and the jq check (Step 6). Column names in each panel's SQL match the reference outputs in Steps 1–3. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-02-dataset-composition-dashboard.md`.
