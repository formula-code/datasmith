# Follow-on Plans 2–4 — Design

**Date:** 2026-08-13
**Status:** design
**Branch:** `spec/build-manifest-verification`
**Predecessors:**
[`2026-07-31-build-manifest-verification-design.md`](2026-07-31-build-manifest-verification-design.md),
[`2026-08-10-verification-measurability-design.md`](2026-08-10-verification-measurability-design.md)

## Governing constraint: no public-facing figure may move

Stated by the operator: *"our reanalysis and new information shouldn't impact the
grafana dashboard figures."*

This was measured, not assumed. Five Grafana panels read `candidate_containers`
(`grafana/provisioning/dashboards-json/datasmith-overview.json`), and **every one is a
`COUNT(*)` over rows**:

| Panel | Query shape |
|---|---|
| Total Problems | `COUNT(*)` |
| PR to Problem Rate | `COUNT(*)` / PR count |
| Problems by Repository | `COUNT(*) GROUP BY owner, repo` |
| Monthly distribution of Problems | `COUNT(*)` joined to `pull_requests` |
| End-to-End Pipeline Funnel | `COUNT(*)` as the "Containers Built" stage |

Grep counts over the dashboard JSON: `manifest_warnings` **0**, `build_manifest` **0**.
`resource_metrics` appears 16 times and `test_duration_s` 3 times, but all three panels
that read them (`Build Metrics`, `Image Size vs Build Time`, `Avg Resource Metrics Over
Time`) source from **`error_logs`**, not `candidate_containers`.

**The operative rule, and it is sharp:**

> Adding or changing a **column** on an existing `candidate_containers` row is invisible
> to every public figure. **Deleting a row** moves five of them at once.

Consequences, binding on the plans below:

- **The follow-on delete is out of scope permanently, not deferred.** The predecessor spec
  frames triage as "this spec stamps; the follow-on deletes." Under this constraint the
  delete half does not happen: removing 636 of 1854 rows would drop *Total Problems* by
  34% and visibly re-rank *Problems by Repository*.
- **`--calibrate` must not write to any table.** Re-running a sample through the normal
  `_save_context` path would upsert on the same key — no row-count change, so no figure
  moves — but it would overwrite `resource_metrics` on the very rows whose original
  measurement is the evidence under audit. Calibrate writes a report file and touches no
  table.
- Plan 2 writes only to `reward.json` / `harbor_runs.reward_payload`; no panel reads
  `reward_payload`. Plan 3 deletes dead Python and rewrites docs; it touches no table.

**Operator ruling on stamp visibility.** `manifest_warnings` is anon-readable, so
`suspect_timeout_720s` becomes visible through `api.formulacode.org`. Accepted: it is an
internal quality flag rather than a claim about the dataset, the column already exists and
is currently NULL on all 1854 rows, and keeping the audit queryable in SQL beside the data
it describes is worth more than hiding it.

## Plan 3 — remove `verifiers.py` and the docs it poisoned

Ordered first: it is pure deletion with zero database contact, so it cannot threaten a
figure.

**Verified still dead (2026-08-13):** `grep -rn "\.verify(" --include=*.py src/` returns no
call site outside `verifiers.py` itself. Thirteen files still reference the API:

| Action | Files |
|---|---|
| Delete | `src/datasmith/docker/verifiers.py`, `tests/docker/test_verifiers.py` |
| Drop exports | `src/datasmith/docker/__init__.py`, `src/datasmith/__init__.py`, `src/datasmith/__init__.pyi` |
| Replace snippets | `tests/test_website_snippets.py` |
| Rewrite | `README.md`, `docs/guide/verification.md`, `docs/guide/docker-images.md` |
| Correct | `docs/design/Datasmith - Overview.md`, `docs/design/components/datasmith.docker.md`, `docs/design/components/datasmith.agents.synthesizer.md`, `docs/design/components/datasmith.agents.md` |

`docs/guide/verification.md`'s "Programmatic verification" section currently shows a
`MultiObjVerifier` snippet that would fail against every image in the registry. It is
replaced with `read_build_manifest` / `evaluate_invariants`, shown against an image that
carries a manifest **or** with the `ok=None` path explicit — the missing-manifest contract
is part of the public API and the snippet must not lie about it.

`docs/design/components/datasmith.docker.md:145` is the exception to "correct": it
diagnosed the timeout defect before it cost 619 rows. Mark it **resolved**, do not rewrite
it away. `profile.sh` is **not** removed — it is still baked into images and stored in
`candidate_containers.profile_sh`.

### `dataset/verify.py`

Two distinct defects, both confirmed present today:

1. `run_profile()` scores a timeout as success (`dataset/verify.py:119-121`). Dead code —
   nothing calls it — but it is the exact inversion this whole effort exists to remove, and
   leaving a working example of it in the tree invites its reuse.
2. `run_tests()` passes **no timeout at all** to `docker.run`, so it cannot time out and can
   hang indefinitely.

Both are fixed here: delete `run_profile()`, and give `run_tests()` a timeout that defaults
to `DATASMITH_VERIFY_TEST_TIMEOUT_S` with a host-side kill, matching `local_ci.py`.

## Plan 2 — trial-time invariants, plus the overrides table

### The overrides table is real, and it is not what the spec assumed

`rl-prep/local-overrides/formulacode_task_overrides.json` exists and holds **5 records**.
Its actual keys:

```
benchmark_dest, benchmark_storage_key, extra_dockerfile_commands,
extra_entrypoint_commands, issue_number, oracle_h, owner, repo,
pip_pins, restore_regex
```

**There is no `expected_n` field**, and `grep -rl expected_n rl-prep/` returns nothing. The
predecessor spec's assumption — that `expected_n` "is a declared field on the override row"
— is false. All five tasks exist in `pull_requests`; one (`networkx#8148`) already has a
`candidate_containers` row.

| owner/repo#issue | benchmark_dest | oracle_h |
|---|---|---|
| pvlib/pvlib-python#369 | `benchmarks/benchmark_clearsky.py` | 22.563 |
| joblib/joblib#484 | `benchmarks/benchmark_sequential.py` | 1.952 |
| networkx/networkx#8148 | `benchmarks/benchmarks/benchmark_aperiodic.py` | 1.323 |
| python-hyper/h11#34 | `bench/benchmarks/bench_keepalive_loop.py` | 1.203 |
| shapely/shapely#2359 | `benchmarks/benchmark_prepare.py` | 2.34 |

### Migration `00026_formulacode_task_overrides.sql`

Numbered **00026**: this tree's migrations run 00001–00023 plus 00025, and `00024` is
claimed by a separate working tree per `00025`'s header. Taking 00026 avoids both.

```sql
create table if not exists formulacode_task_overrides (
  owner                     text    not null,
  repo                      text    not null,
  issue_number              int     not null,
  benchmark_dest            text,
  benchmark_storage_key     text,
  extra_dockerfile_commands text,
  extra_entrypoint_commands text,
  pip_pins                  text[],
  restore_regex             text,
  oracle_h                  numeric,
  expected_n                int,      -- hand-declared; NULL means "not judged yet"
  notes                     text,
  created_at                timestamptz not null default now(),
  updated_at                timestamptz not null default now(),
  primary key (owner, repo, issue_number)
);
```

**Private by default.** No RLS policy, no `anon` grant — migration `00015` revoked the
default broad `anon` SELECT and a new table stays private unless it is genuinely meant to
be public. This one is operator tooling, so it stays private. That also keeps it entirely
outside the public-surface constraint.

`expected_n` is **hand-declared and nullable**, per the operator's ruling. It is a human
judgement about how many benchmarks a PR *should* touch — which is what a dilution
threshold actually is — so it is not derived. The five seed rows land with `expected_n`
NULL, and #18 correctly skips for them until someone fills it in. This is the honest
version: the check is live, and its input is absent for a stated reason.

Seeding is a **separate idempotent script**, `scripts/seed_task_overrides.py`, dry-run by
default with `--apply` to write, following `backfill_tainted_containers.py`'s convention.
It reads the rl-prep JSON, which is untracked, so the script must tolerate its absence
rather than assuming it.

### `benchmark_dest_missing` comes back to life

`benchmark_dest` is the input to the FATAL `benchmark_dest_missing` invariant, which has
been **inert since the previous run** because nothing in the tree sets `$BENCHMARK_DEST`.
With the overrides table populated, `synthesize_images` exports it into the build so
`docker_build_run.sh`'s existing conditional breadcrumb fires.

This is a **behavior change with real risk**, and it is the one part of Plan 2 that can
reject containers: a task with a declared `benchmark_dest` whose file does not survive
`git clean` will now hard-fail stage 6 where it previously passed. That is the entire point
of the gate — it catches the joblib/`asv_benchmarks.txt` wipe — but it applies only to the
5 tasks that have an override row, and only when the file is genuinely missing. Every task
without an override row still skips, exactly as today.

### The six trial-time invariants

Asserted in `harbor_adapter/template/parser.py`, echoed into `reward.json` under a new
`invariants` block. `harbor_runs.reward_payload` already stores the whole object, so this
needs no migration and no schema change.

| # | Invariant | Sev | Catches |
|---|---|---|---|
| 15 | `baseline_sha == base_commit` | FATAL | shapely: baselines measured *after* the patch → H≈1.0 |
| 16 | target benchmark baseline finite and non-zero | FATAL | degenerate-baseline reward garbage |
| 17 | geomean oracle speedup ≥ 1.0 | warn | h11's apparent 0.633 slowdown |
| 18 | `impacted_n / expected_n` ≤ ratio max | warn | networkx measuring 140 when 10 expected |
| 19 | snapshot-vs-ASV constant factor within bounds | warn | h11's ~0.5× systematic bias |
| 20 | baseline provenance: pre-baked cache vs fresh measure | warn | optuna's cold-oracle-vs-warm-agent free ~1.5× |

**#15 has no input today — this is a producer gap, and it must be closed in the same
change.** `parser.py`'s argparse takes `--owner`, `--repo`, `--issue-number`, `--agent-key`
and nothing else; `base_commit` never reaches it. `test.sh` already has `{{ base_commit }}`
as a Jinja variable and passes it to `lsv_measure.py`, so it passes it to `parser.py` too
via a new `--base-commit`. Without that, #15 ships inert — the exact defect class the brief
names non-negotiable.

**#17 is warn, not FATAL, deliberately.** A geomean below 1.0 on the *oracle* means the
task's premise is wrong, but at trial time the run has already been paid for; failing it
changes nothing and would discard a trial whose data is what proves the problem. Recorded
and surfaced.

Severity here means something different from build time: a FATAL trial-time invariant does
not fail a build, it marks the trial's reward as untrustworthy. The `invariants` block
carries `{ok, fatal, warnings, skipped}` so `harbor_healthcheck`'s row builder can act on
it later without re-deriving anything.

### Tunable constants

| Constant | Default | Used by |
|---|---|---|
| `DATASMITH_VERIFY_DILUTION_RATIO_MAX` | `3.0` | #18 (already declared in `manifest.py`) |
| `DATASMITH_VERIFY_CONSTANT_FACTOR_MIN` | `0.5` | #19 |
| `DATASMITH_VERIFY_CONSTANT_FACTOR_MAX` | `2.0` | #19 |

`parser.py` runs inside the trial container with no `datasmith` installed, so it reads these
from `os.environ` at module scope with literal fallbacks — it cannot import
`datasmith.docker.manifest`.

## Plan 4 — audit the timeout-verified cohort

`scripts/audit_timeout_verified.py`, modeled on `backfill_tainted_containers.py`: dry-run by
default, `--apply` to write, backup to `backups/` first.

**Measured today, not taken from the spec:**

- **636** rows at `test_duration_s >= 720`, across **54** distinct repos, out of 1854 total.
- **Zero** of those 636 have a `harbor_runs` row.

That second number kills the spec's ranking scheme. It says to rank "by whether a
`harbor_runs` row already exists," on the logic that a suspect container which already
produced a good oracle trial is lower priority. The join is sound — 7 containers do have
`harbor_runs` rows, and all 7 are fast rows under 720s — so the correct conclusion is that
**every suspect row is equally unvalidated**, and the only useful ranking axis is by repo
(which repos dominate the cohort, so a fix targets the most rows).

### What it writes

`manifest_warnings += 'suspect_timeout_720s'` on matching rows — array append, never
replace, so a row that already carries warnings keeps them. No row is created or deleted, so
no Grafana figure moves. Verified: no dashboard panel reads `manifest_warnings`.

### `--calibrate N` writes no table

Re-runs `N` rows sampled from the slowest repos at `DATASMITH_VERIFY_TEST_TIMEOUT_S=3600`
and reports where completions actually land — the only way to learn the true tail, since the
existing data is censored at 720.

Output goes to `backups/calibrate_timeout_<timestamp>.json` **and nowhere else.** It must
not route through `_save_context`: that would upsert `candidate_containers` on the same key,
silently overwriting the `resource_metrics` of the rows whose original measurement is the
evidence being audited. No figure would move, but the audit would destroy its own evidence.

**Calibration is now materially more expensive than the spec assumed.** A re-run at the
raised limit also pays the measure step (~830 s median, ~1550 s p90) and faces three new
FATAL gates. So calibrate measures two things at once — the true test-duration tail *and*
the new gates' rejection rate on real containers — and its report records both. A dozen rows
is enough to distinguish "most finish by 900 s" from "the tail runs past an hour."

## Testing

| Layer | What | Docker |
|---|---|---|
| unit | each of the 6 trial-time invariants: fires / skips / holds | no |
| unit | producer coverage — every key the invariants read is emitted by `parser.py`, driven off the invariant registry | no |
| unit | `--base-commit` reaches `parser.py`, so #15 can fire (fails if the arg is dropped) | no |
| unit | overrides seeding is idempotent; tolerates the untracked JSON being absent | no |
| unit | audit script: cohort selection, array-append semantics, dry-run writes nothing | no |
| **regression** | **the audit changes no `COUNT(*)`** — snapshot all five Grafana panel counts before and after `--apply`, assert identical | no |
| regression | `dataset/verify.py::run_tests` actually times out | yes, seconds |
| integration | `benchmark_dest` flows override → build arg → breadcrumb → manifest | yes |

The Grafana-invariance test is the one that encodes the operator's constraint as an
executable check rather than a promise. It runs the five panels' actual SQL against the
local database, and if a future change to the audit starts deleting rows it fails
immediately.

## Out of scope

- **Deleting or re-synthesising the 636 rows.** Permanently out under the public-figure
  constraint, not deferred.
- **Populating `expected_n`.** The column ships nullable; filling it is a human judgement
  per task.
- Wiring `profile.sh` back into verification.
- RLVR-side concerns (pass@k, GRPO band, whether an H is interesting).
