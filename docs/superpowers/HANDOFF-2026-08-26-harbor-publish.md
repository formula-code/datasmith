# Handoff: local Harbor verification and DockerHub publication

You are taking over the **downstream half** of the FormulaCode container
pipeline. Another agent is still running stage 6 continuously and adding rows
to `candidate_containers`; your job is everything after that. Do not run
stage 6 and do not stop the grind.

Read `CLAUDE.md` first. Work in `/mnt/sdd1/atharvas/formulacode/datasmith_new`
on branch `spec/ingestion-window`.

## Decisions already made by the operator

You do not need to relitigate these:

* **Local Docker Harbor runs are the evidence of record.** Daytona is
  impractical right now. The publish gate has been made a tunable to admit
  them (see #1); the speedup requirement is unchanged.
* **Overwriting the published DockerHub tags is authorised** (see #2). Record
  digests.

Everything else — tagging beyond the canonical tag, rebuilding the one missing
image, raising measurement rounds — is yours to decide, but say what you chose.

## Your goal

For every `candidate_containers` row with `verification_state='verified'`:

1. Run Harbor **locally** (stage 7, `--harbor-environment docker`) to get a
   `harbor_runs` row.
2. Keep only the tasks whose measured speedup is **positive** — the pipeline's
   own threshold is `MIN_HARBOR_SPEEDUP = 1.05` in
   `src/datasmith/publish/records.py`.
3. Push those containers to DockerHub and report the public URLs.

## Start here

```bash
# The current verified set, with the tag and the local LSV max_speedup.
uv run python scripts/goal_check.py           # condition 1 prints the breakdown
```

```python
from datasmith.utils.db import get_client
c = get_client()
rows = c.table("candidate_containers").select(
    "owner,repo,sha,task_id,build_manifest->verify"
).eq("verification_state", "verified").execute().data
```

The set grows while you work — re-read it, don't cache it.

## Four things that will bite you

### 1. Local Docker evidence is authorised — the knob is already in place

The operator has decided: Daytona is impractical right now, **local Docker
trials are the evidence of record for this batch.**

`publish/records.py` used to hardcode `filters={"environment": "daytona"}`,
which would have dropped everything you produce. It is now a tunable:

```python
DATASMITH_PUBLISH_ENVIRONMENTS: tuple[str, ...]   # default ("daytona",)
```

Run stage 8 with:

```bash
DATASMITH_PUBLISH_ENVIRONMENTS=docker,daytona uv run fc-data \
  --start-date 2017-01-01 --end-date 2030-12-31 --stage 8
```

The default is deliberately unchanged, so nobody inherits this decision by
accident. Four tests in `tests/publish/test_records.py` pin the behaviour,
including one asserting that widening the environment set does **not** move
`MIN_HARBOR_SPEEDUP`.

**Do not touch `MIN_HARBOR_SPEEDUP = 1.05`.** Admitting local trials trades
measurement quality for reach; it must not also drop the requirement that a
container speeds something up. That constant is the dataset's only positive
guarantee.

State in your report which environments you admitted. A published record
cannot say so for itself, and someone will later ask why these numbers came
from a shared build host.

### 2. Overwriting the published tags is authorised — but record the digests

The operator has approved overwriting. Push to the canonical
`formulacode/<owner>-<repo>:<issue>` tags.

Know what you are replacing. Those tags already resolve on DockerHub from an
earlier generation and the digests differ — verified 2026-08-26:

| tag | local (verified) | remote (about to be replaced) |
|-----|------------------|-------------------------------|
| `formulacode/bluesky-tiled:1283` | `sha256:fabba18c…` | `sha256:6902bc69…` |
| `formulacode/numpy-numpy-financial:96` | `sha256:09d672c8…` | `sha256:b168417a…` |

The outgoing generation predates the host-side integrity scan and is the
corpus that was marked `unverified` wholesale. Replacing it is the intent.

Because the overwrite is silent from a consumer's point of view, **record the
pushed digest for every tag** (`docker inspect --format '{{.Id}}'` before the
push, and the registry digest after). That list is the only way anyone can
later tell which generation they pulled.

### 3. One verified image no longer exists locally

`formulacode/pydata-bottleneck:305` (sha `445d3768642a`, local max_speedup
2.52) is **not present on this host** — the tag was reused by a later build.
That row cannot be published without a rebuild. Either rebuild it from the
stored context in `candidate_containers`, or exclude it and say so. Do not
publish the remote image of that name and call it verified; it is a different
build.

Re-check presence before every push:

```bash
docker image inspect <tag> >/dev/null 2>&1 || echo "MISSING: <tag>"
```

### 4. Containers leak if a run times out — check before you blame the machine

`battery.py` was fixed on 2026-08-25 to name its containers and force-remove
them on `TimeoutExpired`; before that, 13 containers survived up to 274
minutes against a 40-minute timeout and held the host at load 372 on 128
cores. If throughput collapses, check for leaks first:

```bash
docker ps --filter "name=fc-" --format '{{.Names}} {{.RunningFor}}'
```

Anything past 90 minutes is a leak (test and measure each cap at 3600s and run
as separate containers, so ~60 min is the honest maximum). A reaper monitor may
already be running; ask before starting a second one.

## Commands

```bash
# Stage 7, local Docker, pinned to specific tasks
uv run fc-data --start-date 2017-01-01 --end-date 2030-12-31 --stage 7 \
  --harbor-environment docker --n-concurrent 4 \
  --tasks owner/repo#PR,owner/repo#PR

# Inspect what it wrote
#   harbor_runs: max_speedup, geomean_speedup, n_benchmarks, status, environment
```

```bash
# Stage 8, admitting local Docker evidence. Pushes to DockerHub AND uploads
# the HuggingFace record; publish_pipeline(dockerhub_push=..., hf_publish=...)
# can disable either if you want DockerHub only.
DATASMITH_PUBLISH_ENVIRONMENTS=docker,daytona uv run fc-data \
  --start-date 2017-01-01 --end-date 2030-12-31 --stage 8

# Dry run first — prints what WOULD be published, runs nothing:
DATASMITH_PUBLISH_ENVIRONMENTS=docker,daytona uv run fc-data \
  --start-date 2017-01-01 --end-date 2030-12-31 --stage 8 --dry-run
```

Run the dry run first every time. Stage 8 publishes to two external services
and marks rows `published_at`; there is no undo.

DockerHub credentials are read from `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`
(`docker/publish.py`); `tokens.env` already defines three `DOCKERHUB_*` vars
and is auto-loaded at import.

**Ordering.** Stage 8 selects from `pull_requests` by the merged_at window and
`published_at IS NULL`, then drops anything without a qualifying `harbor_runs`
row. So: stage 7 first for every verified task, confirm the `harbor_runs` rows
landed, dry-run stage 8, then publish.

## What "verified" already guarantees, and what it does not

`verification_state='verified'` means: the image built, the **host-side** image
scan found no tampering, the verifier accepted the battery, a manifest was
sealed, and measurement ran (`measure_timed_out`, `asv_exec_failed`,
`oracle_patch_failed` all passed).

It does **not** mean a speedup was demonstrated. All three measurement
invariants check that measurement *happened*. Roughly a third of verified rows
measure at or below parity. That is precisely why your stage-7 pass exists.

Treat the manifest's `max_speedup` as a hint for ordering work, not as
evidence:

* it is computed over the patch-affected benchmarks that produced a usable
  ratio — not all benchmarks, and not the full affected set;
* `benchmarks_impactable_n` (from `lsv_init`, pre-patch) and
  `benchmarks_degenerate_n` (from `lsv_measure`, post-patch) come from
  different phases and do not reconcile;
* **`rounds` is 2 on every row.** `measure.sh`'s own comment derives the
  significance floor from `asv._stats.is_different` — 5+5 gives p_min 0.00397,
  above the 0.002 threshold, so **6+6** is the target. At 2 rounds LSV cannot
  call a difference real. Consider `DATASMITH_VERIFY_MEASURE_ROUNDS=6` for your
  Harbor pass, and expect it to cost wall-clock (bottleneck already takes
  ~3000s at 2 rounds).

Some rows are thin regardless: `xdslproject/xdsl#1332` and `UXARRAY/uxarray#71`
each measured a **single** benchmark; `xarray-contrib/flox#172` had 56 of 88
benchmarks come back degenerate. `lsv_measure.py` records an explicit
`measure_error` when LSV measures *zero* benchmarks, but has no guard for
partial degeneracy, so a mostly-degenerate container passes silently. Report
the degenerate fraction alongside every speedup you publish.

## Rules

* Never `git add -A` / `git add .` / `git commit -a` — other sessions share
  this branch. Stage explicit paths.
* Never `docker volume prune` — the local Supabase database lives in a volume.
  If Supabase is unreachable, `npx supabase start`; never
  `supabase stop --no-backup`.
* `pyproject.toml` sets `fix = true`; always use `ruff check --no-fix`.
* Pushing to DockerHub is outward-facing and hard to reverse. Confirm the
  tagging scheme with the operator before the first push, then proceed.
* `scripts/pv_validate.py` pins 16 images **by digest** and skips a case whose
  tag has moved. Do not rebuild or re-tag any task in `pv_validate.CASES`.
  Check with:
  `uv run python -c "import sys; sys.path.insert(0,'scripts'); import pv_validate; print([c.task for c in pv_validate.CASES])"`

## Report back

Per task: local `max_speedup` and degenerate fraction, the Harbor
`max_speedup` / `geomean_speedup` / `n_benchmarks` / `status`, whether it
cleared 1.05, and — if pushed — the full image reference with its digest.
State plainly which tasks you dropped and why.
