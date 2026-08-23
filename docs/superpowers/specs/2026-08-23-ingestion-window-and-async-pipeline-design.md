# Ingestion window correctness and a genuinely async pipeline (stages 2–5)

**Status:** design approved in conversation, not implemented
**Date:** 2026-08-23
**Branch:** `spec/build-manifest-verification` (implementation gets its own branch)
**Scope:** pipeline stages 2 (`scrape_commits`), 3 (`classify_prs`), 4 (`resolve_packages`), 5 (`render_problems`)

---

## 1. Goal

Make stages 2–5 collect the commits they claim to collect, report failure when
they fail, and use the concurrency they advertise.

Everything in this document comes from one observation: a stage 2 run over
2026-08-01 reported 154/154 repositories, zero failures, and 35 stored PRs.
Ground truth for that window is 81. The run was not healthy; it had no way to
say so.

Stages 6–9 are out of scope. See section 10.

---

## 2. Evidence

Every number below was measured against the live GitHub API and the project
database on 2026-08-23. They are reproduced here because the design decisions
rest on them.

### 2.1 Stage 2 loses 57% of in-window PRs

`paginate_merged_prs` orders by `CREATED_AT DESC`. `_should_skip_pr` halts
pagination on the first PR with `created_at < since`
(`runners/scrape_commits.py:33-36`) but accepts on `merged_at` in
`[since, until)` (`:41-46`). Stage 2 therefore captures only PRs **created and
merged** inside the window.

For 2026-08-01 across the 154 tracked repositories:

| | count |
|---|---|
| merged in window (GitHub search `merged:2026-08-01..2026-08-01`) | **81** |
| created before the window, dropped by the stop condition | **46** |
| stage 2 should therefore find | 35 |
| stage 2 actually found | **35** |

The match is exact, so the model of the defect is complete and no loss is
unexplained.

The loss is biased rather than random. Whole repositories disappear:
devito 6 of 6, newton-physics 3 of 3, sunpy 2 of 2, dascore 3 of 4. Repositories
that merge same-day survive intact (django-components 5 of 5). Projects with a
deliberate review process are systematically excluded, and that is where
considered performance work lives. For a performance benchmark this bias runs
in the wrong direction.

### 2.2 Stages 3–5 filter a different column

| stage | filter | file |
|---|---|---|
| 2 | `merged_at` in `[since, until)` | `runners/scrape_commits.py:41-46` |
| 3 | `created_at` between `gte`/`lte` | `update/pipeline.py:387-388` |
| 4 | `created_at` between `gte`/`lte` | `update/pipeline.py:436-437` |
| 5 | `created_at` between `gte`/`lte` | `update/pipeline.py:487-488` |

Stage 2 is half-open. Stages 3–5 are inclusive at the upper bound.

The two defects mask each other exactly. Verified: zero stored rows have
`created_at` earlier than the window start, because stage 2 never fetched any.
Repairing stage 2 alone would scrape more PRs that then never advance. This is
one change across four files.

### 2.3 A failure is indistinguishable from an empty result

`GitHubClient.graphql` (`github/client.py:247-256`) bypasses `_request`
entirely: no retry, no token rotation, and no inspection of `result["errors"]`.
GitHub returns HTTP 200 with an `errors` array for `RATE_LIMITED` and
`NOT_FOUND`. `paginate_merged_prs` then returns bare when `data.repository` is
null (`:299-300`).

Demonstrated: a nonexistent repository returns HTTP 200 plus a `NOT_FOUND`
error, `paginate_merged_prs` yields `[]`, and the stage logs
`Scraped 0 merged PRs` and counts the repository as a success.

`_request` handles 403/429 by reading `X-RateLimit-Reset`
(`github/client.py:71-78`). GitHub's *secondary* rate limit sends `Retry-After`
and frequently no reset header, so that path burns three fast retries and
raises.

`_request` also returns `None` for 404, 406, 410, and 451, and `get_diff`
flattens all of it to `""` (`:174-183`). An unavailable diff and a failed
request are the same value.

A repository audit found the tracked list itself is mostly healthy — 153 of 154
resolvable, zero `NOT_FOUND` — but `pymc-devs/pymc3` was renamed to
`pymc-devs/pymc` and GitHub follows the redirect silently, so rows land under
the stale name. Two repositories are archived and will always return zero.

### 2.4 Concurrency is structural, not actual

Every runner calls the synchronous PostgREST client inside `async def`:
`scrape_commits.py:135,160`, `classify_prs.py:44`, `resolve_packages.py:57`,
`render_problems.py:92,108`, and `base.py:73,92,105`.

Measured on 8 repositories, identical queries:

| | time |
|---|---|
| sync `fetch_all` inside `async`, semaphore 32 | 2.97 s |
| async client, genuinely concurrent | 1.01 s |

`--n-concurrent 32` admits 32 coroutines that then serialize on the event loop
at every database call.

`get_async_client()` (`utils/db.py:70`) constructs a new client on every call,
unlike the memoised `get_client()`.

### 2.5 Skip sets scale with the table, not the work

`scrape_commits.py:135` reads every existing `issue_number` for a repository to
build a skip set. For pandas that is 25,802 rows. `pull_requests` holds 265,181
rows, so a run pulls approximately the whole table to decide 35 upserts, which
accounted for roughly 18 s of the 56 s run.

Round-trip latency, measured both ways because both are supported paths:

| endpoint | ping (median) | pandas skip set |
|---|---|---|
| `https://db.formulacode.org` (CF Access) | 32.7 ms | 1.87 s |
| `http://127.0.0.1:54321` | 5.1 ms | 0.98 s |

`pull_requests` carries only its primary-key index, so both `merged_at` and
`created_at` filters sequentially scan 265,181 rows in roughly 300 ms.

The same unfiltered-read shape appears in stage 4 (`packages`,
`pipeline.py:454`) and stage 5 (`packages`, `candidate_prs`, and
`_fetch_repo_descriptions`, which reads every row of `repositories` and filters
in Python).

Selecting `patch` table-wide is not merely slow: it terminated PostgREST with
`out of memory — cannot enlarge string buffer containing 1073741822 bytes`.

### 2.6 The pre-screen must stay loose

The title filter is deliberately permissive: a positive keyword **or** the
absence of a negative one (`filters.py:108-116`). Measured against 13,008 PRs
that the LLM classifier previously confirmed as performance commits:

| title filter | recall | true perf PRs missed |
|---|---|---|
| **loose (current)** | **99.5%** | 64 |
| strict (positive AND not-negative) | 40.8% | 7,707 |
| positive keyword only | 43.2% | 7,391 |

Tightening the filter to reduce REST spend would discard 59% of the dataset.
Confirmed perf PRs carry titles such as `Pipeline`, `Refactor randoms`,
`Chunkless`, and `Dev`. The loose filter is correct and the REST cost that
follows from it is a designed-in property, not a defect to optimise away.

### 2.7 How the symbolic filter actually decides

Measured on 4,000 PRs merged since 2024 that have stored patches:

| component | rejects | needs the diff |
|---|---|---|
| title filter | 40.4% | no |
| file compliance | 4.3% | no |
| patch size | 3.0% of all, 5.5% of survivors | yes |

All 121 patch-size rejections were "too large"; none were "too small".
`MAX_PATCH_TOKENS` is 16,000 and `PerfClassifier.truncate_patch` truncates to
`DSPY_MAX_TOKENS`, also 16,000. The gate exists to protect the classifier.

### 2.8 Diff availability is a weak signal

The belief that older repositories frequently lack diffs is not supported by
the stored data. Of 265,181 rows, 550 (0.2%) have an empty patch, and the rate
is flat across the corpus:

| merge year | patch missing |
|---|---|
| 2017 | 0.1% |
| 2018–2022 | 0.1–0.2% |
| 2023–2026 | 0.2–0.3% |

Size does not explain it either: for the 434 rows with files present but no
patch, median changed lines is 1,162 and only 10 exceed 20,000 lines.

Caveat: because `get_diff` collapses 404/406/410/451 into `""` (section 2.3),
an unknown share of those 550 are masked failures rather than absent diffs. The
two cannot be separated from stored data. This is one reason section 5 changes
`get_diff`'s contract.

Consequence: "a diff exists" discriminates 0.2% of the population and costs one
REST call per PR to learn. A `HEAD` request is not cheaper — measured, it
decrements `X-RateLimit-Remaining` by exactly 1, the same as a `GET`. An
existence check is therefore strictly worse than fetching the diff.

### 2.9 Budget

| resource | per token per hour | July 2026 need |
|---|---|---|
| GraphQL points | 5,000 | ~170 (2 points per 100-PR page) |
| REST core | 5,000 | ~5,616 diff fetches |

The token pool currently holds **1** token, and stays at 1 by decision
(section 3). A monthly run therefore exceeds the REST budget and must wait out
at least one reset. Waiting is a correctness requirement, not an optimisation.

Volumes: the toy window 2026-08-01..2026-08-03 is 461 merged PRs across 55
active repositories. July 2026 is 8,407 across 101, of which one repository
(PostHog, 5,166) exceeds the search API's result cap.

---

## 3. Decisions

Taken in conversation on 2026-08-23.

| # | decision |
|---|---|
| 1 | The window means **`merged_at`**, half-open `[start, end)`, in stages 2–5 |
| 2 | Backfill of the 265,181 existing rows is deferred; fix forward and verify first |
| 3 | Stage 2 stores **every** merged PR; the pre-screen gates only the diff fetch |
| 4 | On truncation, fail that repository and continue the stage |
| 5 | Monthly batch that runs to completion; **no streaming pipeline** |
| 6 | One spec covering all five root causes |
| 7 | The token pool stays at **1** token |
| 8 | Resume uses a cheap indexed skip predicate; no work-ledger table |
| 9 | The diff fetch moves from stage 2 to **stage 3** |
| 10 | The code stays general purpose: any operator with the key, on any machine |

Decision 5 removes streaming from scope. Stage barriers are appropriate for a
monthly batch, and the work is to make each stage genuinely concurrent.

Decision 6 was taken against a recommendation to split correctness from
performance. The attribution risk is recorded in section 9.

---

## 4. Stage responsibilities and the window contract

### 4.1 One window definition

All four stages filter `merged_at`, half-open `[start, end)`.

`fetch_all` currently offers `filters`, `is_null`, `gte_filters`,
`lte_filters`, and `neq_filters` — there is no strict less-than. The design
adds `lt_filters`, rather than approximating with `lte` and a subtracted
second, because the boundary needs to mean the same thing in the database as it
does in the GitHub query.

### 4.2 Stage 2 asks GitHub the right question

`paginate_merged_prs` is replaced by a search-based fetcher. Instead of paging
every repository by creation order and filtering client-side, stage 2 issues
`repo:{owner}/{name} is:pr is:merged merged:{start}..{end - 1s}`.

`merged:` accepts ISO-8601 timestamps, not only dates. Verified: a single July
day returns 221, and its two half-day shards return 76 and 145. Bisection can
therefore reach one-second granularity, which makes the fetcher lossless at any
repository volume.

`IssueOrderField` offers no `MERGED_AT`, which is why the repository connection
cannot express this query and the search API is used instead.

### 4.3 Two truncation defences

**The search API silently truncates at 1000.** Verified against PostHog's July:
`issueCount` reports 5,166, pagination ends with `hasNextPage=False` after
exactly 1,000 nodes, and no error is raised. This is the same failure class as
the defect being repaired.

The fetcher therefore reads `issueCount` from the first page. If it exceeds the
cap it splits the time range at the midpoint and recurses. Otherwise it
paginates to exhaustion and asserts `len(nodes) == issueCount` for that leaf,
raising `Truncated` on mismatch.

The assertion is per leaf and never on a sum across shards, because GitHub's
`A..B` is inclusive at both ends. Subtracting one second from the exclusive
upper bound keeps shards disjoint; measured duplicate count across PostHog's
seven bisections was zero.

**`files(first: 100)` truncates at 100.** `check_file_compliance` guards on
`len(file_changes) >= MAX_FILES_CHANGED` (500) and
`sum(additions + deletions) >= MAX_TOTAL_CHANGES` (40,000). A 100-node cap
makes the first guard unfireable and the second undercount.

Measured on the toy window: 5 of 461 PRs (1.1%) have
`files.totalCount > len(nodes)`, one has `totalCount = 3000`, and that one PR
flips its verdict — it passes the screen on the truncated list and must not.

When `totalCount > len(nodes)`, the fetcher falls back to REST `get_files` for
that PR. Cost is roughly 5 calls per three-day window, and the guard keeps
exactly the honesty it has today.

### 4.4 Stage 2 becomes GraphQL-only

Stage 2 spends no REST budget except the ~1% `files` fallback. It stores every
merged PR and writes `is_performance_commit_symbolic` from the two components
that are free from GraphQL: the title filter and file compliance.

### 4.5 Stage 3 gains the diff

For each PR it is about to classify, stage 3 fetches the diff, applies
`check_patch_size`, and only then calls the LLM. A PR failing the size gate is
recorded without an LLM call. A PR whose diff is genuinely unavailable is
recorded as such, distinctly from a failed request.

This placement follows from section 2.7: the gate exists to protect the
classifier, so it belongs with the classifier. It follows from section 2.8 that
nothing of value is lost by not testing diff existence earlier.

Expected effect: stage 2 falls from 56 s for one incorrect day to 16 s for
three correct days, roughly 3 minutes for a month. The ~5,616 REST calls for
July move to stage 3, are spent only on PRs that get classified, and their
rate-limit waits overlap with LLM latency.

---

## 5. Failure visibility

| change | reason |
|---|---|
| `graphql()` inspects `result["errors"]`, handled per `path` | section 2.3; partial data plus scoped errors must not fail a whole response |
| `RATE_LIMITED` reports to the token pool and retries with backoff | it arrives as HTTP 200, so `raise_for_status` never sees it |
| `NOT_FOUND` fails that repository | today it is silently an empty result |
| `_request` reads `Retry-After` | secondary limits omit `X-RateLimit-Reset`; with one token this is normal operation |
| `paginate_merged_prs`'s replacement raises instead of returning bare | produces a `runner_failures` row, per decision 4 |
| `get_diff` returns status, not `""` | stage 3 must distinguish absent from failed; also unblocks the section 2.8 caveat |
| each stage logs total / succeeded / failed plus distinct error types | `BaseRunner` catches everything, so silence currently reads as success |
| stage 2 compares `nameWithOwner` to the requested name and warns | catches silent renames such as `pymc3` → `pymc` |
| archived repositories are logged as expected zeros | distinguishes "correctly nothing" from "nothing observed" |

`BaseRunner`'s catch-and-continue behaviour is retained by decision 4. What
changes is that the outcome becomes visible.

---

## 6. Async data layer and resume

### 6.1 Genuine concurrency

`db.py` gains async siblings (`afetch_all`, `abatch_upsert`) and the runners use
them. `get_async_client()` becomes a per-event-loop singleton, matching
`get_client()`.

`scrape_commits` replaces its per-PR upsert inside the page loop with one
batched upsert per repository.

### 6.2 Window-scoped skip predicates

Full-table skip-set reads are replaced by predicates scoped to the window and
backed by indexes. Resume then requires no new state, per decision 8:

| stage | skip when |
|---|---|
| 2 | a `pull_requests` row exists for `(owner, repo, issue_number)` in the window |
| 3 | `is_performance_commit IS NOT NULL` |
| 4 | a `packages` row exists for `(owner, repo, sha)` |
| 5 | a `candidate_prs` row exists for `(owner, repo, issue_number)` |

Stage 5's `_fetch_repo_descriptions` stops reading every `repositories` row and
filters server-side.

### 6.3 Migration 00027

Adds the indexes the new predicates need on `pull_requests`, which today has
only its primary key.

Number 00027 is claimed because 00026 is the highest present on any branch and
00024 is absent from all of them — it lives in the separate working tree noted
in 00025's header. The migration header records this.

The migration grants nothing to `anon`. `pull_requests` is already
anon-readable via `00012`/`00015`; this migration only adds indexes.

### 6.4 General-purpose operation

Decision 10 requires that any operator with the key can run this on any
machine, so no host-specific assumption is permitted.

- **Both database paths stay first class.** Local Postgres and
  `db.formulacode.org` through CF Access are both supported. The 32.7 ms versus
  5.1 ms measurement is not a reason to prefer one; it is a reason to reduce
  round trips, which sections 6.1 and 6.2 do. A 25,802-row skip set is
  expensive over either path.
- **Every knob is `DATASMITH_`-prefixed and env-overridable**, per the tunable
  constants rule in CLAUDE.md: concurrency caps, pacing interval, bisection
  threshold, page size. A larger server edits `tokens.env`, not source.
- **Concurrency is explicit, never implicit `cpu_count()`.** Stage 4's
  `run_in_executor(None, ...)` inherits the default pool sized
  `min(32, cpu + 4)`, which silently means something different per machine and
  on a 128-core host becomes 32 concurrent git clones and `uv pip compile` runs.
  It gets an explicit bounded pool with a `DATASMITH_` default.
- **The two budgets are separate knobs.** Cores scale stage 4. The GitHub token
  scales stages 2 and 3 and does not care about machine size. Separating them
  stops an operator raising one dial and tripping secondary rate limits.
- **Concurrent writers are safe enough.** Two machines may process overlapping
  windows. Upserts are keyed on the primary key and idempotent, so a duplicated
  fetch costs work but not correctness. `runner_progress` and `runner_failures`
  are keyed by a per-run `runner_id` and do not collide. The
  read-skip-set-then-write pattern is a read-modify-write race whose worst case
  is duplicated effort, not a corrupt row.
- **Preflight reports what the machine has**: token pool size, measured
  database round-trip latency, and resolved concurrency caps.

### 6.5 A guardrail on large columns

`fetch_all` has no protection against selecting a large text column
table-wide, and doing so killed PostgREST (section 2.5). Reads of `patch` must
carry a filter or use a server-side aggregate.

---

## 7. Testing

CI runs `make check` plus tests on Python 3.11 and 3.12, and `make test` runs
`-m "not slow"`. Anything touching the network is marked `slow`. `src/` avoids
3.12-only syntax; mypy runs strict with `disallow_untyped_defs`.

### 7.1 Invert the test that encodes the defect

`tests/runners/test_scrape_commits.py::test_early_termination` currently
asserts that PR #3 — created 2024-06-10 and merged 2024-06-10, both inside the
window — must **not** be stored, because pagination stopped first. The suite
locks in the data loss. Inverting this test is the first change and the one
that proves the repair is real.

### 7.2 New offline unit tests

Against fixtures, not GitHub:

- bisection triggers above the cap and stops once a shard fits
- a leaf whose node count disagrees with `issueCount` raises `Truncated`
- a `files` payload with `totalCount > len(nodes)` triggers the REST fallback
- a GraphQL body carrying `errors` is not treated as an empty result
- `RATE_LIMITED` retries; `NOT_FOUND` fails that repository
- `_request` honours `Retry-After`
- `get_diff` distinguishes absent from failed

### 7.3 A window-contract test spanning stages 2–5

Because the two defects mask each other, a stage-2-only test would still pass
if stages 3–5 disagreed. One test asserts that a PR merged inside the window
but created before it is selected by every stage. Under current code that PR is
invisible everywhere.

### 7.4 Verification against ground truth

Pre-committed so the result cannot be rationalised after the fact.

| check | target |
|---|---|
| rows with `merged_at` in `[2026-08-01, 2026-08-04)` | **461** (35 exist, 426 new) |
| pandas on 2026-08-01 | **15**, not 7 |
| stage 3 selection includes PRs created before the window | greater than 0; currently 0 exist |

If the count is anything other than 461, the window mapping is wrong and work
stops rather than proceeding to stages 3–5.

### 7.5 Scale check, marked `slow`

PostHog's July exercises bisection past the cap: 5,166 unique, zero duplicates.
The prototype passes this and the production implementation must also.

---

## 8. Documentation

CLAUDE.md needs three corrections that this work makes visible.

1. It states stage-2 pre-screening means irrelevant PRs are "never stored". By
   decision 3 they *are* stored, and the pre-screen gates only the diff fetch.
2. The stage 2 and 3 descriptions change: stage 2 no longer fetches diffs.
3. Any description of the date window states `merged_at`, half-open.

---

## 9. Risks

**Single-spec attribution.** Decision 6 puts correctness and performance changes
in one plan. A regression surfacing after both land is harder to attribute.
Mitigation: the section 7.4 verification runs on the correctness changes alone,
before the async work begins, so there is a trustworthy intermediate baseline.

**Deferred backfill.** All 265,181 existing rows were collected under the broken
window, so the historical dataset carries the section 2.1 bias. Decision 2
defers this deliberately. It is not resolved by this work and needs its own
task.

**One token.** Decision 7 means a monthly run crosses a rate-limit reset by
design. If pacing is wrong the failure mode is a long stall rather than a loud
error, so the pacing path needs explicit logging.

**Estimated volumes.** July's ~5,616 REST calls extrapolate the toy window's 67%
pass rate onto 8,407 PRs. The real rate will differ somewhat.

---

## 10. Out of scope

- Stages 6–9
- Backfilling or re-scraping the existing 265,181 rows
- A streaming pipeline (excluded by decision 5)
- A work-ledger table (excluded by decision 8)
- Tightening the title filter (excluded by the evidence in section 2.6)
- Adding tokens or a GitHub App (excluded by decision 7)
