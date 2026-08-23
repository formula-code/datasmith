# GitHub rate limits: does a GitHub App fix them?

**Date:** 2026-08-23
**Type:** spike (investigation only — no behaviour was changed)
**Question:** We hit rate limits everywhere we scrape GitHub with a personal
access token. Do GitHub App installation access tokens have higher limits?

## Answer in one paragraph

**A GitHub App will not raise the limits that matter, and the data shows we are
not hitting a rate limit anyway.** Installation tokens start at the same
5,000/hr as a PAT. Both GraphQL queries stage 2 uses bill 1–2 points per
100-PR page, so the GraphQL budget is ~2% consumed at our volume. Stage 3 has
684 diffs left of 193,399. The only GitHub rate-limit failures on record are a
three-day episode in **March 2026**, raised by a code path that is now
deprecated. What actually broke stage 2 recently was something else entirely:
on **2026-08-18** the PAT returned **401 Unauthorized** — it had expired. That
*is* a problem a GitHub App solves, but for credential-lifecycle reasons, not
throughput ones.

## Reason 1 — the linked article documents a mechanism, not a higher limit

`docs.github.com/.../generating-an-installation-access-token-for-a-github-app`
explains how to *mint* a token. It says nothing about a larger quota, because
there isn't one at the entry point:

> "GitHub Apps authenticating with an installation access token use the
> installation's minimum rate limit of 5,000 requests per hour."

That is exactly the PAT limit. The uplift is conditional:

| Condition | Limit |
|---|---|
| Installation, base | 5,000/hr — same as our PAT |
| Installation with >20 repositories | +50/hr per repository |
| Installation on an org with >20 users | +50/hr per user |
| Hard ceiling (non-Enterprise) | **12,500/hr** |
| Installation on a GitHub Enterprise Cloud org | 15,000/hr |

Reaching the 12,500 cap needs roughly 170 repositories inside one installation.
Best realistic case is **2.5x**, and only after standing up an org and
installing across it.

## Reason 2 — our primary budgets are nowhere near the constraint (measured)

Raising a ceiling we do not reach changes nothing. Every number below was
measured live against the configured token, not assumed.

**Both GraphQL queries are cheap.** GitHub bills *points*, and points are not
node counts — the difference is large enough that an early estimate of ~121
points/page, extrapolated from `nodeCount`, was wrong by two orders of
magnitude. Measured with `rateLimit { cost limit remaining nodeCount }` spliced
into the real query text:

| Query | Billed cost | nodeCount | Pages affordable/hr |
|---|---|---|---|
| `_SEARCH_MERGED_PRS_QUERY` (current stage 2) | **2** | 12,100 | 2,500 ≈ 250,000 PRs/hr |
| `_MERGED_PRS_QUERY` (deprecated `paginate_merged_prs`) | **1** | 2,100 | 5,000 |

Stage 2's heaviest observed month is 5,654 symbolic PRs — about 2% of the
hourly ceiling.

**REST core is nearly idle.** Stage 3 spends one core request per diff, and is
already caught up:

```
symbolic-pass total          : 193,399
symbolic-pass AND patch NULL :     684   <- all the work that remains
symbolic-pass AND has patch  : 192,715
```

Per-month outstanding is single digits outside the in-flight month. A monthly
window is 1,374–5,654 symbolic PRs, well under one hour of the 5,000/hr core
budget. Stage 3 also already self-paces at
`DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S=0.75` (~4,800/hr).

**Live `/rate_limit`** on the configured PAT: `core 5000/5000 used=0`,
`graphql 4968/5000`.

## Reason 3 — the 403s on record are historical, from deprecated code

`runner_failures` holds 147 genuine 403s. (A first pass matched 273, but that
included false positives — `synthesize_images` rows where "403" or "429" was a
PR number.) Dating them is what changes the conclusion:

| Fact | Value |
|---|---|
| Months containing a 403 | **2026-03 only** |
| Window | 2026-03-28 18:25 → 2026-03-30 10:13 |
| Raised from `paginate_merged_prs` (**deprecated**) | 143 |
| Raised elsewhere | 4 |
| Python in every traceback | 3.10 (current venv is 3.12) |
| Most recent failure of any kind in the table | 2026-08-23 |

All 147 come from one three-day burst five months ago, on an older interpreter,
and 143 of them from `paginate_merged_prs` — which the source itself marks
deprecated ("orders by `CREATED_AT` and so cannot express a `merged_at`
window"). Stage 2 now goes through `fetch_merged_prs`. **This is a fixed past,
not a live symptom.**

For completeness on which limit those were: GitHub distinguishes the two by
status code.

> Primary: "the response status will still be `200`, but you will receive an
> error message, and the value of the `x-ratelimit-remaining` header will be
> `0`."
>
> Secondary: "the response status will be `200` or `403`, and you will receive
> an error message that indicates that you hit a secondary rate limit."

A 403 on `/graphql` cannot be the primary limit. And `client.graphql()` already
handles the primary 200 path correctly — it detects
`errors[].type == "RATE_LIMITED"`, reports to the token pool, and retries with
backoff. So the March 403s were *secondary* limits: anti-abuse controls on burst
shape (100 concurrent; 900 pts/min REST; 2,000 pts/min GraphQL). The docs
describe **no auth-tier variation** in any of them and no App uplift — an
installation token raises the primary quota only.

## What actually broke stage 2 in August: an expired token

The recent GitHub failures are not 403s at all:

| Date | Count | Error | Runner |
|---|---|---|---|
| 2026-08-18 19:55:40–42 | **23** | `401 Unauthorized` on `POST /graphql` | `scrape_commits` |

All 23 land inside a two-second window — the signature of every in-flight
concurrent request failing at once against a credential the server rejects, not
of a quota being consumed. `tokens.env` was rotated **five days later**, at
2026-08-23 06:43 (`tokens.env.bak.20260823-064343`). The token in place today
works.

So the timeline is:

```
2026-03-28..30   403 secondary limits, deprecated paginate_merged_prs, py3.10
2026-08-18       401 Unauthorized  <- fine-grained PAT expired; stage 2 down
2026-08-23 06:43 tokens.env rotated by hand
2026-08-23       buckets idle, queries billing 1-2 points
```

A fine-grained PAT (`github_pat_...`) carries an expiry. When it lapses, every
GitHub stage fails instantly and indistinguishably from a hard outage — and a
burst of instant failures is easy to read as "we're being rate limited."

**This is the one place a GitHub App genuinely helps.** An installation token is
minted programmatically from the App's private key and expires after an hour by
design, so the client refreshes it automatically. There is no annual expiry
cliff and no manual rotation step. That is a reliability argument, and it is a
good one — it just isn't the throughput argument the article was read as making.

## The test that gates any App work

An installation token is scoped to the repositories where the App is installed.
We cannot install an App on `pandas-dev/pandas`, `numpy/numpy`, or the other 152
third-party repos in `repositories`. Whether installation tokens carry an
implicit public-read grant for repos outside the installation is **untested**,
and it is the cheapest possible kill: mint one token, then
`GET /repos/pandas-dev/pandas/pulls?state=closed` and one GraphQL query against
a non-installed repo. A 404 ends the App path outright, whatever its
credential-lifecycle merits. Run this before building anything.

## Gaps this investigation hit

1. **The 403/429 body is discarded.** `client._request` calls
   `resp.raise_for_status()`, which keeps only the status line. GitHub puts the
   discriminating sentence ("You have exceeded a secondary rate limit") in the
   *body* and the timing in the `Retry-After` / `x-ratelimit-*` headers. This
   spike had to infer primary-vs-secondary from a status code plus the docs.

2. **Rate limits absorbed by waiting leave no trace.** `_rate_limit_wait`
   honours `Retry-After` up to `DATASMITH_GH_MAX_RETRY_WAIT_S=3600`, so a run
   that is being throttled *stalls silently* rather than failing. Stage 3 has a
   stall logger (`DATASMITH_CLASSIFY_DIFF_STALL_LOG_S`); stage 2 has none, and
   nothing is written to `runner_failures`. If the live complaint is slowness
   rather than errors, the current instrumentation could not show it — so the
   absence of recent 403s is weaker evidence than it looks.

3. **`TokenPool` is inert.** `GH_TOKENS` holds one entry, so rotation never
   happens. Note that adding more PATs from the same account is not an obvious
   fix: secondary limits apply per user, so N tokens of one user plausibly share
   one anti-abuse budget. The docs are silent; this is the conservative reading,
   not a verified fact.

## Recommendation — cheapest first

1. **Make the next occurrence diagnosable.** Fold the response body and the
   `Retry-After` / `x-ratelimit-*` headers into the raised error and the
   `runner_failures` row on every 403/429, and give stage 2 the stall logger
   stage 3 already has. Small, no behaviour change, and it removes the exact
   ambiguity that made this investigation indirect.

2. **Treat token expiry as the known live failure.** Either move to App
   installation tokens *for their auto-refresh*, or — far cheaper — add a
   preflight assertion that fails loudly on 401 and warns when the PAT is within
   N days of expiry (`GET /rate_limit` response headers expose the token's
   validity; `preflight.py` already calls that endpoint).

3. **Do not build the App integration for rate-limit reasons.** It is the most
   expensive option on the table, and the measurements say it addresses a limit
   we are not reaching. If it is built, build it for credential lifecycle, and
   run the public-repo read test first.

## Open question for the operator

The data does not show present-tense rate limiting: buckets are idle, both
queries bill 1–2 points, and the last 403 was in March. Gap 2 above means a
silent stall would not appear in `runner_failures`, so this is not conclusive.

**Which stage and which run is actually hurting, and does it show as errors or
as slowness?** That answer is worth more than any further measurement here.
