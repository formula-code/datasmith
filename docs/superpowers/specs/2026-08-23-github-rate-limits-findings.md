# GitHub rate limits: does a GitHub App fix them?

**Date:** 2026-08-23
**Type:** spike (investigation only — no behaviour was changed)
**Question:** We hit rate limits everywhere we scrape GitHub with a personal
access token. Do GitHub App installation access tokens have higher limits?

## Answer

**No.** Three independent reasons close the path. Any one of them is sufficient.

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

## Reason 2 — our primary budgets are not the constraint (measured)

Raising a ceiling we do not reach changes nothing. All three numbers below were
measured live against the configured token, not assumed.

**GraphQL — not close.** Stage 2's real `_SEARCH_MERGED_PRS_QUERY`, run against
`pandas-dev/pandas` for a one-month window, bills:

```
cost=2   nodeCount=12100   -> 100 PRs returned
=> 2,500 pages/hour affordable  ~=  250,000 PRs/hour
```

Note `nodeCount` is 12,100 but GitHub bills **2 points**. An earlier estimate of
~121 points/page, extrapolated from node count, was wrong — which is why this
was measured rather than reasoned about. Stage 2's heaviest observed month is
5,654 symbolic PRs, about 2% of the hourly ceiling.

**REST core — nearly idle.** Stage 3 spends one core request per diff, and is
already caught up:

```
symbolic-pass total          : 193,399
symbolic-pass AND patch NULL :     684   <- all the work that remains
symbolic-pass AND has patch  : 192,715
```

Per-month outstanding is single digits except for the in-flight month. A monthly
window is 1,374–5,654 symbolic PRs, i.e. under one hour of the 5,000/hr core
budget. Stage 3 also already self-paces at
`DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S=0.75` (~4,800/hr).

**Live `/rate_limit` on the configured PAT** showed `core 5000/5000 used=0` and
`graphql 4968/5000` — idle.

## Reason 3 — the failures we actually have are *secondary* limits

`runner_failures` holds 147 genuine 403s. (A first pass matched 273, but that
included false positives: `synthesize_images` rows where "403" or "429" was a PR
number.)

| Runner | 403s | Endpoint |
|---|---|---|
| `scrape_commits` (stage 2) | 143 | `POST /graphql` |
| `render_problems` (stage 5) | 4 | `GET /repos/.../issues/N` |

GitHub distinguishes the two limits by status code:

> Primary: "the response status will still be `200`, but you will receive an
> error message, and the value of the `x-ratelimit-remaining` header will be
> `0`."
>
> Secondary: "the response status will be `200` or `403`, and you will receive
> an error message that indicates that you hit a secondary rate limit."

**A 403 on `/graphql` is therefore not the primary limit.** It cannot be —
primary GraphQL exhaustion is a 200. And `client.graphql()` already handles that
200 path correctly (detects `errors[].type == "RATE_LIMITED"`, reports it to the
token pool, retries with backoff), so the 403s that escape are the secondary
path, retried `DATASMITH_GH_RETRIES=3` times and then raised.

Secondary limits are anti-abuse controls on *burst shape*:

- no more than 100 concurrent requests
- no more than 900 points/min REST, **2,000 points/min GraphQL**
- 80 content-generating requests/min, 500/hr

The documentation describes **no auth-tier variation** in any of these, and no
App uplift. An installation token raises the primary quota; it does not buy
relief from the limit we are actually hitting.

## The test we did not need to run

An installation token is scoped to the repositories where the App is installed.
We cannot install an App on `pandas-dev/pandas`, `numpy/numpy`, or the other 152
third-party repos in `repositories`. Whether installation tokens carry an
implicit public-read grant is **untested** — it stopped mattering once reasons
1–3 closed the path. If the App is ever revisited, test this first: it is the
cheapest possible kill.

## What is actually wrong

1. **The 403 body is discarded.** `client._request` calls
   `resp.raise_for_status()`, which keeps only the status line. GitHub puts the
   discriminating sentence ("You have exceeded a secondary rate limit") in the
   *body*, and the timing in the `Retry-After` / `x-ratelimit-*` headers. Today
   no operator can tell which limit fired — this investigation had to infer it
   from the status code plus the docs.

2. **`TokenPool` is inert.** `GH_TOKENS` holds one entry, so rotation never
   happens and the `_RateLimit` bookkeeping tracks a single bucket. Adding more
   PATs from the same account is not an obvious fix either: secondary limits
   apply per user, so N tokens of one user plausibly share one anti-abuse
   budget. The docs are silent on this; it is the conservative reading, not a
   verified fact.

3. **Burst shape, not volume.** Stage 2 runs
   `DATASMITH_SCRAPE_COMMITS_CONCURRENCY=5` repos concurrently, each bisecting
   its window with back-to-back GraphQL searches and **no inter-request
   spacing**. Stage 3 has a pacing dial; stage 2 has none. That asymmetry
   matches the failure distribution exactly — 143 stage-2 403s against 0 from
   stage 3, the stage with far more request volume.

## Recommendation — cheapest first

1. **Capture the evidence.** On every 403/429, fold the response body and the
   `Retry-After` / `x-ratelimit-*` headers into the raised error and the
   `runner_failures` row. Then re-run one stage-2 month and read what GitHub
   actually said. Small, no behaviour change, and every other decision depends
   on it.

2. **Pace stage 2 like stage 3.** Add a `DATASMITH_SCRAPE_COMMITS_MIN_INTERVAL_S`
   dial and jitter the retry backoff. Secondary limits respond to spacing, not
   to a bigger quota.

3. **Only if (1) proves a primary limit is genuinely the wall** — revisit the
   App, and test public-repo read access before building anything.

**Do not build the GitHub App integration.** It is the most expensive option on
the table, and the evidence says it addresses a limit we are not hitting.

## Open question for the operator

If the App is pursued anyway, the design hinges on one fact: is a GitHub
organization available, and are multiple installations acceptable? Each
installation carries its own bucket, so installation count — not the App itself
— is the only real multiplier.
