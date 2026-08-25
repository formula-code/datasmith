# Running stages 2–5

Every step here exists because it went wrong on the July/August 2026 run. The
failures are named so the checks are not cargo cult.

## 0. Preflight — always

```bash
./scripts/ingest_preflight.sh
```

Exits non-zero and refuses to pass on any of these:

| check | the failure it catches |
|---|---|
| `fc-data` resolves into **this** checkout | a conda install of a *different* checkout shadowed it on `PATH`; hours of debugging ran against code that was never edited |
| Supabase reachable | the run dies mid-stage and the resume predicate cannot be read |
| GitHub tokens + budget | one token paces stage 3 at ~4,800 diff fetches/hour and crosses a reset |
| model endpoint alive | **vLLM ports are ephemeral.** A restart moves them, and a stale `DSPY_API_BASE` is a silent 30-minute stall, not an error. The check prints the live ports |
| model context ≥ patch + output + prompt | `ContextWindowExceededError` cannot be retried away, and a failed classify leaves `is_performance_commit` NULL, so the row is re-selected forever |
| repo exclusions loaded | migration `00030` unapplied means paying for six repos that produce no containers |

## 1. The window is `merged_at`, half-open `[start, end)`

`--end-date` is **exclusive**. A PR merged at midnight on the end date belongs to
the next window, so consecutive runs partition the corpus instead of overlapping.
All of stages 2–8 go through `window_filters()`; stage 9 opts out deliberately.

```bash
# all of July
--start-date 2026-07-01 --end-date 2026-08-01
```

## 2. Commands

Run stages in order. Each is resumable — re-running skips completed work via an
indexed predicate, so a killed run costs nothing but the item in flight.

```bash
cd /mnt/sdd1/atharvas/formulacode/datasmith_new
S=2026-07-01; E=2026-08-01          # end date is EXCLUSIVE

# stage 2 — GraphQL only, no REST, no LLM.  Cheap: ~170 of 5,000 GraphQL
# points for a whole month.  Raise concurrency freely.
.venv/bin/fc-data --stage 2 --n-concurrent 16 --start-date $S --end-date $E

# stage 3 — the expensive one.  Bound by the GitHub token, NOT by cores.
#   DIFF_MIN_INTERVAL_S x tokens must stay under 5,000 REST/hour/token:
#     1 token  -> 0.75  (~4,800/h)
#     2 tokens -> 0.40  (~9,000/h)
#   LLM workers: raise until the server reports num_requests_waiting > 0,
#   then stop.  One gemma replica saturates near 32.
DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S=0.40 \
DATASMITH_CLASSIFY_LLM_WORKERS=32 \
  .venv/bin/fc-data --stage 3 --n-concurrent 32 --start-date $S --end-date $E

# stage 4 — the ONLY stage that scales with cores.  git clone + uv pip compile,
# so it is disk-bound: raise for more spindles, not more cores.
DATASMITH_RESOLVE_PACKAGES_WORKERS=16 \
  .venv/bin/fc-data --stage 4 --n-concurrent 16 --start-date $S --end-date $E

# stage 5 — LLM again, same backend as stage 3.  Do not run it concurrently
# with stage 3 unless the model server has headroom for both.
DATASMITH_RENDER_PROBLEMS_WORKERS=16 \
  .venv/bin/fc-data --stage 5 --n-concurrent 16 --start-date $S --end-date $E
```

Run detached so a dropped shell does not kill the job, and log somewhere durable
(**not** a session scratch directory — those are deleted with the job):

```bash
mkdir -p logs/ingestion-$(date +%Y-%m)
setsid .venv/bin/fc-data --stage 3 ... \
  > logs/ingestion-$(date +%Y-%m)/stage3.log 2>&1 < /dev/null & disown
```

Do **not** kill it with `pkill -f "fc-data --stage 3"`: that pattern matches the
shell running the `pkill` too, so it kills itself and leaves the job alive. Kill
by PID.

## 3. Two budgets, two dials

Cores scale stage 4. The GitHub token scales stages 2 and 3 and does not care
how large the machine is. Raising one must never raise load on the other, which
is why they are separate knobs.

**More cores will not speed up stage 3.** On the 2026-08 run the limits were, in
order: the LLM server's concurrency ceiling (~32 per replica), then the REST
budget. Cores never entered it.

## 4. Read the end-of-stage line

Every stage ends with:

```
classify_prs finished: 1487 item(s), 1487 succeeded, 0 failed; error types: none
```

This line exists because a stage 2 run once reported 154/154 repositories and
zero failures while silently storing 35 of 81 PRs. **Silence is not success.**
If `failed > 0`, read the causes:

```sql
select count(*), left(error_message,90) from runner_failures
where created_at > now() - interval '6 hours' group by 2 order by 1 desc;
```

## 5. Verify against ground truth, not against "looks like more"

Stage 2's row count should match GitHub's own count for the window:

```bash
.venv/bin/python - <<'EOF'
import asyncio, datasmith
from datasmith.utils.tokens import TokenPool
from datasmith.github.client import GitHubClient
from datasmith.utils.db import fetch_all
Q = 'query($q:String!){search(query:$q,type:ISSUE,first:1){issueCount}}'
async def main():
    gh = GitHubClient(TokenPool())
    total = 0
    for r in fetch_all("repositories", select="owner, repo", filters={"ingest_enabled": True}):
        res = await gh.graphql(Q, {"q": f"repo:{r['owner']}/{r['repo']} is:pr is:merged merged:2026-07-01..2026-07-31"})
        total += res["data"]["search"]["issueCount"]
    print("GitHub says:", total)
    await gh.close()
asyncio.run(main())
EOF
```

Then compare with the stored count for the same window. They should agree.

## 6. Known-good settings

`tokens.env` after the 2026-08 run:

```
SUPABASE_URL=http://127.0.0.1:54321     # local; remote via db.formulacode.org also supported
DSPY_MODEL=openai/gemma-4-12b-it
DSPY_API_BASE=http://127.0.0.1:30020/v1 # RE-CHECK: the port moves on every restart
DSPY_API_KEY=EMPTY
DSPY_MAX_TOKENS=6144                    # OUTPUT cap, reserved from context on every request
DSPY_TEMPERATURE=0
DATASMITH_CLASSIFY_PATCH_TOKENS=12000   # INPUT cap; deliberately separate from the output cap
```

`DSPY_MAX_TOKENS` is an **output** ceiling that every request subtracts from the
model's context. Setting it to 16000 against a 32k model left too little room for
the patch; setting it to 2048 truncated the model's reasoning before it emitted
the label, which stalled the stage. 6144 is the measured middle.

## 7. What is still worth fixing

**62% of stage-6 failures (6,066 of 9,760) are `Agent exited without running
local_ci.py`** — the same rate at repositories that succeed as at those that
never do. That is the largest single loss in the pipeline and is not addressed by
anything above.
