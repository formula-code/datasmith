# Running stages 2–5

This runbook gives the checks and settings for a stage 2 to 5 run, and the failure that each check prevents.

## 0. Preflight

```bash
./scripts/ingest_preflight.sh
```

It exits non-zero when any of these checks fails:

| check | the failure it prevents |
|---|---|
| `fc-data` resolves into **this** checkout | an install of a different checkout earlier on `PATH` runs code you did not edit |
| Supabase reachable | the run stops mid-stage and the resume check cannot read the database |
| GitHub tokens and budget | one token limits stage 3 to about 4,800 diff fetches per hour, and a long run crosses a reset |
| model endpoint alive | **vLLM ports change when a server restarts.** A stale `DSPY_API_BASE` stalls the stage with no error. The check prints the live ports |
| model context ≥ patch + output + prompt | `ContextWindowExceededError` does not go away on retry, and a failed classification leaves `is_performance_commit` NULL, so the row is selected again on every run |
| repo exclusions loaded | without migration `00030`, the run spends budget on repositories that produce no containers |

## 1. The window is `merged_at`, half-open `[start, end)`

`--end-date` is **exclusive**. A PR merged at midnight on the end date belongs to
the next window, so consecutive runs do not overlap. Stages 2 to 8 use
`window_filters()`; stage 9 uses no window.

```bash
# all of July
--start-date 2026-07-01 --end-date 2026-08-01
```

## 2. Commands

Run the stages in order. Each stage can resume: a rerun skips completed work, so
a killed run loses only the items in progress.

```bash
cd <datasmith checkout>
S=2026-07-01; E=2026-08-01          # end date is EXCLUSIVE

# stage 2: GraphQL only, no REST and no LLM. A month uses a small part of the
# 5,000 GraphQL points per hour, so concurrency can be high.
.venv/bin/fc-data --stage 2 --n-concurrent 16 --start-date $S --end-date $E

# stage 3: the expensive stage. The GitHub REST budget limits it, not the cores.
#   DIFF_MIN_INTERVAL_S and the token count must keep each token under 5,000 REST calls per hour:
#     1 token  -> 0.75  (~4,800/h)
#     2 tokens -> 0.40  (~9,000/h)
#   LLM workers: raise them until the server reports num_requests_waiting > 0.
DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S=0.40 \
DATASMITH_CLASSIFY_LLM_WORKERS=32 \
  .venv/bin/fc-data --stage 3 --n-concurrent 32 --start-date $S --end-date $E

# stage 4: the only stage that scales with cores. git clone and uv pip compile
# also load the disk.
DATASMITH_RESOLVE_PACKAGES_WORKERS=16 \
  .venv/bin/fc-data --stage 4 --n-concurrent 16 --start-date $S --end-date $E

# stage 5: uses the same LLM backend as stage 3. Run it at the same time as
# stage 3 only if the model server has capacity for both.
DATASMITH_RENDER_PROBLEMS_WORKERS=16 \
  .venv/bin/fc-data --stage 5 --n-concurrent 16 --start-date $S --end-date $E
```

Run the job detached, so that a closed shell does not stop it, and write the log
to a durable folder (not a session scratch directory, which is deleted with the
session):

```bash
mkdir -p logs/ingestion-$(date +%Y-%m)
setsid .venv/bin/fc-data --stage 3 ... \
  > logs/ingestion-$(date +%Y-%m)/stage3.log 2>&1 < /dev/null & disown
```

Do **not** stop it with `pkill -f "fc-data --stage 3"`: the pattern also matches
the shell that runs `pkill`, which then kills itself and leaves the job running.
Stop it by PID.

## 3. Two budgets, two settings

Cores limit stage 4. The GitHub token budget limits stages 2 and 3, whatever the
size of the machine. They are separate settings, so a change to one does not add
load on the other.

**More cores do not make stage 3 faster.** Its limits are the LLM server's
concurrency, then the REST budget.

## 4. Read the end-of-stage line

Every stage ends with a line like this:

```
classify_prs finished: 1487 item(s), 1487 succeeded, 0 failed; error types: none
```

Check that the stored rows match it: a stage can report no failures and still
store fewer rows than expected. If `failed > 0`, read the causes:

```sql
select count(*), left(error_message,90) from runner_failures
where created_at > now() - interval '6 hours' group by 2 order by 1 desc;
```

## 5. Compare with GitHub's count

The stage 2 row count must match GitHub's own count for the window:

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

Then compare it with the stored count for the same window.

## 6. Example settings

```
SUPABASE_URL=http://127.0.0.1:54321     # local; remote access through db.formulacode.org also works
DSPY_MODEL=openai/<served model name>
DSPY_API_BASE=http://127.0.0.1:<port>/v1 # check the port again after a server restart
DSPY_API_KEY=EMPTY
DSPY_MAX_TOKENS=6144                    # OUTPUT limit, reserved from the context on every request
DSPY_TEMPERATURE=0
DATASMITH_CLASSIFY_PATCH_TOKENS=12000   # INPUT limit for the patch; separate from the output limit
```

`DSPY_MAX_TOKENS` is an **output** limit that every request reserves from the
model's context. With a 32k context, 16000 leaves too little room for the patch,
and 2048 cuts off the model's reasoning before it writes the label. A value near
6144 avoids both problems.
