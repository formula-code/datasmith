# Scaling to 100 honest containers

**Endpoint (set by the user on 2026-08-24):** the work is not done until at
least **100 honest containers** exist. This supersedes "flip
`DATASMITH_PV_ENABLED` and scale toward 100-200" as the stopping condition —
the flag is now a step, not the finish line.

## 1. What counts as honest

Under the gate as it now stands, a container is honest when all of the
following hold. Anything short of this is not counted, however many rows exist.

| condition | where it is recorded |
|---|---|
| the image builds | stage 6 `TRY_DEFAULT` / `PRODUCE_VERIFY` succeeds |
| the host image scan finds nothing | `image_integrity.evaluate` returns no findings |
| the verifier accepts | `GradedReport.accepted`, with no hard failures |
| a manifest is sealed | `candidate_containers.build_manifest IS NOT NULL` |

The host-scan condition is new and is the one the previous corpus fails.

## 2. Baseline, honestly counted

**Revised 2026-08-24 23:40 — the baseline is ZERO, not four.**

```
candidate_containers rows                1858
  ... carrying a build_manifest              7
  ... across distinct repositories         134
  ... verification_state = 'verified'        0
honest containers today                      0
```

Migration `00029_candidate_containers_verification_state.sql` marks every row
`unverified`. The earlier figure of four counted containers that pass the
in-container honesty gate, which is not the same claim.

Three facts forced the revision:

1. **The seven manifest-carrying containers predate today.** Provenance:
   trackintel 2026-04-15, networkx 2026-04-16, xbatcher 2026-05-06, optuna and
   pysindy 2026-08-18, bottleneck and tiled 2026-08-23. (`updated_at` is NOT
   maintained on upsert — networkx's row still reads April while its manifest
   was rewritten today — so the corpus cannot be dated from that column.)
2. **One of the seven is the adversarial negative control.** `pysindy#139`
   carries a sealed manifest claiming 2 benchmarks measured in 5.38s, and the
   host scan rejects it on five findings. A sealed manifest is not evidence of
   honesty; that container is proof of the opposite.
3. **The gate never ran on any of them, including the one rebuilt today.**
   See section 3b: a TRY_DEFAULT success returns before the gate is reached.

**1858 is not progress toward 100, and neither is 4.** The handoff records that 130 of those 134
repositories carry a `builtins.sys` shim injected into site-packages as a
workaround for a defect since fixed, and the host scan confirms the taint
directly: it rejected `apache/arrow` from that corpus for a `sitecustomize.py`
in two places *and* a `grep` wrapper that suppresses the secret scan. That
corpus is to be regenerated, so the real baseline is 4 and the gap is 96.

Raw material is not the constraint: 16,855 performance PRs across 148
repositories are available, with packages resolved.

## 3. The blocker between here and 100

Reaching 100 is **not** simply a matter of running stage 6 for longer. The
defect recorded in `2026-08-24-check-id-vocabulary-proposal.md` is a yield
problem, and at scale it is the dominant one.

`severity.py`'s only unconditionally-soft check id, `pytest_pass_ratio`, is a
build-manifest field name and not a battery fact name, so the verifier agent
never emits it. Every failing check is therefore graded HARD. Observed
consequence: **trackintel#596 — a container that passes the honesty gate and
whose label is `accept` — was rejected at 376 of 377 tests passing.**

Most real repositories have at least one test that fails for reasons that have
nothing to do with whether we built the container: a network fixture, a
platform-sensitive float, a flaky timeout. With the soft tier unreachable,
each of those is a hard rejection that costs three rebuild rounds and then
yields nothing. The accept rate that produced 4 honest containers out of 134
repositories is the rate this defect implies.

**So the vocabulary fix moves from "argue separately" into scope.** The earlier
instruction was not to adjust `severity.py` to make the confusion matrix look
better, and that still holds: `severity.py` stays untouched, the fix is in the
verifier's prompt, it was diagnosed by reading a report rather than the matrix,
and it cannot reach the negative controls because those are rejected
deterministically before the agent is asked. It is validated on its own terms,
per section 5 below, and if it fails that validation it is reverted rather than
kept.

## 3b. The gate does not cover the path most containers take

Found 2026-08-24 while establishing which containers came from the current
pipeline. `agents/synthesizer.py` says it plainly in its own comment:

> the states above are untouched: a repo the stock template already builds
> never reaches here, and that path costs nothing

`TRY_DEFAULT` builds with the stock template, and on success it calls
`_save_context` and RETURNS. `PRODUCE_VERIFY` — and therefore the host image
scan and the verifier — is only reached when TRY_DEFAULT has already failed.

So `DATASMITH_PV_ENABLED=1` gates the repair path, not the happy path. A
repository the stock template builds first time is admitted to
`candidate_containers` having been checked by `classify_context`, which audits
the BUILD SCRIPTS, and by the legacy `run_tests` exit code — but nothing looks
at the image.

This was measured, not inferred: `networkx#8148` was rebuilt through the
current pipeline on 2026-08-24, took the TRY_DEFAULT path, sealed a manifest
recording 140 benchmarks in 1673.88s, and was never image-scanned.

**Consequence for this goal.** Most of the 96 remaining containers will come
from repositories the stock template can build — that is the cheap, common
case. If the scan only runs on the repair path, most of the corpus arrives
unscanned, and `verification_state` can never legitimately move to `verified`
for them. **The host scan has to gate every admitted container, whichever path
produced it.** That is the next change, and it is a precondition for counting
anything toward 100.

The scan is deterministic, needs no agent, and costs ~90s against a build that
costs 300-700s, so running it on the TRY_DEFAULT path is cheap. The verifier
agent is a separate question: the stock template succeeding is already decent
evidence about function, and the expensive half is the agent, not the scan.

## 4. Order of work

Each step gates the next. Nothing is skipped because the endpoint moved.

1. **Finish the labelled-set run.** In progress. Produces conditions 1 and 2,
   and the reports that explain condition 3.
2. **Explain every disagreement.** trackintel and fluids are done
   (`2026-08-24-host-image-scan.md` section 4b); tiled remains.
3. **Task 12** — the end-to-end round on `OGGM/oggm#1830`. Condition 4.
   Predicted to be at risk from the same vocabulary defect: round 2 accepts
   only if oggm's suite passes completely. Recorded either way, not softened.
4. **Land the vocabulary fix and re-validate it.** Required for yield.
5. **Flip `DATASMITH_PV_ENABLED=1`** once all four conditions hold.
6. **Regenerate at scale**, with `DATASMITH_SKIP_SIMILAR_CONTEXTS=1`.

### Why step 6 needs that flag

`TRY_SIMILAR` reuses a context that succeeded for another commit of the same
repository, and 128 of those stored contexts install a sitecustomize shim. The
host scan will reject every container built from one. Leaving `TRY_SIMILAR` on
would spend a full reflexive round per repository rediscovering that, so it is
turned off for the regeneration rather than left to be learned 128 times.

## 5. Validating the vocabulary fix before scaling on it

Re-run the labelled set after the change and require:

- both negative controls still rejected — structural, since they
  short-circuit;
- **zero new accepts among the six hard-grounds containers.** If any of
  xarray, joblib, dimod, aicsimageio, geocat-comp or datashader flips to
  accept, the change did more than restore the intended mapping: revert it;
- trackintel#596 accepted, with an argued `waiver_reason` a human agrees with;
- fluids#38 still rejected — its `asv_discover` failure is real.

## 6. Cost, stated plainly

A build costs 300-700s. The reflexive loop allows up to
`DATASMITH_PV_MAX_ROUNDS` (3) of them, and each round adds a host scan (~90s),
a battery (~1-5 min) and an agent call. A single task therefore costs roughly
10-40 minutes, and a rejected one costs the most.

96 more honest containers, at an accept rate that will not be known until the
fix is validated, is a **multi-day run**, not a session. It has to be operated
as a resumable background job with progress read from the database, not held
in one process. Progress is counted with:

```sql
select count(*) from candidate_containers where build_manifest is not null;
```

which is why the manifest condition is part of the definition in section 1:
it is the only one of the four that survives in durable storage.


---

## Appendix: condition 4 needs a different task, and which one

Added 2026-08-24 23:2x, after three runs of Task 12 on `OGGM/oggm#1830`.

`oggm#1830` cannot satisfy condition 4. Its container has **no benchmark
directory at all** — `asv.conf.json` declares `"benchmark_dir": "benchmarks"`,
`/workspace/repo/benchmarks/` does not exist, and there is no
`formulacode_task_overrides` row to supply one. A container that measures zero
benchmarks can never be accepted, and the producer owns only
`docker_build_pkg.sh` and `docker_build_run.sh`, so no edit it can make will
create a benchmark suite. Full evidence in `pv-validation.md`.

Condition 4 needs a task meeting BOTH requirements:

- **(a)** `TRY_DEFAULT` fails for a cause the producer can fix in its two files;
- **(b)** the repository has a benchmark suite that actually measures.

`error_logs` yields six tasks satisfying (a) with a `ModuleNotFoundError`.
Three were screened for (b) by inspecting each repository's existing image:

| candidate | (a) TRY_DEFAULT failure | (b) benchmark suite |
|---|---|---|
| **mars-project/mars#3329** | `No module named 'pkg_resources'` | **yes** — `benchmarks/` with `__init__.py`, 17 `.py` files |
| pytroll/satpy#3219 | `No module named 'imp'` (removed in Python 3.12) | yes — `benchmarks/` with `__init__.py`, 7 `.py` files |
| napari/napari#8789 | `No module named 'src'` | **no** — `benchmarks/` missing, same as oggm |

`dimod#1371` and `geocat-comp#748` also satisfy (a) but are labelled hard
rejects in the validation set for collection errors, so they cannot reach an
accept either.

**Selected: `mars-project/mars#3329`.** A missing `pkg_resources` is a missing
`setuptools`, which is exactly the "one added dependency, closes in two rounds"
shape section 9 wanted from oggm and did not get. satpy is the fallback, but
its failure is a Python-version incompatibility (`imp` was removed in 3.12)
rather than a missing dependency, and the producer cannot change the
interpreter from the two files it owns.

**Caveat, stated because it was not verified directly:** requirement (b) was
screened against each repository's `:latest` image from the older corpus, which
was built at a different commit than the PR under test. It is strong evidence
that the repository HAS a suite, not proof that #3329's commit does. The run
itself will settle it.

Re-selecting the candidate is a change to the spec's stated plan. It is argued
here rather than made silently, and oggm's failure is recorded in full rather
than written off.


---

## Appendix B: raising the round budget for mars, argued before the run

Written 2026-08-25 03:35, BEFORE running it, so the reasoning cannot be fitted
to the outcome.

`mars-project/mars#3329` was run three times. The first two exposed defects in
the harness (a false accept on `build_failed`, and a signature that could not
tell two failures apart). The third ran cleanly and is the one that counts:

```
round 1  key=("Build failed at stage 'pkg': Docker build failed (rc=1)")
round 2  key=("7.213 ModuleNotFoundError: No module named 'mars'")
round 3  key=("Build failed at stage 'pkg': Docker build failed (rc=1)")
stop_reason: budget          <- ran out of rounds, NOT out of progress
```

Three distinct failures, correct termination. The loop stopped because
`DATASMITH_PV_MAX_ROUNDS` is 3, not because the producer had stalled — the
no-progress rule never fired.

### Why raising it is legitimate rather than convenient

Spec section 9 states condition 4 as "one repository must complete reject ->
producer edit -> rebuild -> accept". **It fixes no round count.**
`DATASMITH_PV_MAX_ROUNDS` is a documented tunable whose default of 3 was chosen
for cost, not correctness, and the loop's own third stopping rule exists
precisely so that extra budget cannot be wasted: a producer that stops making
progress is cut off at two identical signatures regardless of the cap.

So raising the budget cannot turn a stalled repair into a false success. It can
only let a repair that is still moving continue.

### Why the remaining failure looks reachable

The build fails because the isolated build environment resolves
`setuptools 63.4.3`, which predates PEP 660 and therefore has no
`build_editable` hook, while the task's own `env_payload` pins
`setuptools==82.0.1`. The producer owns `docker_build_pkg.sh`, and
`--no-build-isolation` (or pinning the build requirement) is exactly the kind
of edit that file exists for. Two of the three rounds were spent discovering
the shape of the problem.

### The bar

Run with `DATASMITH_PV_MAX_ROUNDS=6`. Condition 4 is satisfied only by a real
`accepted` outcome with a sealed manifest and a measurement. If it stops on
`no_progress`, the producer has stalled and the extra budget has answered the
question honestly in the negative — record it and stop, rather than raising the
cap again.

**A budget raised twice is a criterion being negotiated.** Once is a
correction; twice is fitting.


### Appendix B, result: mars at 6 rounds — condition 4 still FAILS

```
round 1/6  hard=['build_failed']                 key=("Build failed at stage 'pkg': Docker build failed (rc=1)")
round 2/6  hard=['import_sweep','build_failed']  key=("Build failed at stage 'pkg': Docker build failed (rc=1)")
no progress in round 2; stopping        stop_reason: no_progress
```

The pre-registered bar said: if it stops on `no_progress`, the producer has
stalled, record it in the negative and do not raise the cap again. It stopped
on `no_progress`, so **condition 4 remains unsatisfied and the flag stays 0.**

But the reading is not that clean, and the honest record has to say both
things. **The two rounds were NOT the same failure** — their hard-failure sets
differ (`['build_failed']` vs `['import_sweep', 'build_failed']`), which is
objective evidence the producer changed something and the build failed
differently. The no-progress rule fired on a FALSE equality.

### The signature is not robust for buildkit logs

`_signature` takes the last line that names a cause, else the last non-noise
line. A buildkit build log ends on generic progress lines —

```
#14 11.47   Installing build dependencies: finished with status 'done'
#14 12.05   Checking if build backend supports build_editable: finished with status 'done'
```

— while the distinguishing error sits well above them. Earlier tonight's fix
(feeding the real log through `_default_failure_message` instead of
`json.dumps`) demonstrably helped: the 3-round run produced a distinct round-2
key, `"7.213 ModuleNotFoundError: No module named 'mars'"`. It is not reliable.

**This is recorded, not fixed.** Two reasons:

1. The obvious repair — folding `graded.hard_failures` into the mode-A progress
   key — was considered and rejected by the original design for a stated
   reason: those ids come from the agent and are not stable, so the same
   failure described two ways would read as progress and rule 3 would never
   fire. Trading a rule that fires too eagerly for one that never fires is not
   obviously an improvement, and deciding it at 4am against a criterion I want
   to pass is exactly the wrong moment.
2. Continuing to adjust the harness until mars accepts IS fitting the gate to
   its own validation, whichever file the edit lands in. The line has held all
   session and it holds here.

### Where condition 4 actually stands

Three candidates, three distinct outcomes, none of them an accept:

| task | outcome |
|---|---|
| oggm#1830 | **cannot** ever accept — no benchmark directory in the container |
| mars#3329 | producer did not converge; loop stopped on a false no-progress |
| tiled#1283 | never reaches the loop — TRY_DEFAULT builds it first time |

What HAS been demonstrated end-to-end is every mechanism condition 4 exists to
test, minus the repair itself: tiled#1283 went build -> host scan -> verifier
accept -> sealed manifest and is the first row marked `verified`. What remains
undemonstrated is specifically **reject -> producer edit -> rebuild -> accept**.

The next attempt should start from a task whose TRY_DEFAULT failure is a plain
missing dependency in a repository already proven measurable. Of the 27
recorded TRY_DEFAULT failures only tiled and networkx are in proven-measurable
repositories, and both now build first time — so the honest next move is to
widen the measurable set (run more repositories through stage 6 and see which
measure) rather than to keep re-running the same three tasks.


---

## Appendix C: the 15-task sweep — the round budget is the binding constraint

Sample drawn 2026-08-25 with `random.seed(20260825)`, one PR per repository
across 15 repositories, from a pool of 13,836 performance PRs in 130
repositories with resolved packages. Everything already touched this session
and the whole digest-pinned validation set were excluded. The seed is recorded
so the sample is reproducible and demonstrably not cherry-picked.

**Ran at `model_reasoning_effort=medium`.** `codex.py` passes the effort to
`codex exec` explicitly, which overrides `~/.codex/config.toml`, so raising it
there had no effect on datasmith. `DATASMITH_CODEX_REASONING_EFFORT=high` is
now set in `tokens.env` for future runs; this sweep predates it and its numbers
should be read as a medium-effort baseline.

### The producer reasons well

The per-round plan logging added the same morning is what made this visible;
before it, `outcome.plans` was collected and discarded. Actual edits:

```
mujoco_warp#633   "Pin the MuJoCo Python binding to version 3.3.0, which
                   exposes mjSENS_E_POTENTIAL"
TileDB-Py#869     "Pin setuptools below 81, prevent backend setup from
                   upgrading it, and constrain fallback isolated builds"
   -> round 2     "Pin isolated builds to setuptools==70.3.0 and pass both
                   PIP_CONSTRAINT and PIP_BUILD_CONSTRAINT"
dpctl#1651        "Detect legacy skbuild references and install scikit-build
                   in each ASV environment"
   -> round 2     "Install dpctl's scikit-build/CMake native build
                   dependencies and run setup.py build_ext --inplace"
numpy-financial#47 "Detect JSON/JSONC ASV configs and generate a minimal
                   asv.conf.json when none exists"
   -> round 2     "Changed the generated ASV config key from asv_version to
                   version"
```

The mujoco edit is the behaviour the exercise was meant to test: read a missing
symbol, find the version that provides it, pin it. These are researched fixes.
The naive `setuptools==63.4.3` pin on mars#3329 that prompted the question is
the outlier, not the pattern.

### ...and then runs out of rounds

| task | stop_reason |
|---|---|
| TileDB-Inc/TileDB-Py#869 | **budget** |
| IntelPython/dpctl#1651 | **budget** |
| numpy/numpy-financial#47 | **budget** |
| geopandas/geopandas#472 | no_progress |
| google-deepmind/mujoco_warp#633 | no_progress |
| microsoft/Qcodes#5320 | no_progress |

Three of six were **still making progress when the cap cut them off**. These
are multi-step repairs -- pin a version, discover the next constraint, pin that
-- and `DATASMITH_PV_MAX_ROUNDS=3` does not fit their shape.

### Why raising it is evidence, not fitting

The pre-registered worry (appendix B) was that a raised cap could mask a
stalled producer. `budget` and `no_progress` are distinct stop reasons and the
loop's third rule still cuts off a stalled producer at two identical
signatures regardless of the cap, so extra rounds cannot manufacture an
acceptance -- only let a live repair continue.

Appendix B declined to raise the cap a second time on a single data point
(mars). This is six tasks with a 50% budget-exhaustion rate, which is a
different quality of evidence for a different claim: not "let mars through" but
"the default does not fit the workload".

**Recommendation:** raise `DATASMITH_PV_MAX_ROUNDS` for the scale run, and
re-run this same seeded sample at `high` effort to measure the two changes
together. The sample is reproducible, so the comparison is like-for-like.

### Correction recorded

An earlier reading of `error_logs` reported "11 of 38 failures are build
backend absent" and recommended fixing that class. **That counted ROWS, not
tasks.** Deduplicated: 44 rows are 33 distinct tasks, the bucket holds 7 tasks,
and 6 of those 7 predate commit `cc85734` ("install the PEP 517 backend before
a no-isolation build"), which added `ensure_build_backend`. The only live
member was mars#3329, counted four times because it was re-run four times. The
class is already fixed and needs no further work.


### Appendix C, final baseline: 0 of 11

The seeded sweep completed 2026-08-25 07:22 at `effort=medium`,
`max_rounds=3`.

```
15 tasks selected
 -4 dropped before any build: 2 with no resolved packages, 2 with no pr_context
 11 attempted -> 0 succeeded, 11 failed
```

| stop_reason | n | tasks |
|---|---|---|
| **budget** (still repairing when cut off) | 5 | TileDB-Py#869, dpctl#1651, numpy-financial#47, satpy#2998, sourmash#1946 |
| no_progress (producer stalled) | 6 | outlines-core#241, geopandas#472, mujoco_warp#633, Qcodes#5320, climpred#515, hangar-py#84 |

Across 29 rejected rounds, how far the pipeline actually got:

```
10  build failed
 9  missing dependency
 8  reached MEASUREMENT (asv_exec_failed) -- the container BUILT
 1  wheel build failed
 1  no asv.conf found
```

**Eight rounds produced a container that built and then failed measurement.**
The pipeline gets further than a pass/fail count suggests.

### A structural note on the measurement failures

A container that builds but trips a FATAL manifest invariant such as
`asv_exec_failed` makes `verify_context` return `success=False`, so the loop
sees `mode=build_failed`, the verifier never receives an image, and the battery
never runs. The producer is then asked to fix an ASV INVOCATION problem from a
build log alone, without the evidence that would most help it.

Rejecting is correct -- `asv_exec_failed` gates publishing. But "failed to
build" and "built, then could not measure" are different situations with
different remedies, and the loop currently cannot tell the producer which one
it is facing. The producer still made credible attempts (one proposed using
`ROOT_PATH` for the repository location, passing the discovered ASV config by
absolute path, and selecting an ASV executable present in either the target
environment or the base PATH), which suggests the gap is evidence rather than
reasoning.

**Not changed yet.** With 8 rounds and high effort now in play, the re-run
tests whether the producer solves this blind before any complexity is added
for it. If it does not, surfacing the built image to the verifier on a
measurement-only failure is the next change to argue.

### Attrition matters for the 100 target

4 of 15 sampled tasks (27%) could not be attempted at all for want of stage-4
packages or stage-5 `pr_context`. If that rate holds, 100 verified containers
needs on the order of 140 attemptable tasks before any repair failure is
counted. **Stage 4 and 5 coverage is an independent constraint on the goal**,
and no improvement to the gate or the producer touches it.

### The re-run

Started 07:24 on the identical seeded sample at `max_rounds=8`,
`effort=high`, `model=gpt-5.6-luna`. Same tasks, same seed, so the delta is
attributable to those two changes rather than to sample variation.


---

## Appendix D: the round-budget change was WRONG, and the real constraint

The re-run of the identical seeded sample at `max_rounds=8`,
`effort=high` refutes appendix C's recommendation. Recorded in full because a
plan that keeps only its confirmed predictions is worth nothing.

| task | baseline (3 rounds, medium) | re-run (8 rounds, high) |
|---|---|---|
| TileDB-Inc/TileDB-Py#869 | budget @3 | budget @8 |
| pytroll/satpy#2998 | budget @3 | budget @8 |
| IntelPython/dpctl#1651 | budget @3 | **no_progress @4** |
| sourmash-bio/sourmash#1946 | budget @3 | **no_progress @3** |
| pangeo-data/climpred#515 | no_progress @3 | no_progress @4 |
| geopandas#472, mujoco_warp#633, Qcodes#5320, hangar-py#84 | no_progress | no_progress, same round |

**0 successes in 20 attempts across both configurations.**

### What the mistake was

Appendix C inferred from a 50% `budget` rate that the cap was cutting off
repairs that would otherwise converge. That inference was wrong. `budget` means
only "the producer had not yet repeated a progress key" -- it is the absence of
a stall signal, not evidence of convergence. Given more room, two of the four
`budget` tasks stalled EARLIER than their old cap (dpctl at 4, sourmash at 3),
and the two that used all eight rounds still did not converge.

Doubling the reasoning effort changed nothing either.

The change is harmless -- stopping rule 3 still ends a stalled producer, so the
extra rounds cost nothing on tasks that stall -- and it stays. But it is not
the lever, and appendix C should be read with this appendix attached.

### The actual constraint: benchmark measurability

```
benchmark_information:  35,152 measurements across      15 repos
repos with perf PRs:                                   148 repos
```

**Only ~10% of repositories have ever demonstrated a working ASV suite.** The
seeded sample was drawn uniformly across 130 repositories, so roughly nine in
ten of its tasks came from repositories that cannot produce a measurable
container however well the build is repaired. That is the same wall reached
independently three times tonight:

- oggm#1830 -- `benchmarks/` directory absent from the container entirely;
- fluids#38 -- `benchmark_dir` commented out, no `__init__.py`, discovers zero;
- the sweep -- 8 rounds ending in `asv_exec_failed` and `benchmark_dest_missing`.

A FormulaCode container that measures zero benchmarks is worthless however
cleanly it builds. Exactly 7 containers have ever measured successfully.

### The reachable pool

Restricting to the 15 proven-measurable repositories:

```
Qiskit/qiskit        785      networkx/networkx     153      UXARRAY/uxarray    49
xdslproject/xdsl     345      scikit-image          125      shapely/shapely    40
modin-project/modin  294      pybamm-team/PyBaMM     94      TileDB-Py          32
optuna/optuna        235      xarray-contrib/flox    57      bottleneck         22
pydata/xarray        206      pymc-devs/pymc3        56      pybop-team/PyBOP   17
                                              TOTAL: 2510 perf PRs, all with resolved packages
```

**2,510 candidates for a 100-container target -- a 25:1 ratio.** The pool was
never the problem; the sampling was.

### What to do

1. Draw the scale run from those 2,510, not uniformly across the corpus.
2. Run stage 9 to populate `benchmark_codes` (currently 0 rows). It AST-parses
   benchmark functions straight from a checked-out repository, so it can widen
   the known-measurable set WITHOUT building anything -- the cheapest possible
   way to find out which of the other 133 repositories have a usable suite.
3. Treat "does this repository have a measurable ASV suite" as a stage-0
   admission test. Building a container for a repository that cannot be
   measured spends 30-50 minutes to produce something the dataset cannot use.


---

## Appendix E: the stall detector was blind, and what it cost

Found 2026-08-25 by reading the round-by-round trace the plan logging had just
made visible. **This is the single most expensive defect of the session.**

### The bug

`loop._signature` has two branches. Branch 2 strips BuildKit's step/elapsed
stamp with `re.sub(r"^[#\d.\s]+", "", ln)`. Branch 1 -- taken whenever the
line names a known cause, which is the COMMON case -- did not:

```python
for ln in reversed(lines):
    if any(marker in ln for marker in _NAMED_CAUSES):
        return ln[:90].replace("|", "/")      # <- no strip
...
    body = re.sub(r"^[#\d.\s]+", "", ln)     # <- strips
```

BuildKit stamps every line with elapsed seconds, so the progress key carried a
TIMESTAMP. Two identical failures never compared equal, and stopping rule 3 --
"the same failure again, stop" -- could not fire on the failures it existed to
catch.

### What it cost, measured

From the targeted sweep's own log:

```
17 progress keys recorded -> 16 "distinct" as recorded (pre-fix)
                          ->  4 distinct after stripping the timing prefix

  8x  "conclude that your requirements are unsatisfiable."
  4x  "ModuleNotFoundError: No module named 'pkg_resources'"
  3x  "Failed to build installable wheels for some pyproject.toml based projects"
  2x  "Build manifest invariant violations: asv_exec_failed"
```

**One task repeated one unsatisfiable-requirements failure eight times** and
the loop scored every repeat as progress. `4.213 ...`, `3.760 ...`,
`4.290 ...`, `4.368 ...` -- the elapsed seconds differed, so the keys differed.
Each wasted round is a full container build. TileDB-Py#869 and satpy#2998 did
the same on a wheel build.

### It also invalidated a conclusion

Appendix C read a 50% `budget` rate as "these repairs were still progressing
when the cap cut them off" and recommended raising the budget. That reading was
an artefact of this bug: `budget` largely meant "the stall detector could not
fire", not "the producer was converging".

Appendix D then recorded the round-budget change as refuted. **Both appendices
are half right, and only this fix reconciles them:**

- Raising the budget WAS correct -- numpy-financial#47 needed round 5 to close
  the loop, which a cap of 3 made unreachable.
- Most `budget` stops WERE fake -- 17 keys collapse to 4 real failures.

With the signature fixed, rule 3 fires at round 2 on a repeated failure and the
extra rounds go to repairs that are genuinely moving instead of to loops
re-running one error. The two changes are complementary, not contradictory, and
neither is safe without the other: a raised budget on a blind stall detector
multiplies wasted builds.

### Fixed in both implementations

`_strip_buildkit_prefix` now runs in both branches, in `loop.py` AND in
`scripts/prepass_trial.py`. The duplication exists because `scripts/` is not
importable; the handoff records that the two silently diverged once before, so
a test asserts they strip identically. Six regression tests; both mutations
caught (removing the strip; letting prepass drift).


---

## Appendix F: successes cluster — the route to 100 is repos, not tasks

The single most useful strategic finding of 2026-08-25, and it was not
predicted by anything earlier in this document.

### Observed

```
random sample (15 repos, 1 PR each)      0 successes / 11 attempted
targeted sample (proven-measurable)      3 successes /  ~10 attempted
   ... of which, in the SAME repo:       3   (xdslproject/xdsl)
   ... of which came from neighbours:    2
```

`synthesize_images` enqueues chronologically adjacent PRs after each success:

```
Enqueued 18 neighbor PR(s) for xdslproject/xdsl#1332 (+/-60 days)
```

xdsl#1332 succeeded, 18 neighbours were queued, and two of them have since
succeeded as well. **Containers do not accrue one build at a time; they
cluster around each solved repository.**

### Why

The expensive thing is not the container -- it is discovering a working
recipe for the repository: which setuptools bound, which resolver, which build
backend, which ASV config. Once the producer has solved that for one PR, the
neighbouring PRs of the same repository inherit a nearly-solved environment.
The first container in a repo costs 5-8 rounds; the rest cost far less.

### What this implies for the target

100 containers is **not** 100 independent repairs. It is roughly 10-15
repositories whose recipe can be solved once, then harvested. That reframes
every knob examined tonight:

- `DATASMITH_PV_MAX_ROUNDS=8` and `effort=high` matter for the FIRST container
  in a repository -- the expensive one. They are cheap insurance thereafter.
- Repository selection matters far more than task selection. A repository that
  cannot be solved wastes its whole neighbourhood; one that can, pays for the
  discovery many times over.
- The two structural blockers found tonight are therefore per-REPOSITORY costs,
  not per-task ones: no measurable ASV suite (kills the whole neighbourhood),
  and pre-PEP-625 sdists rejected by `uv` (solvable once per repository, and
  the producer already does it unprompted -- "Use pip for runtime helper and
  legacy dependency installation instead of uv").

### The corresponding risk

Clustering cuts both ways. Three containers from one repository is three
containers from ONE recipe, and a corpus of 100 drawn from 12 repositories is
far less diverse than 100 drawn from 100. `pull_requests` has 148 repositories
with performance commits; a scale run should cap per-repository harvest rather
than take the cheapest 100, or the dataset will measure a dozen build recipes
rather than the ecosystem.
