# Pre-pass progress log

Running notes for the autonomous session of 2026-08-23. Written as work lands,
so the state survives if the session ends.

**Goal:** 100 to 200 honest containers, where the package works, pytest and asv
run, Harbor runs, and the pipeline scales.

**Spec:** `2026-08-23-reproducible-container-build-design.md`
**Plan:** `2026-08-23-reproducible-container-prepass.md`

---

## The finding that reframes the whole pipeline

`docker_build_run.sh` called `asv machine` bare. `asv` lives in
`/opt/conda/envs/$ENV_NAME/bin`, while `PATH` carries `/opt/conda/bin`, the base
conda. The call exits 127 and fails the run stage.

Measured over the 1856 stored `build_run_sh` scripts:

| | rows | repos |
|---|---|---|
| contain an `asv machine` line | 1847 | |
| ... env made explicit **on that line** | 1776 | |
| ... still **bare** on that line | 71 | 35 |
| contain no `asv machine` line | 9 | |
| activate the environment **somewhere in the file** | 1856 | 134 of 134 |

So every repository's script activates the environment, and 1776 of the 1847
that call `asv machine` do it on that exact line. The 71 bare ones activate
earlier in the file instead.

The first thing every synthesis agent did was repair our own template. This is
the largest single reason the no-agent path never succeeded, and it means the
measured agent cost was spent substantially on our bug rather than on the
repositories.

**Correction.** An earlier version of this log, and the commit message of
`2cdce16`, both say "all 1856 scripts add an activation to that line". That is
overstated by 80 rows. The load-bearing claim survives -- all 134 repositories
repair it -- but the headline number was wrong. Found by a verification agent
and reproduced independently.

Fixed in `2cdce16`, with a guard that scans every build-stage template for bare
calls to environment-only binaries.

## The no-agent path now works, and produces an honest container

`networkx/networkx#8148`, built 2026-08-23 08:50 with `--agent none` and
`DATASMITH_SKIP_SIMILAR_CONTEXTS=1`, so nothing but the stock template was used:

```
Default template build succeeded for networkx/networkx#8148   1695s
6754 passed, 23 skipped, 2 xfailed
honesty gate: HONEST, 10 of 10 checks
  sitecustomize  none      <- the shim is gone
  benchmarks     38
  pytest         6779 collected
```

This is the first time TRY_DEFAULT has ever succeeded for this task. Two of our
own template bugs were the only blockers.

**Caveat 1. `extensions_import` is unproven.** networkx is pure Python, so the
gate reported `extensions 0/0`. Every container gated so far has had zero
compiled extensions. Nobody should read "10 of 10 passed" as covering compiled
code. The first real test of that check is a compiled repository. If it reports
`0/0` there, the check is broken rather than the container.

**Caveat 2.** The gate is defeatable, and I proved it. A 19-line adversarial
`sitecustomize.py` that patches `Path.is_file` and `shutil.which` made the gate
report HONEST on a container with a replaced `grep`. This container passes
because no agent built it, so nothing had motive or opportunity. The gate
passing is consistent evidence, not proof.

## A third of the corpus was excluded from the no-agent path

Stage 6 required a non-empty `candidate_prs` row (`issues_json` or
`initial_observations`) before it would attempt any build. That context exists
to give an LLM agent something to work with. `TRY_DEFAULT` never reads it.

    performance PRs   12944
    eligible           8879
    excluded           4065   (31%)

Found by pinning `pydata/bottleneck#468`, which has a `can_install` packages row
and a `rendered_problem`. The log read:

    --tasks filter selected 1 PR(s)
    Synthesizing images for 0 PRs

Both skip reasons logged at DEBUG, so at normal level the task simply vanished.
Fixed in `6d4b407`. The filter now applies only when an LLM agent will run,
dropped pinned tasks are named in a warning, and skip counts report at INFO.

This is the third of our own defects that blocked the no-agent path, after the
bare `asv` call and the self-matching secret scan. None of the three was a
repository problem.

## No existing container is honest

`networkx#8148`, the container whose 1.602x speedup was reported as the good
result, carries this in site-packages:

```python
import builtins
import sys as _sys
if not hasattr(builtins, 'sys'):
    builtins.sys = _sys
```

That is the workaround for our missing `import sys` in `pytest_runner.py`, and
it runs in every Python process including the measured benchmark. 130 of 134
repositories carry one. Regenerating the corpus is the right call.

---

## Landed

| Commit | Change |
|---|---|
| `dd0f574` | `import sys` in pytest_runner, plus a ruff guard over the excluded template directories |
| `f621e33` | read `asv.conf.json` with dict access, not `getattr` -- the whole read was a no-op |
| `301ac37` | honesty gate for built containers, validated against a known tamper |
| `caac23e` | read the asv `matrix` as name-to-versions instead of discarding the keys |
| `c632ef8` | prune Docker on disk pressure, not on a timer |
| `6037ee7` | record the no-agent build outcome in `error_logs` |
| `b91af24` | honour `--tasks` in stage 6, not only stage 7 |
| `44eda59` | `DATASMITH_SKIP_SIMILAR_CONTEXTS`, so a build cannot inherit an agent's context |
| `be57a2c` | block the test suite from writing to a real database, and carry build output into failure rows |
| `2cdce16` | activate the env before calling asv in the run stage |

Suite: 895 passing, mypy clean.

## The honesty gate

`scripts/honesty_probe.py` collects facts inside the container.
`scripts/container_honesty.py` decides what they mean. The split exists so a
script cannot quietly stop doing one of the two.

Validated against `dynamicslab/pysindy#139`, which fails four checks:
`python_is_elf`, `python_not_wrapped`, `grep_is_system`, `no_sitecustomize`.

The first version of the gate was defeated by that container. A bash wrapper
that `exec`s the real interpreter makes `sys.executable` report the target, so
the ELF test passed with the wrapper installed. The gate now tests the file
PATH resolves and flags disagreement between the two.

Harbor is not yet part of the gate. Stage 7 with `--harbor-environment docker`
is the intended route, and harbor 0.1.43 imports cleanly.

## Two defects found in my own work, both caught by verification

- The `dropped_count` counter returned 0 when it could not detect drops, which
  reads as "no drops". Now three-valued.
- The honesty gate's first interpreter check was defeated by the wrapper it
  existed to detect.

Neither was caught by me. Both were caught by re-running against real data.

---

## Not yet done

1. **Harbor in the honesty gate.** Required by the goal.
2. **Cache mounts.** Three of them, pip, conda, ccache, plus dropping the two
   `micromamba clean --all` calls. The prune fix was the prerequisite.
3. **`TRY_SIMILAR` keying.** It keys on `(owner, repo)` and orders by PR date.
   At 1.70 commits per lock set, keying on the dependency set is likely the real
   caching win the goal asks for.
4. **The pins gate.** `_c_pins` strips versions from both sides, so
   `cython==3.0.5` passes against `cython==0.29.33`. `pins_resolved` is also
   whitespace-split, which breaks direct references such as
   `archspec @ file:///...`.
5. **Unpinned toolchain.** compilers, asv, LSV and snapshot-tester all install
   from a moving source.
6. **The 20-repository trial.** Task 5 of the plan, still held back.

## Rules being followed while unsupervised

- Two failed attempts on a repository, then move on and record the signature.
- Every first-principles edit lands with its regression test in the same commit.
- Explicit paths on every `git add`. Ten unrelated files are modified in the
  tree and must not be committed.
- Containers pinned to a cpuset inside the 96-core budget.
