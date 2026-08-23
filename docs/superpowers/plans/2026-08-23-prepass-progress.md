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

### Second container, compiled, also honest

`pydata/bottleneck#468`, built 2026-08-23 10:29 the same way:

```
Default template build succeeded   2597s
honesty gate: HONEST, 10 of 10
  extensions     4/4 import     <- the check finally fired
  benchmarks     51
  pytest         192 collected
```

`extensions_import` now has a real test and passes it. Caveat 1 below is
resolved, and kept for the record.

**Caveat 1, RESOLVED. `extensions_import` was unproven.** networkx is pure Python, so the
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

---

## The 24-repository trial, and what it measured

Run at 12:19 on 2026-08-23. 24 repositories, one task each, ONE stage-6 run at
concurrency 16, `--agent none` with TRY_SIMILAR disabled. Load reached 43, so
the parallelism was real.

**The denominator is not a build rate.** Template fixes landed while the trial
was in flight, and build contexts synthesize per task, so early tasks ran the
pre-fix templates and later ones did not. The value of this run is the
distribution of failure signatures.

Two repositories built: `mie-lab/trackintel#596` (510s) and
`xarray-contrib/xbatcher#167` (316s).

### Signature distribution, 22 failures

| n | cluster | fixed? |
|---|---|---|
| 7 | PEP 517 build backend missing | yes, `cc85734` |
| 5 | unclassified, see below | partly |
| 4 | missing dependency at import | no |
| 4 | pytest collection aborted | yes, `d1e20fc` |
| 1 | editable metadata generation failed | partly |
| 1 | commit SHA no longer a tree | no, upstream |

The two landed fixes address 11 of the 22 directly, and the source-count change
in `a79e49d` covers most of the unclassified group.

### Selection was biased, and the first trial measured nothing

The harness took the LOWEST issue number per repository. Reproducible, but it
picks the oldest commit in every repo -- the one with the most dependency rot.
All four tasks in the first run failed inside 40 seconds on a missing asv
config, a SHA that is no longer a tree, setuptools metadata, and a missing
interpreter. None of that says anything about whether the pipeline works today.
Now takes the newest commit. Fixed in `6a3780d`.

## Four more defects of ours, all found by reading the failures

Every one made a working repository look broken.

**1. `micromamba remove` pruned the interpreter away** (`22ffd85`). The env
stage removes the package under test before reinstalling. micromamba prunes
dependencies by DEFAULT, so the removal took its dependency closure with it.
`apache/arrow#1646` unlinked libarchive here and died several steps later on
"No virtual environment or system Python installation found for path
/opt/conda/envs/asv_3.8/bin/python". That env is present and healthy in
`formulacode/base:latest` -- verified directly. Our own removal broke it. Every
command in the loop ends in `|| true`, so nothing reported the real cause.

**2. `--no-build-isolation` with no backend installed** (`cc85734`). The
editable install only ever ran with `--no-build-isolation`, which requires the
PEP 517 backend to be present already. Nothing ever put it there. The retry
differed from the first attempt only in `$EXTRAS`, so both failed identically
for every repo using hatchling, scikit-build-core, flit, poetry-core or
uv_build. Seven of 22 failures.

Deliberately installs only the distribution behind `build-backend`, not the
whole `build-system.requires` list: projects routinely pin an old numpy there
for build-time ABI reasons, and installing that would compile the extension
against one numpy and import another at measurement time -- which passes the
smoke check and mismeasures later.

**3. Collection was unscoped, and one bad module hid the suite** (`d1e20fc`).
`run_pytest_and_collect([])` handed pytest an empty argument list, which means
"collect from rootdir". The comment directly above it says the paths are made
absolute "to avoid collecting the whole tree accidentally".

`CalebBell/fluids#38` died on `ERROR docs/conf.py` and
`ERROR jinja_patch_plugin_pandas.py`. Neither is a test. fluids sets
`--doctest-modules` in addopts and declares no `testpaths`, so pytest imported
every .py in the tree. **`jinja_patch_plugin_pandas.py` is our own file** --
`run-tests.sh` writes a pandas-specific jinja shim into every repository root.
So we planted the file and then told pytest to import it.

Separately, pytest stops at the first collection error, so one missing optional
dependency reported zero tests -- indistinguishable from a repo with no suite.
`NCAR/geocat-comp#748` (dask) and `AllenCellModeling/aicsimageio#486`
(bioformats) both aborted that way.

**4. "No benchmarks" and "our discovery broke" printed the same line**
(`a79e49d`). `run-tests.sh` exits 78 when `asv_benchmarks.txt` is empty, saying
the task "has no benchmarks and cannot be used in the FormulaCode dataset".
Six repositories hit it, including `pydata/xarray` and `joblib/joblib`.

joblib had already run **1526 passing tests** before reaching that line. Both
the build-time and runtime paths end in `Benchmarks.load`, which reads
`results/benchmarks.json`; a benchmark module importing an absent dependency
yields zero, which reads identically to a repo with no suite. Now counts the
suite from source with `ast` first, and only a real zero writes a task off.

## The honesty gate had the same defect

`benchmarks_discovered` also read only `Benchmarks.load`, so on any image that
had never run asv it returned None and **skipped**. A container with no
benchmarks was indistinguishable from one that had not run yet. The probe's
docstring claimed it asked asv to discover "rather than reading a file the
build wrote"; it was reading a file the build wrote. Fixed in `1ff26b4`, with
an independent `ast` count of the benchmark directory.

`pytest_collects` returned `rc in (0, 5)`. pandas collects 205357 tests and
then exits non-zero because its own addopts turn a deprecation warning into an
error. That is the repository's warning policy, not a broken container.

## Open, and honestly unresolved

- **Harbor.** Started on networkx#8148 and bottleneck#468 at 12:22 with
  `--harbor-environment docker`. Still running at the time of writing. No
  result yet, so no claim either way.
- **Import failures, 4 repos, and they are NOT all the same problem.** Filing
  these together as "missing dependency" would send the next reader after the
  wrong fix.
  - `imp` (satpy) and `pkg_resources` (mars) are **Python-version
    misassignment**. Both got `asv_3.12`. `imp` was removed in 3.12, and
    setuptools is no longer present by default. Neither is a resolution gap;
    stage 4 chose a Python the code cannot run on.
  - `salem` (oggm) and `src` (napari) may be genuine resolution gaps. Not
    diagnosed.
- **`warp-lang==1.13.0.dev20260302`** (mujoco_warp) no longer exists on PyPI.
  Stage 4 pinned a dev build. Version-reproducibility cannot be achieved
  against a version that was deleted.
- **Compiled wheel failures** (shapely, msprime) need the truncated compiler
  output to diagnose.
- **A load-sensitive flake.** `test_missing_patch_is_fetched_then_classified`
  failed once with a KeyError while 22 builds were running at load 34.9. It
  passes 12 of 12 at load 17.6, and passes at all four of the runner commits
  that preceded it, so it is not a regression from this work. It is a threaded
  test asserting on mock call order. Recorded, not fixed.
