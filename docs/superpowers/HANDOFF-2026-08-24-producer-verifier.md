# Handoff: producer/verifier gate, 2026-08-24

Written for the next agent. Read this before the spec or the plan.

**Spec:** `docs/superpowers/specs/2026-08-23-producer-verifier-design.md`
**Plan:** `docs/superpowers/plans/2026-08-23-producer-verifier.md` (12 tasks)
**Validation result:** `docs/superpowers/plans/pv-validation.md`
**Branch:** `spec/ingestion-window` (not the branch the session snapshot names)

---

## 1. The one thing that matters

**The verifier ACCEPTED `formulacode/attack-demo:1`.** That is the negative
control, and the spec says in section 9 that a verifier which accepts it "is
worse than the script it replaces".

The cause is a design error, not a bug. `attack-demo:1` carries 19 lines of
adversarial `sitecustomize.py` that patch `Path.is_file` and `shutil.which`. The
battery's `integrity_probe` runs that probe **inside the container**, so the same
shim that defeated the deterministic honesty gate also lied to the probe. The
verifier then reasoned correctly from false facts.

The probe was known to be defeatable before the verifier was designed on top of
it. That is the mistake to not repeat.

**Integrity facts must be collected from OUTSIDE the container.** Image-layer
inspection (`docker save` / `docker history`, walking the tar), not a program
the container gets to execute. Anything the container runs, the container can
lie about.

`DATASMITH_PV_ENABLED` is 0 and must stay 0 until the gate rejects
`attack-demo:1`.

## 2. State

| | |
|---|---|
| Plan tasks landed | 10 of 12 (`agents/reflexive/` complete, wired into stage 6) |
| Task 11 (validation) | RAN, result FAIL. The agent died on an API 529 after writing results |
| Task 12 (end-to-end round) | not started |
| Suite | 1337 passing, 0 failures, mypy clean on `src` |

Confusion matrix from the 16-container set:

```
true_accept 3 | true_reject 7 | false_accept 1 | false_reject 3 | either 2
```

The single false accept is `attack-demo:1`. The three false rejects are
`trackintel#596`, `fluids#38` and `tiled#1283` — all honest containers, so the
instrument is noisy in both directions, though only the false accept blocks the
pass criterion.

### Honest containers today

Four pass the honesty gate: `networkx#8148`, `pydata/bottleneck#468`,
`mie-lab/trackintel#596`, `xarray-contrib/xbatcher#167`. `bluesky/tiled#1283`
built successfully but was never gated.

`candidate_containers` has 1858 rows, but only 7 carry a manifest and 130 of 134
repositories carry a `builtins.sys` shim injected to work around a defect since
fixed. **That corpus is tainted and is meant to be regenerated**, so do not read
1858 as progress toward the target.

## 3. Traps that cost real time today

- **`pyproject.toml` sets `fix = true`.** A bare `uv run ruff check src/ tests/`
  REWRITES files. Three other Claude sessions share this branch and write it
  concurrently. Always use `ruff check --no-fix`.
- **`[tool.mypy] files = ["src"]`.** mypy never sees `tests/` or `scripts/`. A
  "mypy clean" claim is not evidence about a test file.
- **`src/datasmith/agents/templates/` and `docker/templates/` are excluded from
  ruff and mypy** with `force-exclude`. `tests/docker/test_template_lint.py`
  re-enables F821/F822/F811 there — that guard exists because `sys.exit()` once
  shipped without `import sys`.
- **Never `git add -A`, `git add .`, or `git commit -a`.** Other sessions leave
  files staged in the index; one of them left `classifiers.py` staged mid-run.
  Use explicit paths.
- **Never `docker volume prune`.** The local Supabase database lives in a volume.
- **Battery cost.** 8 commands x 1800s x 16 containers is 64h worst case. Set
  `DATASMITH_PV_BATTERY_TIMEOUT_S=600` for validation runs.
- **`scripts/status.py`** answers "what is building / what did harbor do" in
  0.35s via direct Postgres. `fetch_all` pages whole tables through PostgREST and
  took 60–120s for the same question.

## 4. Where the bodies are buried

Two classes of defect recurred all session, and both will recur again.

**Silent drops.** Things that fail by producing nothing rather than an error:
`emit_manifest.py` reads a FIXED key list, so a new `fc_note` breadcrumb is
written and discarded (a test now walks the templates and fails on any dropped
key). `_PROBE_SRC` resolved to a path that does not exist, so `is_file()` was
false and the read-only mount was silently skipped. `Benchmarks.load` reads a
file that does not exist until asv has run, so a benchmark check SKIPPED rather
than failed.

**Guards that do not guard.** `test_..._agrees_with_the_prepass_one` was written
to stop two `_signature` implementations drifting; they had already drifted on
two axes and every test case hit the one branch where they agreed. The tunables
guard globbed one subpackage while its docstring promised the codebase. The
PRODUCE_VERIFY branch had five tests, all of which grep `inspect.getsource` and
would stay green if the wiring were wrong.

**Mutation-test any guard you rely on.** Break the code deliberately and confirm
the test fails. Three of the defects above were found that way and one was not
caught because the mutation hit the wrong one of three identical lines.

## 5. What to do, in order

1. **Rebuild integrity collection from outside the container.** Then re-run
   `python scripts/pv_validate.py`. The gate must reject `attack-demo:1` and
   `pysindy#139`. Nothing else matters until this passes.
2. **Investigate the three false rejects** before trusting the instrument. All
   three are containers known to be sound.
3. **Task 12**, the end-to-end round on `OGGM/oggm#1830` (fails on one missing
   dependency, `salem`, so the loop should close in two rounds).
4. **Only then flip `DATASMITH_PV_ENABLED`** and scale. The pass criterion is in
   the spec, section 9: both negative controls rejected, zero false accepts in
   the hard class, every disagreement explained, one end-to-end round completed.

## 6. Do not fit the gate to its own validation

The severity table in `agents/reflexive/severity.py` is a starting position, not
a result. If a disagreement suggests the table is wrong, record the finding and
argue the change separately. Adjusting the table until the confusion matrix looks
good is how a gate stops meaning anything.
