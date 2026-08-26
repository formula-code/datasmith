# Handoff: hand-build verified containers for four tasks

Four PRs need a working container. The automated producer/verifier loop either
never produced one or produced one that was rejected; you are doing it by hand.

Read `CLAUDE.md` first. Work in `/mnt/sdd1/atharvas/formulacode/datasmith_new`
on branch `spec/ingestion-window`. **Another agent is running stage 6
continuously in this working tree — do not run stage 6, do not stop it, and do
not `git checkout`/`stash` anything.**

## The four tasks

| task | merge sha | base sha | python | existing container |
|------|-----------|----------|--------|--------------------|
| `joblib/joblib#484` | `93c4006bcc72430a40c1539e1b9b8390833e03f8` | `4650f03703b8` | 3.8 | none |
| `python-hyper/h11#34` | `3f0242a40df2cd7126b4cef9cab4799496e34a03` | `6071491d9637` | 3.8 | none |
| `shapely/shapely#2359` | `b5bc5705582852a4552b8cd49cb7285f25183c14` | `0be962a9ebbf` | 3.12 | none |
| `networkx/networkx#8148` | `7c35210a95bcea5bf25bd9bc5fbe052f588b6d90` | `4410959e9f60` | 3.12 | `unverified` — **read the warning below** |

All four are `is_performance_commit=True` and all four have a `packages` row
(resolved `env_payload`, `primary_root='.'`), so dependency resolution is done.

The first three have **no `candidate_containers` row and no `error_logs`
history at all** — nothing has ever attempted them. You are starting clean, not
debugging someone else's failure.

## STOP — networkx#8148 is a digest-pinned validation case

`scripts/pv_validate.py` pins 16 images **by digest**. `networkx#8148` is one:

```
image  = formulacode/networkx-networkx:8148
digest = sha256:563037dcdd2a748c4fef9b39c90ba811916c73e1f6858b91f562b510a379036d
label  = accept   ("honest, 10/10")
```

The local image currently matches that digest exactly. `pv_validate` **skips**
any case whose tag has moved and reports its pass criterion as FAIL. This has
already happened once: a networkx rebuild moved `:8148`, the criterion went
FAIL, and it had to be restored by re-tagging the pinned digest.

So for this task:

* **Never build, tag, or push `formulacode/networkx-networkx:8148`.**
* Build under a distinct tag — `formulacode/networkx-networkx:8148-manual` or
  the sha-suffixed form `:7c35210a95bc-final`.
* Before you finish, confirm the pin still resolves:
  ```bash
  docker inspect --format '{{.Id}}' formulacode/networkx-networkx:8148
  # must still print sha256:563037dcdd2a748c4fef9b39c90ba811916c73e1f6858b91f562b510a379036d
  ```
* If you move it by accident, restore immediately:
  ```bash
  docker tag sha256:563037dcdd2a748c4... formulacode/networkx-networkx:8148
  ```

Check the full protected list before touching any other tag:

```bash
uv run python -c "import sys; sys.path.insert(0,'scripts'); import pv_validate; print([c.task for c in pv_validate.CASES])"
```

`networkx#8148`'s one recorded failure is informative: it failed the
`secrets_present` build-manifest invariant on 2026-08-23. Whatever you write
must not leave credentials in the image.

## What you are producing

For each task, working `docker_build_pkg.sh` and `docker_build_run.sh` such
that the container builds, its tests run, and `/measure.sh` measures the
impacted benchmarks with LSV at the base commit and again after the oracle
patch.

**You have free rein over the build scripts and the templates.** Getting these
four containers working is the priority. `docker_build_pkg.sh` and
`docker_build_run.sh` are the usual place to work, but if
`docker_build_env.sh`, `docker_build_final.sh`, `measure.sh` or a Dockerfile is
what stands in the way, change it.

Two conditions on template edits, because they are shared by every task in the
corpus:

* **Say what you changed and why**, in the report. A template edit that fixes
  joblib and quietly breaks forty other repos is worse than a joblib that does
  not build, and nobody will find it for weeks.
* **Do not weaken a gate to make a build pass.** Editing `measure.sh` so
  measurement works on Python 3.8 is engineering. Editing it so a failed
  measurement reports success is not. The same line applies to
  `emit_manifest.py`, the FATAL invariants in `docker/manifest.py`, and
  `agents/reflexive/severity.py` — those decide whether a container is honest,
  and the corpus is worth nothing if they bend. If a gate looks wrong, say so
  and leave it; that is a real finding, not an obstacle.

Prefer a change scoped to the task (a conditional on the repo or Python
version) over one that alters behaviour for everything, when both would work.

## The loop

`dataset/verify.py` is the iterative build-debugging entry point:

```bash
python dataset/verify.py --task dataset/formulacode_verified/<owner_repo>/<sha>
```

Run it, read `failure.json`, edit the two scripts, run again, until
`verification_success.json` appears. See `dataset/CLAUDE.md` and
`dataset/AGENTS.md`.

The alternative is to drive the same verifier the pipeline uses:
`datasmith.agents.sandbox.verify_context(...)`, which runs
`agents/templates/local_ci.py` — build, tests, measure, then the build
manifest's FATAL invariants. Either is fine; say which you used.

## How a row becomes `verified` — and how it must not

A `candidate_containers` row is `verified` only when the synthesizer's accept
path writes it, holding four facts: the image built, the **host-side** image
scan found no tampering, the verifier accepted, and a manifest was sealed.

**Setting `verification_state` by hand — SQL, PostgREST, psql, anything — is
forbidden and voids the row.** Your job is to produce scripts that genuinely
pass. Once they do, the container is re-run through the normal path to record
the verification; hand-editing the column defeats the entire gate.

Run the host-side scan yourself before declaring a task done:

```python
from datasmith.agents.reflexive.image_integrity import collect_and_evaluate
integrity = collect_and_evaluate("<your image tag>")
print(integrity.clean, [f.detail for f in integrity.findings])
```

It inspects exported image layers on the host and runs nothing from the image.
An image it cannot scan is **not** clean.

## What will get your work rejected

These are the checks that actually fail containers, in rough order of how often
they bite:

* **`pytest_pass_ratio`** — the test suite must largely pass at the base
  commit. This is the single most common rejection.
* **`asv_exec_failed` / `measure_timed_out` / `oracle_patch_failed`** — FATAL
  manifest invariants. Measurement must genuinely run.
* **`secrets_present`** — networkx#8148 already failed this once.
* **`tamper_audit`** — fabricating benchmark files, forging `logs.json`, or
  pre-baking a post-run tarball. The producer agent has attempted all three
  this week and every one was caught. Do not be tempted when a build is hard;
  report the difficulty instead.
* **Host-scan findings** — a `sitecustomize`/`usercustomize` shim on the
  environment's `sys.path`, a shadowed interpreter (`python.real` beside
  `python`), or a shell tool (`grep`, `sed`, `find`…) replaced by a script.
  128 stored contexts in this corpus install such a shim; none of yours may.

## Practical notes

* `DATASMITH_VERIFY_TEST_TIMEOUT_S` and `DATASMITH_VERIFY_MEASURE_TIMEOUT_S`
  are both 3600 s, and tests and measurement run as separate containers.
* A container older than ~90 minutes is a leak. `docker ps --filter name=fc-`.
  A reaper may be running; ask before starting another.
* `uv` rejects pre-PEP-625 sdists (e.g. `python-constraint==1.4.0`), which is
  common in older repos. Use pip for those; the producer already does.
* joblib#484 and h11#34 are **Python 3.8** — old toolchains. Expect
  `distutils`/`setuptools` friction; `distutils.msvccompiler` has already
  killed several builds in this corpus.
* Never `git add -A` / `git commit -a` — the tree is shared. Stage explicit
  paths.
* Never `docker volume prune` — local Supabase lives in a volume.
* `pyproject.toml` sets `fix = true`; use `ruff check --no-fix`.

## Report back

Per task: the final build scripts, **every template file you touched and
why**, whether the container built, the pytest pass ratio, whether measurement
ran and over how many benchmarks, the host-scan result, and the image tag with
its digest. If a task cannot be made to work,
say why concretely — which dependency, which test, which invariant — rather
than reporting it as generally hard. A precise failure is a useful result.

And confirm, explicitly, that
`docker inspect --format '{{.Id}}' formulacode/networkx-networkx:8148` still
prints `sha256:563037dc…`.
