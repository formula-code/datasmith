# Developing LSV alongside fc-data

LSV (`asv.contrib.lightspeed`) measures the speedup in a container. It selects
the benchmarks that a patch affects, times them before and after the patch, and
passes the numbers to `measure.sh`, then `emit_measure.py`, then the build manifest.

It lives in a **separate repository** (formula-code/lsv, a fork of asv), so a
change to how FormulaCode measures is a two-repo change.

## Which LSV an image has

Images install LSV from GitHub at one pinned commit. The pin is in two places,
and they must stay the same:

- `LSV_REF` in `src/datasmith/harbor_adapter/template/environment/Dockerfile`
  (task images built from rendered tasks)
- the `uv pip install git+https://github.com/formula-code/lsv.git@<sha>` line in
  `src/datasmith/docker/templates/docker_build_final.sh` (stage 6 images)

To change LSV: push a commit to formula-code/lsv, update both pins, then
re-render and rebuild the tasks that need it.

## Two packaging details

### The distribution is named `lsv`, the import package is `asv`

`pyproject.toml` declares `name = "lsv"` while shipping the `asv/` package.
Two consequences:

- `SETUPTOOLS_SCM_PRETEND_VERSION_FOR_**LSV**` sets the version, not
  `..._FOR_ASV`. Without it, an install from a tree with no `.git` fails,
  because setuptools_scm has nothing to read.
- `asv/_dist_version.py` asks `importlib_metadata` for the `lsv` version first
  and then for `asv`. The `asv` name resolves only when the PyPI `asv`
  distribution is also installed.

### The PyPI `asv` wheel must be uninstalled first

Two copies of the `asv` package on `sys.path` break plugin discovery:
`pkgutil.iter_modules` reads one copy while the `Command` base class comes
from the other, so the command registry is empty and the CLI fails with
`KeyError: 'Quickstart'`. Uninstall the wheel before installing the fork.

Because the fork then provides the CLI, `pyproject.toml` declares an `asv`
console script next to `lsv`, because `docker_build_final.sh` runs
`asv run --bench just-discover` for benchmark discovery.

## Iterating without rebuilding an image

The timing wrappers are datasmith templates, not LSV code. Bind-mount the
working tree copies over a rendered task's `tests/` to try a change (Harbor
uploads `tests/` at verification time, so mount the whole folder too):

```bash
D=src/datasmith/harbor_adapter/template/tests
docker run --rm \
  -v <task>/tests:/tests:ro \
  -v $D/lsv_init.py:/tests/lsv_init.py:ro \
  -v $D/lsv_measure.py:/tests/lsv_measure.py:ro \
  <task-image> bash /tests/test.sh
```

> **Check the version you are overriding.** A task rendered from another
> template tag can carry a newer copy. Diff before you mount.

## Timing hyperparameters

`rounds` maps to asv's `processes`; `repeat` is samples per round. asv's
auto-repeat picks 1–10 and **halves it when `rounds > 1`**, so more
`rounds` alone does not always give more accuracy.

`asv._stats.is_different` runs a Mann-Whitney U test at `p_threshold=0.002`
when raw samples are present, and otherwise uses a conservative 99% CI-overlap
check. Its lower bound,
`p_min = 1/binom(n_a + n_b, min(n_a, n_b))`, means the Mann-Whitney branch
**cannot fire below 6 samples per leg**:

| samples per leg | `binom` | `p_min` | branch taken |
|---|---|---|---|
| 5 + 5 | 252 | 0.00397 | CI-overlap (weak) |
| 6 + 6 | 924 | 0.00108 | Mann-Whitney U |

These variables set them:

| variable | consumed by | default |
|---|---|---|
| `DATASMITH_VERIFY_MEASURE_ROUNDS` | `measure.sh` → `--rounds` | 2 |
| `DATASMITH_VERIFY_MEASURE_REPEAT` | `measure.sh` → `--repeat` | asv auto |
| `DATASMITH_VERIFY_MEASURE_WARMUP_S` | `measure.sh` → `--warmup-time` | asv auto |

`asv.contrib.lightspeed` passes all three through `_timing_params`.

## Changes to the fork

A change to formula-code/lsv changes how every task image measures. Get a review
before you push it, and do not push experiments.
