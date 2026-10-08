# Stage 4 (resolve_packages) — bug audit, 2026-08-23

## Method
- Corpus profile: all 13,016 rows of `packages` in local Supabase.
- Fresh probe: 13 Jan-2026 perf commits, one per repo, run through `analyze_commit`
  with a scratch SQLite cache (so the resolver really ran) and the existing 20G git
  cache (so no re-cloning). No Supabase writes. 143s wall, 6 threads.
  Artifacts: `probe/out/*.json`.
- Note: the probe did NOT modify the shared `package_blocklist.json`
  (still 4721 bytes, Apr 15 07:55).
- 143s is a *resolution* number, not an ingestion number — clones were pre-warmed.

## Corpus numbers
| Metric | Value |
|---|---|
| rows in `packages` | 13,016 |
| `can_install = false` | 3,245 (24.9%) |
| `resolution_strategy = unresolved(pass-through)` | 2,903 (22.3%) |
| rows with **zero** version pins | 1,603 (12.3%) |
| pass-through **yet** `can_install = true` | 462 |
| fresh probe failures | 3 / 13 (numpy, scipy, arrow) |

The three fresh failures are numpy, scipy and apache/arrow — the most
benchmark-valuable repos in the corpus.

## Bugs

### B1 — `fix_marker_spacing` corrupts every standard PEP 508 marker  (fatal, verified)
`package_filters.py:173-174` runs an unanchored substitution:
```python
marker = re.sub(r"(?<=[^\s])and(?=[^\s])", " and ", marker)
marker = re.sub(r"(?<=[^\s])or(?=[^\s])",  " or ",  marker)
```
`or` occurs inside `platform`; `and` occurs inside `standard`. Verified output:
```
"numpy; platform_system=='Windows'"  -> "numpy; platf or m_system=='Windows'"
"foo;   platform_machine=='x86_64'"  -> "foo; platf or m_machine=='x86_64'"
"bar;   sys_platform=='linux'"       -> "bar; sys_platf or m=='linux'"
"qux;   extra=='standard'"           -> "qux; extra=='st and ard'"
```
`sys_platform`, `platform_system`, `platform_machine` and `extra` are the four most
common markers in Python packaging.

Kill chain observed on apache/arrow — uv's own error text:
```
error: Couldn't parse requirement in `-` at position 153
  Caused by: Expected a quoted string or a valid marker name, found `sys.platf`
pyuwsgi;sys.platf or m!='win32' and python_version<'3.13'
```
One bad string aborts the whole compile *and* the whole dry-run: there is no
per-requirement isolation. arrow -> `can_install=false` -> dropped from the dataset.

Why it is invisible in the DB: `uv pip compile` output carries no markers, so
corrupted text is never persisted (0/13,016 rows contain it). The damage is
**silent exclusion of repos**, not poisoned rows.

### B2 — the self-healing blocklist is a global, cross-repo, order-dependent side effect  (fatal)
`blocklist.py:21` — `BLOCKLIST_PATH = GIT_CACHE_DIR / "package_blocklist.json"`, one
file for the entire corpus, read at filter time (`package_filters.py:256`) and
appended to whenever any single resolve fails. 268 entries today, including:
- **numpy's own submodules**: `arraypad`, `arrayprint`, `arraysetops`, `defmatrix`,
  `multiarray`, `mtrand`, `nanfunctions`, `stride-tricks`, `ufuncs`, `umath`,
  `chebyshev`, `hermite`, `legendre`
- **stdlib**: `tomllib`, `annotationlib`
- **real, installable PyPI projects**: `black`, `codecov`, `dspy`, `umap`, `rdkit`,
  `xdist`, `atomicwrites`, `cpuinfo`, `graph-tool`, `social-core`, `social-django`
- **conda-only names**: `blas`, `lapack`, `openblas`, `nomkl`, `matplotlib-base`,
  `freetype`, `libjpeg`, `qhull`, `doxygen`, `ccache`
- **bare tokens**: `setup`, `basic`, `compat`, `compute`, `streams`, `generator`,
  `authors`, `feedback`, `com`, `proto`, `svg`

Consequences: resolving repo A changes repo B's result; the same commit resolves
differently depending on run order and on history; the list only ever grows; and it
masks the real defect (B3) instead of fixing it.

### B3 — the import analyzer treats first-party, stdlib and C-symbol names as PyPI distributions
Source of most blocklist entries. numpy's fresh dep list contains `version` and
`plex`; scipy's contains `conda-build`, `torch`, `jax`, `cupy`. `version` is a dead
py2 project whose sdist raises
`ImportError: cannot import name 'izip_longest'` — that single harvested token is
what fails numpy's resolution.

### B4 — dev/doc/CI requirements are harvested as runtime dependencies
Fresh numpy deps include `sphinx`, `pydata-sphinx-theme`, `sphinx-design`,
`sphinx-copybutton`, `jupyterlite-sphinx`, `jupyterlite-pyodide-kernel`, `towncrier`,
`ruff`, `cython-lint`, `gitpython`, `PyInstaller`, `spin`, `numba`, `pandas`,
`matplotlib`. scipy adds `myst-nb`, `jupytext`, `torch`, `jax`, `cupy`, `conda-build`.
scipy's resolution dies on a `conda-build` yanked-version conflict — a
*documentation* requirement makes the environment unsatisfiable.

### B5 — `--all-extras` explodes the environment
`dependency_resolver.py:37` always passes `--all-extras`. PostHog resolves to
**412** packages (aioboto3, azure-ai-agents, anthropic, ...), napari to **291**
(sphinx, bioio-*, botocore). None of it is needed to run a benchmark.

### B6 — the two resolution paths produce different environments, and both fight the base image
The pyproject/setup.py path (`orchestrator.py:352-369`) returns before section D,
the only place that injects `pytest`, `setuptools` and `hypothesis`. So h5py resolves
to exactly one dependency, `numpy==2.4.1`, while numpy and scipy get the tooling
appended. Same stage, two different meanings for `env_payload`.

Correction to an earlier reading: this does **not** leave h5py's container without a
test runner. The base image already installs `hypothesis<5`, `pytest` and
`versioneer` (`docker_build_base.sh:769`), `asv` (`:771`), and `pip setuptools wheel`
(`docker_build_env.sh:262`).

Two caveats on that base-image install, both verified at
`docker_build_base.sh:769-777`: `hypothesis`, `pytest` and `versioneer` go in
best-effort (`>/dev/null 2>&1 || true` — failure swallowed, output discarded), and on
Python >= 3.9 `asv` is installed from **git HEAD**
(`git+https://github.com/airspeed-velocity/asv`), unpinned. The measurement
instrument itself therefore varies between base-image builds.

The real harm is ownership. `env_payload` and the base image both claim the same
packages and can pin them differently — an unconstrained `hypothesis` in
`env_payload` overrides the base image's deliberate `hypothesis<5`. Tooling belongs
to the base image; the seed should carry project dependencies only.

### B7 — `python_version` is an accident of control flow, not a decision
`orchestrator.py:603-607` assigns `python_version`/`resolved_dependencies`/
`can_install` **unconditionally**, before the success check, so a failed iteration
still leaves its interpreter recorded. Candidates are tried newest-first and the
loop `break`s on any non-ABI error, so the answer is "the newest interpreter that
did not crash first". The project's own `requires-python` is parsed
(`uv_build_and_read_metadata`) and then never used to constrain the choice.

Observed: 13 commits from the same month resolved to Python 3.9, 3.11, 3.12, 3.13
and 3.14.

### B8 — results are not reproducible and carry no provenance
Same 13 SHAs, stored vs fresh: dependency sets agree (jaccard 1.00 on 10/13) but
**`python_version` differs on 7 of 13** — every stored row says 3.12, the fresh run
says 3.9-3.14. apache/arrow drifted in deps too (31 -> 45 packages, jaccard 0.69).
No column records resolver version, run date, uv version or blocklist state, so
rows produced months apart are indistinguishable and silently incomparable.

### B9 — `can_install=true` is a much weaker claim than it reads as
It means "uv could install these wheels into an empty venv". It does not mean the
repo builds, that its C extensions compile, that the benchmarks import, or that asv
runs. h5py passes on numpy alone. 462 rows pass while never having compiled at all.

### B10 — 7 of the 10 written columns are write-only
Readers outside stage 4: `env_payload` (36 sites), `python_version` (31),
`can_install` (6). **Zero** for `build_commands`, `install_commands`, `primary_root`,
`resolution_strategy`, `requires_python`, `package_name`, `package_version`.
Worse:
- `requires_python` is hardcoded `None` (`resolve_packages.py:104`) while
  `wheel_requires_python` is computed and discarded.
- `excluded_missing_on_pypi` / `excluded_exists_incompatible` / `excluded_other` are
  reset to `{}` at `orchestrator.py:645-647` immediately before being packed into
  the return value — dead code.
- `dry_run_log`, the single most useful debugging field, is returned and never
  persisted. Every failure diagnosis requires a full re-run.

### B11 — no normalisation or conflict-merging of requirement strings
apache/arrow carries six conflicting numpy constraints (`numpy`, `numpy>=1.25`,
`numpy>=2.0.0`, `numpy~=2.1.0`, `numpy~=1.23.2;...`, `numpy~=1.26.0;...`) plus
`Cython`, `cython` and `cython>=3.1` as three separate entries, and conda names
(`boost-cpp==1.68.0`, `thrift-cpp`, `libprotobuf`, `aws-sdk-cpp`, `snappy`, `lz4-c`,
`libgrpc`) alongside the nonsense token `pip+setuptools_scm`.

### B12 — 1,603 rows ship a floating environment
Rows whose `env_payload` contains no `==` at all. A container built from one installs
*today's* versions against a years-old commit. scipy is stored as py3.8 with
`['numpy', 'spline', 'setuptools', 'hypothesis', ...]` — unpinned, and `spline` and
`Image` are dead/squatted PyPI names.

### B13 — per-commit Python, per-repo image: the interpreter silently does not match  (fatal)
`get_repo_image_name(owner, repo)` returns `"{ns}/{owner}-{repo}:latest"` — the tag
carries **no Python version** (`images.py:40-44`). But `build_repo_image` bakes
`PY_VERSION` into it (`images.py:98-99`), and `_ensure_prerequisite_images` only
builds when the tag is absent (`synthesize_images.py:62-63`).

So one repo image is built once, using the `python_version` of whichever commit was
processed first, and every later PR of that repo reuses it.

**129 of 147 repos (88%) have commits that disagree on `python_version`** — dipy,
astropy, dask, arrow, devito and 124 others each span 3.8 through 3.12. Their
`env_payload` is pinned against an interpreter the container does not run.

Design consequence: the Python version is a **per-repo** property, because the
artifact it configures is per-repo. Resolving it per-commit is category error.
