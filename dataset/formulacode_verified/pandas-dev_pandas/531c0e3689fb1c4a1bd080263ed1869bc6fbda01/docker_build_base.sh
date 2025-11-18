#!/usr/bin/env bash
set -euo pipefail

REQUESTED_PY_VERSION="${PY_VERSION-}"

usage() {
    local status=${1:-1}
    echo "Usage: $0 [--py-version <major.minor>|<major.minor>]" >&2
    exit "$status"
}

while (($#)); do
    case "$1" in
        --py-version)
            if (($# < 2)); then
                echo "--py-version flag requires a value" >&2
                usage
            fi
            REQUESTED_PY_VERSION="$2"
            shift 2
            ;;
        -h|--help)
            usage 0
            ;;
        *)
            if [[ -z "$REQUESTED_PY_VERSION" ]]; then
                REQUESTED_PY_VERSION="$1"
                shift
            else
                echo "Unexpected argument: $1" >&2
                usage
            fi
            ;;
    esac
done

if [[ -n "$REQUESTED_PY_VERSION" ]]; then
    if [[ ! "$REQUESTED_PY_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
        echo "Invalid Python version '$REQUESTED_PY_VERSION'; expected format <major>.<minor> (e.g. 3.8)" >&2
        exit 1
    fi
fi

# -------- Helpers installed for all shells --------
install_profile_helpers() {
    cat >/etc/profile.d/asv_utils.sh <<'EOF'
# asv_utils.sh — login/interactive shell helpers for ASV builds
export MAMBA_ROOT_PREFIX="${MAMBA_ROOT_PREFIX:-/opt/conda}"

# Initialize micromamba for bash shells (no-op if not present)
if command -v micromamba >/dev/null 2>&1; then
    eval "$(micromamba shell hook --shell=bash)"
fi

# Find and cd into the first directory that contains an asv.*.json
cd_asv_json_dir() {
    local match
    match=$(find . -type f -name "asv.*.json" | head -n 1)
    if [[ -n "$match" ]]; then
        cd "$(dirname "$match")" || echo "Failed to change directory"
    else
        echo "No 'asv.*.json' file found in current directory or subdirectories."
        return 1
    fi
}

# Return just the conf filename (e.g., asv.conf.json)
asv_conf_name() {
    local f
    f=$(find . -type f -name "asv.*.json" | head -n 1)
    [[ -n "$f" ]] && basename "$f" || return 1
}

write_vars() {
  local key="$1" value="${2-}"          # default to empty if unset to avoid set -u crash
  mkdir -p /etc/asv_env
  # Safely append a properly quoted export line:
  printf 'export %s=%q\n' "$key" "$value" >> /etc/profile.d/asv_build_vars.sh
}

# Build performance knobs (overridable)
export MAKEFLAGS="${MAKEFLAGS:--j$(nproc)}"
export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-$(nproc)}"
export NPY_NUM_BUILD_JOBS="${NPY_NUM_BUILD_JOBS:-$(nproc)}"

# Shared uv cache to speed repeated builds
export UV_CACHE_DIR="${UV_CACHE_DIR:-/opt/uvcache}"
mkdir -p "$UV_CACHE_DIR"

# Legacy pip cache (keeping for compatibility)
export PIP_CACHE_DIR="${PIP_CACHE_DIR:-/opt/pipcache}"
mkdir -p "$PIP_CACHE_DIR"
EOF
}

# -------- Persisted build variables --------
write_build_vars() {
    local py_versions="$1"

    mkdir -p /etc/asv_env
    echo "$py_versions" > /etc/asv_env/py_versions

    # Exported for every future shell (pkg script, interactive, etc.)
    cat >>/etc/profile.d/asv_build_vars.sh <<EOF
# Auto-generated during docker_build_env.sh
export ASV_PY_VERSIONS="${py_versions}"
EOF
}

# Append install-related variables (extras/specs) so the follow-up script can use them.
append_install_vars() {
    local extras_all="$1"
    local setuppy_cmd="$2"

    mkdir -p /etc/asv_env
    printf "%s\n" "$extras_all" > /etc/asv_env/extras_all
    printf "%s\n" "$setuppy_cmd" > /etc/asv_env/setuppy_cmd

    # Export for future shells
    cat >>/etc/profile.d/asv_build_vars.sh <<EOF
export ALL_EXTRAS="${extras_all}"
export SAVED_SETUPPY_CMD="${setuppy_cmd}"
EOF
}

# -------- Install a reusable smoke-check CLI --------
install_smokecheck() {
    cat >/usr/local/bin/asv_smokecheck.py <<'PY'
#!/usr/bin/env python
import argparse, importlib, pathlib, sys
import importlib.machinery as mach

def _strip_ext_suffix(filename: str) -> str:
    # Remove the *full* extension suffix, e.g.
    # ".cpython-310-x86_64-linux-gnu.so", ".abi3.so", ".pyd", etc.
    for suf in mach.EXTENSION_SUFFIXES:
        if filename.endswith(suf):
            return filename[:-len(suf)]
    # Fallback: drop last extension and any remaining ABI tag after the first dot
    stem = pathlib.Path(filename).stem
    return stem.split(".", 1)[0]

def import_and_version(name: str):
    m = importlib.import_module(name)
    ver = getattr(m, "__version__", "unknown")
    print(f"{name} imported ok; __version__={ver}")

def probe_compiled(name: str, max_ext: int = 10):
    m = importlib.import_module(name)
    if not hasattr(m, "__path__"):
        print("No package __path__ (likely a single-module dist); skipping compiled probe.")
        return
    pkg_path = pathlib.Path(list(m.__path__)[0])
    so_like = list(pkg_path.rglob("*.so")) + list(pkg_path.rglob("*.pyd"))
    failed = []
    for ext in so_like[:max_ext]:
        rel = ext.relative_to(pkg_path)
        parts = list(rel.parts)
        parts[-1] = _strip_ext_suffix(parts[-1])  # replace filename with real module basename
        dotted = ".".join([name] + parts)
        try:
            importlib.import_module(dotted)
        except Exception as e:
            failed.append((dotted, str(e)))
    if failed:
        print("WARNING: Some compiled submodules failed to import:")
        for d, err in failed:
            print(" -", d, "->", err)
    else:
        print("Compiled submodules (if any) import ok")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--import-name", required=True)
    p.add_argument("--repo-root", default=".")
    p.add_argument("--pytest-smoke", action="store_true",
                   help="Run a quick pytest smoke: -k 'not slow' --maxfail=1")
    p.add_argument("--max-ext", type=int, default=10)
    args = p.parse_args()

    import_and_version(args.import_name.strip("\"\' "))
    probe_compiled(args.import_name, max_ext=args.max_ext)

    if args.pytest_smoke:
        import subprocess, os
        if any((pathlib.Path(args.repo_root)/p).exists() for p in ("tests", "pytest.ini", "pyproject.toml")):
            print("Running pytest smoke...")
            rc = subprocess.call([sys.executable, "-m", "pytest", "-q", "-k", "not slow", "--maxfail=1"], cwd=args.repo_root)
            if rc != 0:
                sys.exit(rc)
        else:
            print("No tests detected; skipping pytest smoke.")
    print("Smokecheck OK ✅")

if __name__ == "__main__":
    main()
PY
    chmod +x /usr/local/bin/asv_smokecheck.py
}
install_smokecheck

# -------- Install an import-name detector CLI --------
install_detect_import_name() {
    cat >/usr/local/bin/detect_import_name <<'PY'
#!/usr/bin/env python
import argparse, pathlib, re, sys, subprocess, configparser, json

# --- optional TOML loader (py3.11+: tomllib; else tomli if available) ---
try:
    import tomllib as toml
except Exception:
    try:
        import tomli as toml
    except Exception:
        toml = None

EXCEPTIONS = {
    # common dist->import mismatches
    "scikit-learn": "sklearn",
    "opencv-python": "cv2",
    "pyyaml": "yaml",
    "beautifulsoup4": "bs4",
    "pillow": "PIL",
    "mysqlclient": "MySQLdb",
    "psycopg2-binary": "psycopg2",
    "opencv-contrib-python": "cv2",
    "protobuf": "google",  # top-level package
    "apache-beam": "apache_beam",
}

# All the package names we typically query.
EXCEPTIONS.update({
    # --- core scientific stack ---
    "scikit-learn": "sklearn",
    "numpy": "numpy",
    "pandas": "pandas",
    "scipy": "scipy",
    "scikit-image": "skimage",
    "pywt": "pywt",
    "xarray": "xarray",
    "bottleneck": "bottleneck",
    "h5py": "h5py",
    "networkx": "networkx",
    "shapely": "shapely",
    "dask": "dask",
    "distributed": "distributed",
    "joblib": "joblib",
    "astropy": "astropy",
    "pymc3": "pymc3",

    # --- ML / stats / optimization / viz ---
    "optuna": "optuna",
    "arviz": "arviz",
    "pymc": "pymc",
    "kedro": "kedro",
    "modin": "modin",
    "napari": "napari",
    "deepchecks": "deepchecks",
    "voyager": "voyager",                     # spotify/voyager
    "warp": "warp",                           # NVIDIA/warp
    "newton": "newton",                       # newton-physics/newton

    # --- domain / ecosystem libs ---
    "geopandas": "geopandas",
    "cartopy": "cartopy",
    "iris": "iris",
    "anndata": "anndata",
    "scanpy": "scanpy",
    "sunpy": "sunpy",
    "pvlib-python": "pvlib",
    "PyBaMM": "pybamm",
    "momepy": "momepy",
    "satpy": "satpy",
    "pydicom": "pydicom",
    "pynetdicom": "pynetdicom",

    # --- file formats / IO / infra ---
    "asdf": "asdf",
    "arrow": "pyarrow",                       # apache/arrow
    "ArcticDB": "arcticdb",
    "arctic": "arctic",

    # --- web / frameworks / utils ---
    "django-components": "django_components",
    "h11": "h11",
    "tqdm": "tqdm",
    "rich": "rich",
    "posthog": "posthog",
    "datalad": "datalad",
    "ipyparallel": "ipyparallel",

    # --- numerical / symbolic / control ---
    "autograd": "autograd",
    "python-control": "control",
    "loopy": "loopy",
    "thermo": "thermo",
    "chempy": "chempy",
    "adaptive": "adaptive",

    # --- scientific image / signal ---
    "metric-learn": "metric_learn",

    # --- quantum / physics ---
    "Cirq": "cirq",
    "memray": "memray",
    "devito": "devito",

    # --- bio / chem / data ---
    "sourmash": "sourmash",
    "dipy": "dipy",

    # --- protocol buffers / codegen / outlines ---
    "python-betterproto": "betterproto",
    "outlines": "outlines",

    # --- DS viz / raster ---
    "datashader": "datashader",
    "xarray-spatial": "xarray_spatial",

    # --- misc ---
    "enlighten": "enlighten",
    "xorbits": "xorbits",
    "geopandas": "geopandas",
    "lmfit-py": "lmfit",
    "mdanalysis": "MDAnalysis",
    "nilearn": "nilearn",
})


EXCLUDE_DIRS = {
    ".git", ".hg", ".svn", ".tox", ".nox", ".venv", "venv",
    "build", "dist", "__pycache__", ".mypy_cache", ".pytest_cache",
    "docs", "doc", "site", "examples", "benchmarks", "tests", "testing",
}

def _norm(s: str) -> str:
    return re.sub(r"[-_.]+", "", s).lower()

def read_pyproject(root: pathlib.Path):
    cfg = {}
    p = root / "pyproject.toml"
    if toml and p.exists():
        try:
            cfg = toml.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return cfg

def read_setup_cfg(root: pathlib.Path):
    p = root / "setup.cfg"
    cp = configparser.ConfigParser()
    if p.exists():
        try:
            cp.read(p, encoding="utf-8")
        except Exception:
            pass
    return cp

def dist_name_from_config(pyproject, setup_cfg):
    # PEP 621 name
    name = (pyproject.get("project", {}) or {}).get("name")
    if not name:
        # setup.cfg [metadata] name
        if setup_cfg.has_section("metadata"):
            name = setup_cfg.get("metadata", "name", fallback=None)
    # setup.py --name as last resort
    return name

def package_roots_from_config(root, pyproject, setup_cfg):
    roots = set([root])
    # setuptools package-dir mapping
    # pyproject: [tool.setuptools.package-dir] "" = "src"
    pkgdir = ((pyproject.get("tool", {}) or {}).get("setuptools", {}) or {}).get("package-dir", {})
    if isinstance(pkgdir, dict):
        if "" in pkgdir:
            roots.add((root / pkgdir[""]).resolve())
        for _, d in pkgdir.items():
            try:
                roots.add((root / d).resolve())
            except Exception:
                pass
    # setup.cfg [options] package_dir
    if setup_cfg.has_section("options"):
        raw = setup_cfg.get("options", "package_dir", fallback=None)
        if raw:
            # can be "=\nsrc" or mapping lines
            lines = [l.strip() for l in raw.splitlines() if l.strip()]
            # accept simple "=src" or "" = "src"
            for ln in lines:
                m = re.match(r'^("?\'?)*\s*=?\s*("?\'?)*\s*(?P<path>[^#;]+)$', ln)
                if m:
                    roots.add((root / m.group("path").strip()).resolve())
    # setup.cfg [options.packages.find] where
    if setup_cfg.has_section("options.packages.find"):
        where = setup_cfg.get("options.packages.find", "where", fallback=None)
        if where:
            for w in re.split(r"[,\s]+", where):
                if w:
                    roots.add((root / w).resolve())
    return [r for r in roots if r.exists()]

def explicit_modules_from_config(pyproject, setup_cfg):
    mods = set()
    # pyproject (tool.setuptools) py-modules / packages
    st = ((pyproject.get("tool", {}) or {}).get("setuptools", {}) or {})
    for key in ("py-modules", "packages"):
        val = st.get(key)
        if isinstance(val, list):
            mods.update(val)
    # setup.cfg [options] py_modules / packages
    if setup_cfg.has_section("options"):
        for key in ("py_modules", "packages"):
            raw = setup_cfg.get("options", key, fallback=None)
            if raw:
                for tok in re.split(r"[\s,]+", raw.strip()):
                    if tok and tok != "find:":
                        mods.add(tok)
    return sorted(mods)

def read_top_level_from_egg_info(root):
    # editable installs often leave ./<name>.egg-info/top_level.txt
    for ei in root.rglob("*.egg-info"):
        tl = ei / "top_level.txt"
        if tl.exists():
            try:
                names = [l.strip() for l in tl.read_text(encoding="utf-8").splitlines() if l.strip()]
                if names:
                    return names
            except Exception:
                pass
    # also consider dist-info during local builds
    for di in root.rglob("*.dist-info"):
        tl = di / "top_level.txt"
        if tl.exists():
            try:
                names = [l.strip() for l in tl.read_text(encoding="utf-8").splitlines() if l.strip()]
                if names:
                    return names
            except Exception:
                pass
    return None

def walk_candidates(roots):
    """Return set of plausible top-level import names under candidate roots."""
    cands = set()
    for r in roots:
        for path in r.rglob("__init__.py"):
            try:
                pkg_dir = path.parent
                # skip excluded dirs anywhere in the path
                if any(part in EXCLUDE_DIRS for part in pkg_dir.parts):
                    continue
                # Construct package name relative to the nearest search root
                try:
                    rel = pkg_dir.relative_to(r)
                except Exception:
                    continue
                if not rel.parts:
                    continue
                top = rel.parts[0]
                if top.startswith("_"):
                    # usually private tooling
                    continue
                cands.add(top)
            except Exception:
                pass
        # standalone modules at top-level of roots (py_modules case)
        for mod in r.glob("*.py"):
            if mod.stem not in ("setup",):
                cands.add(mod.stem)
    return sorted(cands)

def score_candidates(cands, dist_name):
    """Assign a score preferring names that match the dist name."""
    scores = {}
    n_dist = _norm(dist_name) if dist_name else None
    prefer = None
    if dist_name and dist_name.lower() in EXCEPTIONS:
        prefer = EXCEPTIONS[dist_name.lower()]
    # also try normalized exception keys (e.g. capitalization)
    for k, v in EXCEPTIONS.items():
        if _norm(k) == _norm(dist_name or ""):
            prefer = v

    for c in cands:
        s = 0
        if prefer and _norm(c) == _norm(prefer):
            s += 100
        if n_dist and _norm(c) == n_dist:
            s += 80
        if n_dist and (_norm(c).startswith(n_dist) or n_dist.startswith(_norm(c))):
            s += 20
        # shorter, simpler names get a slight bump
        s += max(0, 10 - len(c))
        scores[c] = s
    return sorted(cands, key=lambda x: (-scores.get(x, 0), x)), scores

def detect(root: str, return_all=False):
    root = pathlib.Path(root).resolve()

    pyproject = read_pyproject(root)
    setup_cfg = read_setup_cfg(root)
    dist_name = dist_name_from_config(pyproject, setup_cfg)

    # 1) top_level.txt (best signal if present)
    top = read_top_level_from_egg_info(root)
    if top:
        if return_all:
            return top
        # If multiple, score them
        ordered, _ = score_candidates(top, dist_name or "")
        return [ordered[0]]

    # 2) explicit declarations (py_modules / packages lists)
    explicit = explicit_modules_from_config(pyproject, setup_cfg)

    # 3) find correct search roots (src layout, package_dir, etc.)
    roots = package_roots_from_config(root, pyproject, setup_cfg)

    # 4) walk code to infer candidates
    walked = walk_candidates(roots)

    # merge explicit + walked
    cands = list(dict.fromkeys(explicit + walked))  # keep order & de-dup

    # 5) fallback from dist name heuristics/exceptions if still empty
    if not cands and dist_name:
        # exception or simple normalization
        guess = EXCEPTIONS.get(dist_name.lower()) or re.sub(r"[-\.]+", "_", dist_name)
        cands = [guess]

    if not cands:
        return []

    if return_all:
        # return ordered list
        ordered, _ = score_candidates(cands, dist_name or "")
        return ordered
    else:
        ordered, _ = score_candidates(cands, dist_name or "")
        return [ordered[0]]

def main():
    ap = argparse.ArgumentParser(description="Detect the top-level Python import name for a repo.")
    ap.add_argument("--repo-root", default=".", help="Path to repository root")
    ap.add_argument("--all", action="store_true", help="Print all plausible names (JSON list)")
    args = ap.parse_args()

    names = detect(args.repo_root, return_all=args.all)
    if not names:
        sys.exit(1)
    if args.all:
        print(json.dumps(names))
    else:
        print(names[0])

if __name__ == "__main__":
    main()
PY
    chmod +x /usr/local/bin/detect_import_name
}

install_detect_import_name

install_detect_extras() {
    cat >/usr/local/bin/detect_extras <<'PY'
#!/usr/bin/env python
"""
Emit space-separated extras discovered in a repo.
Sources:
 - pyproject.toml -> [project.optional-dependencies] / [tool.poetry.extras]
 - setup.cfg -> [options.extras_require]
 - setup.py -> via `egg_info` then parse *.egg-info/{PKG-INFO,requires.txt}
"""
import argparse, pathlib, sys, subprocess, configparser, re
try:
    import tomllib as toml
except Exception:
    try:
        import tomli as toml
    except Exception:
        toml = None

def read_pyproject(root: pathlib.Path):
    p = root / "pyproject.toml"
    if toml and p.exists():
        try:
            return toml.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def read_setup_cfg(root: pathlib.Path):
    p = root / "setup.cfg"
    cp = configparser.ConfigParser()
    if p.exists():
        try:
            cp.read(p, encoding="utf-8")
        except Exception:
            pass
    return cp

def extras_from_pyproject(pyproject):
    names = set()
    proj = (pyproject.get("project", {}) or {})
    opt = proj.get("optional-dependencies", {}) or {}
    names.update(opt.keys())
    poetry = ((pyproject.get("tool", {}) or {}).get("poetry", {}) or {}).get("extras", {}) or {}
    names.update(poetry.keys())
    return names

def extras_from_setup_cfg(setup_cfg):
    names = set()
    sec = "options.extras_require"
    if setup_cfg.has_section(sec):
        names.update(setup_cfg.options(sec))
    return names

def ensure_egg_info(root: pathlib.Path):
    if (root / "setup.py").exists():
        try:
            subprocess.run([sys.executable, "setup.py", "-q", "egg_info"],
                           cwd=root, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

def extras_from_egg_info(root: pathlib.Path):
    names=set()
    for ei in root.glob("*.egg-info"):
        pkgi = ei / "PKG-INFO"
        if pkgi.exists():
            try:
                for line in pkgi.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if line.startswith("Provides-Extra:"):
                        names.add(line.split(":",1)[1].strip())
            except Exception:
                pass
        req = ei / "requires.txt"
        if req.exists():
            try:
                for line in req.read_text(encoding="utf-8", errors="ignore").splitlines():
                    m = re.match(r"^\[(.+)\]$", line.strip())
                    if m:
                        names.add(m.group(1).strip())
            except Exception:
                pass
    return names

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".")
    args = ap.parse_args()
    root = pathlib.Path(args.repo_root).resolve()

    pyproject = read_pyproject(root)
    setup_cfg = read_setup_cfg(root)

    names = set()
    names |= extras_from_pyproject(pyproject)
    names |= extras_from_setup_cfg(setup_cfg)

    if (root / "setup.py").exists():
        ensure_egg_info(root)
        names |= extras_from_egg_info(root)

    # Print space-separated (sorted) list; empty output if none
    if names:
        print(" ".join(sorted(names)))
    else:
        print("", end="")

if __name__ == "__main__":
    main()
PY
    chmod +x /usr/local/bin/detect_extras
}
install_detect_extras

# -------- Script body --------

install_profile_helpers
# shellcheck disable=SC1091
source /etc/profile.d/asv_utils.sh

# Ensure base micromamba is active for introspecting ASV config
micromamba activate base

# Minimal tools in base to parse metadata (pyproject & egg-info)
micromamba install -y -n base -c conda-forge python tomli setuptools >/dev/null

# Create the per-version envs with common build deps & ASV
if [[ -n "$REQUESTED_PY_VERSION" ]]; then
    PY_VERSIONS="$REQUESTED_PY_VERSION"
    write_build_vars "$PY_VERSIONS"
    echo "[docker_build_base] Restricting micromamba env creation to Python $PY_VERSIONS"
else
    PY_VERSIONS="3.7 3.8 3.9 3.10 3.11 3.12"
    echo "[docker_build_base] Building micromamba envs for Python versions: $PY_VERSIONS"
fi
for version in $PY_VERSIONS; do
    ENV_NAME="asv_${version}"

    if ! micromamba env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
        micromamba create -y -n "$ENV_NAME" -c conda-forge "python=$version"
    fi

    # Generic toolchain useful for many compiled projects (installed once here)
    micromamba install -y -n "$ENV_NAME" -c conda-forge \
        pip git conda mamba "libmambapy<=1.9.9" \
        cython fakeredis threadpoolctl \
        compilers meson-python cmake ninja pkg-config tomli

    # install hypothesis<7 if python<3.9
    PYTHON_LT_39=$(micromamba run -n "$ENV_NAME" python -c 'import sys; print(sys.version_info < (3,9))')
    PYTHON_BIN="/opt/conda/envs/$ENV_NAME/bin/python"
    if [ "$PYTHON_LT_39" = "True" ]; then
        # uv pip install --python "$PYTHON_BIN" "Cython<3" "setuptools<70" "wheel>=0.38" >/dev/null 2>&1 || true
        uv pip install --python "$PYTHON_BIN" "hypothesis<5" pytest versioneer >/dev/null 2>&1 || true
        # uv pip install --python "$PYTHON_BIN" --upgrade pip "setuptools>79" wheel pytest asv
        uv pip install --python "$PYTHON_BIN" --upgrade asv
    else
        uv pip install --python "$PYTHON_BIN" hypothesis pytest versioneer >/dev/null 2>&1 || true
        # uv pip install --python "$PYTHON_BIN" --upgrade pip "setuptools>79" wheel pytest
        uv pip install --python "$PYTHON_BIN" git+https://github.com/airspeed-velocity/asv
    fi

done

echo "Base environment setup complete."
