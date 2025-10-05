from __future__ import annotations

import ast
import configparser
import contextlib
import datetime as dt
import io
import os
import re
import shlex
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, cast

import tomllib as _toml
from asv.config import Config
from git import Commit, Repo

from datasmith.core.cache import CACHE_LOCATION, cache_completion
from datasmith.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class Candidate:
    root_relpath: str
    pyproject_path: Path | None = None
    setup_cfg_path: Path | None = None
    setup_py_path: Path | None = None
    req_files: list[Path] = field(default_factory=list)
    env_yamls: list[Path] = field(default_factory=list)  # environment.yml/.yaml


@dataclass
class CandidateMeta:
    name: str | None = None  # PyPI name
    version: str | None = None
    import_name: str | None = None  # importable module (when we can guess)
    requires_python: str | None = None
    core_deps: set[str] = field(default_factory=set)  # runtime
    extras: dict[str, set[str]] = field(default_factory=dict)
    build_requires: set[str] = field(default_factory=set)  # [build-system].requires


@dataclass
class _ASVCfgAggregate:
    pythons: set[tuple[int, ...]] = field(default_factory=set)
    build_commands: set[str] = field(default_factory=set)
    install_commands: set[str] = field(default_factory=set)
    matrix: dict[str, set[str]] = field(default_factory=dict)


_ASV_REGEX = re.compile(r"(^|/)\.?asv[^/]*\.jsonc?$")
_REQ_TXT_REGEX = re.compile(r"(^|/)(requirements(\.[-\w]+)?|constraints(\.[-\w]+)?)\.txt$")
_PYPROJECT = "pyproject.toml"
_SETUP_CFG = "setup.cfg"
_SETUP_PY = "setup.py"
_ENV_YML_NAMES = {"environment.yml", "environment.yaml"}
_GIT_CACHE_DIR = Path(os.getenv("GIT_CACHE_DIR", str(Path(CACHE_LOCATION).parent / "git"))).expanduser()
_GIT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

# Some common import->PyPI name fixes
_SPECIAL_IMPORT_TO_PYPI = {
    "sklearn": "scikit-learn",
    "PIL": "Pillow",
    "cv2": "opencv-python",
    "yaml": "PyYAML",
    "bs4": "beautifulsoup4",
    "Crypto": "pycryptodome",
}

# Conda-only and system packages that don't exist on PyPI
_CONDA_SYSTEM_PACKAGES = {
    "pkg-config",
    "compilers",
    "c-compiler",
    "cxx-compiler",
    "fortran-compiler",
    "gcc",
    "gxx",
    "gfortran",
    "clang",
    "clangxx",
    "make",
    "cmake",
    "autoconf",
    "automake",
    "libtool",
    "m4",
    "patch",
    "bison",
    "flex",
}


def _repo_key(repo_name: str) -> str:
    return repo_name.replace("/", "__")


def _base_clone_path(repo_name: str) -> Path:
    return _GIT_CACHE_DIR / "base_clones" / _repo_key(repo_name)


def _mirror_path(repo_name: str) -> Path:
    return _GIT_CACHE_DIR / "mirrors" / f"{_repo_key(repo_name)}.git"


def _ensure_base_clone(repo_name: str) -> Repo:
    """
    Ensure a non-bare base clone exists (partial clone). Suitable for adding worktrees.
    """
    url = f"https://github.com/{repo_name}.git"
    path = _base_clone_path(repo_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        # Non-bare partial clone keeps blobs lazy; great for fast checkouts at many SHAs.
        repo = Repo.clone_from(url, path, multi_options=["--filter=blob:none"])
    else:
        repo = Repo(path)
        # Keep it fresh
        with suppress(Exception):
            repo.remote().fetch(prune=True)
    return repo


def _ensure_mirror(repo_name: str) -> Path:
    """
    Ensure a local bare mirror exists (used for fast reference clones fallback).
    """
    url = f"https://github.com/{repo_name}.git"
    mpath = _mirror_path(repo_name)
    mpath.parent.mkdir(parents=True, exist_ok=True)
    if not mpath.exists():
        Repo.clone_from(url, mpath, mirror=True, multi_options=["--filter=blob:none"])
    else:
        with suppress(Exception):
            Repo(mpath).remote().update(prune=True)
    return mpath


def _ensure_commit_available(repo: Repo, sha: str) -> None:
    """
    Make sure the repo has the object for `sha`. If not, fetch just that SHA.
    """
    with suppress(Exception):
        repo.commit(sha)
        return
    # Try to fetch the specific object (GitHub supports this)
    repo.git.fetch("origin", sha)


def _prepare_repo_checkout(repo_name: str, sha: str, tmp_root: Path) -> tuple[Repo, Path, Callable[[], None]]:
    """
    Prefer a worktree from a cached base clone; fall back to a reference clone against a local mirror.
    Returns (repo, working_tree_path, cleanup_callback).
    """
    # 1) Worktree path (preferred)
    base_repo = None
    wt_dir = tmp_root / "wt"
    try:
        base_repo = _ensure_base_clone(repo_name)
        _ensure_commit_available(base_repo, sha)
        base_repo.git.worktree("add", "--detach", str(wt_dir), sha)
        wt_repo = Repo(wt_dir)

        def _cleanup_worktree() -> None:
            with suppress(Exception):
                base_repo.git.worktree("remove", "--force", str(wt_dir))

        return wt_repo, wt_dir, _cleanup_worktree  # noqa: TRY300
    except Exception as e:
        # Fall back to a fresh clone referencing a local mirror
        logger.debug(f"Worktree path failed; falling back to reference clone: {e}")

    # 2) Reference clone fallback
    repo_dir = tmp_root / "repo"
    mirror = _ensure_mirror(repo_name)
    url = f"https://github.com/{repo_name}.git"
    repo = Repo.clone_from(
        url,
        to_path=repo_dir,
        reference=str(mirror),  # --reference=<mirror>
        multi_options=["--filter=blob:none", "--no-tags"],
    )
    _ensure_commit_available(repo, sha)
    with suppress(Exception):
        repo.git.checkout(sha)

    def _cleanup_refclone() -> None:
        # TempDirectory will remove the files; nothing special required
        return None

    return repo, repo_dir, _cleanup_refclone


def _parse_extras_segment(token: str) -> list[str]:
    if "[" not in token or not token.endswith("]"):
        return []
    segment = token[token.rfind("[") + 1 : -1]
    if not segment:
        return []
    return [part.strip() for part in segment.split(",") if part.strip()]


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _rfc3339(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _base_tmp_for_commit(commit: Commit) -> Path:
    worktree = commit.repo.working_tree_dir
    if worktree is None:
        raise ValueError("Commit repository has no working tree directory")
    repo_root = Path(worktree)
    return repo_root.parent


def _materialize_blobs(
    commit: Commit,
    predicate: Callable[[str], bool],
    out_dirname: str,
) -> dict[str, Path]:
    """
    Copy matching blobs from <commit> into a temp folder under the clone tempdir,
    preserving relative paths. Returns a mapping {repo_relpath -> local Path}.
    """
    base = _base_tmp_for_commit(commit) / out_dirname
    base.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    for raw_item in commit.tree.traverse():
        item = cast(Any, raw_item)
        if getattr(item, "type", None) != "blob":
            continue
        relpath = cast(str, getattr(item, "path", ""))
        if predicate(relpath):
            dst = base / relpath
            dst.parent.mkdir(parents=True, exist_ok=True)
            data_stream = getattr(item, "data_stream", None)
            if data_stream is None:
                continue
            with io.BytesIO(data_stream.read()) as src, open(dst, "wb") as f:
                f.write(src.read())
            out[relpath] = dst
    return out


def _read_blob_text(commit: Commit, relpath: str, default: str | None = None) -> str | None:
    try:
        blob = cast(Any, commit.tree / relpath)
        if getattr(blob, "type", None) != "blob":
            return default
        data_stream = getattr(blob, "data_stream", None)
        if data_stream is None:
            return default
        raw_bytes = data_stream.read()
        if not isinstance(raw_bytes, (bytes, bytearray)):
            return default
        return bytes(raw_bytes).decode("utf-8", errors="replace")
    except Exception:
        return default


def _parse_requirements_txt(path: Path) -> set[str]:
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line)
    return out


def _parse_pyproject(path: Path) -> CandidateMeta:
    raw = _toml.loads(path.read_text(encoding="utf-8", errors="replace"))
    data = cast(dict[str, Any], raw)
    meta = CandidateMeta()
    proj = data.get("project") or {}
    if proj:
        meta.name = proj.get("name") or meta.name
        v = proj.get("version")
        if isinstance(v, str):
            meta.version = v
        deps = proj.get("dependencies") or []
        meta.core_deps.update([d for d in deps if isinstance(d, str)])
        opt = proj.get("optional-dependencies") or {}
        for k, arr in opt.items():
            if isinstance(arr, list):
                meta.extras[k] = {d for d in arr if isinstance(d, str)}
        rp = proj.get("requires-python")
        if isinstance(rp, str):
            meta.requires_python = rp

    # build-system (for conditional promotion based on import scan)
    bsys = data.get("build-system") or {}
    breq = bsys.get("requires") or []
    for x in breq:
        if isinstance(x, str):
            meta.build_requires.add(x)

    # importable module name heuristic: sometimes in tool.spin or package dir
    # Not strictly needed; we infer from sources later.
    return meta


def _parse_setup_cfg(path: Path) -> CandidateMeta:
    cfg = configparser.ConfigParser()
    cfg.read_string(path.read_text(encoding="utf-8", errors="replace"))
    meta = CandidateMeta()
    if cfg.has_section("metadata"):
        meta.name = cfg.get("metadata", "name", fallback=None) or meta.name
        meta.version = cfg.get("metadata", "version", fallback=None) or meta.version
    if cfg.has_section("options"):
        if cfg.has_option("options", "install_requires"):
            reqs = [
                x.strip()
                for x in cfg.get("options", "install_requires", raw=True, fallback="").splitlines()
                if x.strip()
            ]
            meta.core_deps.update(reqs)
        if cfg.has_option("options", "python_requires"):
            meta.requires_python = cfg.get("options", "python_requires", fallback=None) or meta.requires_python
    # extras
    for sec in cfg.sections():
        if sec.startswith("options.extras_require"):
            if sec == "options.extras_require":
                for k, v in cfg.items(sec):
                    arr = [x.strip() for x in v.splitlines() if x.strip()]
                    meta.extras[k] = set(arr)
            else:
                _, _, extra = sec.partition(":")
                arr = [x.strip() for x in cfg.get(sec, "__name__", fallback="").splitlines() if x.strip()]
                if arr:
                    meta.extras[extra] = set(arr)
    return meta


def _parse_conda_env_yaml(path: Path) -> set[str]:  # noqa: C901
    """
    Extremely light parser for environment.yml/.yaml. We collect:
      - pip subsection strings as-is
      - top-level dependency strings, stripped of version/channel (best effort)
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return set()
    out: set[str] = set()
    in_deps = False
    in_pip = False
    indent_pip = None
    for raw in lines:
        line = raw.rstrip()
        if not line.strip() or line.strip().startswith("#"):
            continue
        if line.startswith("dependencies:"):
            in_deps, in_pip, indent_pip = True, False, None
            continue
        if not in_deps:
            continue
        # detect pip subsection start
        if re.match(r"\s*-\s*pip\s*:\s*$", line):
            in_pip = True
            indent_pip = len(line) - len(line.lstrip(" "))
            continue
        if in_pip:
            # entries are like "  - pkg==1.2"
            if (len(line) - len(line.lstrip(" "))) > (indent_pip or 0):
                m = re.match(r"\s*-\s*([^\s].+)$", line)
                if m:
                    out.add(m.group(1).strip())
                continue
            else:
                in_pip = False  # fell out of pip block
        # top-level dependency like "- numpy=1.26" or "- python=3.12"
        m = re.match(r"\s*-\s*([A-Za-z0-9_.-]+)(?:[=<>!].*)?$", line)
        if m:
            name = m.group(1)
            if name.lower() not in {"python", "pip", "setuptools", "wheel"}:
                out.add(name)
    return out


def _discover_candidates(commit: Commit) -> dict[str, Candidate]:  # noqa: C901
    """
    Discover packaging roots and requirement/conda files across the repo at this commit.
    """

    def predicate(rel: str) -> bool:
        base = rel.rsplit("/", 1)[-1]
        if base in (_PYPROJECT, _SETUP_CFG, _SETUP_PY):
            return True
        if base in _ENV_YML_NAMES:
            return True
        return bool(_REQ_TXT_REGEX.search(rel))

    blob_map = _materialize_blobs(commit, predicate, out_dirname="_pkg_blobs")
    candidates: dict[str, Candidate] = {}

    def ensure_candidate(root_rel: str) -> Candidate:
        if root_rel not in candidates:
            candidates[root_rel] = Candidate(root_relpath=root_rel)
        return candidates[root_rel]

    for rel, local_path in blob_map.items():
        root = str(Path(rel).parent or ".")
        cand = ensure_candidate(root)
        name = local_path.name
        if name == _PYPROJECT:
            cand.pyproject_path = local_path
        elif name == _SETUP_CFG:
            cand.setup_cfg_path = local_path
        elif name == _SETUP_PY:
            cand.setup_py_path = local_path
        elif _REQ_TXT_REGEX.search(rel):
            cand.req_files.append(local_path)
        elif name in _ENV_YML_NAMES:
            cand.env_yamls.append(local_path)

    return candidates


def _analyze_candidate_meta(cand: Candidate) -> CandidateMeta:
    meta = CandidateMeta()
    # Prefer pyproject for name/version/deps and build requires
    if cand.pyproject_path and cand.pyproject_path.exists():
        meta = _parse_pyproject(cand.pyproject_path)
    # Merge setup.cfg info
    if cand.setup_cfg_path and cand.setup_cfg_path.exists():
        m2 = _parse_setup_cfg(cand.setup_cfg_path)
        meta.name = meta.name or m2.name
        meta.version = meta.version or m2.version
        meta.requires_python = meta.requires_python or m2.requires_python
        meta.core_deps.update(m2.core_deps)
        for k, v in m2.extras.items():
            meta.extras.setdefault(k, set()).update(v)
    # requirements*.txt: only obvious runtime ones (skip dev/test/docs)
    for req in cand.req_files:
        if any(token in req.name for token in ("dev", "test", "docs")):
            continue
        meta.core_deps.update(_parse_requirements_txt(req))
    # environment.yml hints
    for y in cand.env_yamls:
        meta.core_deps.update(_parse_conda_env_yaml(y))
    return meta


def _select_primary_candidate(  # noqa: C901
    repo_name: str, candidates: dict[str, Candidate], install_cmds: set[str], analyzed: dict[str, CandidateMeta]
) -> str:
    """
    Heuristic:
      1) If any install_cmd references a path ('.', './sub', 'sub'), prefer that.
      2) If only one candidate -> choose it.
      3) Prefer candidate whose meta.name matches repo_name suffix (case-insensitive, '-'/'_' normalized).
      4) Prefer candidate with a pyproject.toml.
      5) Fall back to the shortest path.
    """
    norm = lambda p: str(Path(p).as_posix().strip("./")) or "."
    paths = []
    for cmd in install_cmds:
        toks = shlex.split(cmd)
        for t in toks:
            base = t.split("[", 1)[0]
            if base.startswith((".", "/")) or "/" in base or base in (".",):
                paths.append(norm(base))
    for p in paths:
        if p in candidates:
            return p
    if len(candidates) == 1:
        return next(iter(candidates.keys()))
    repo_suffix = repo_name.split("/", 1)[-1].lower().replace("_", "-")
    by_name = []
    for root, meta in analyzed.items():
        if meta.name:
            nm = meta.name.lower().replace("_", "-")
            if nm == repo_suffix or nm == repo_suffix.replace("-", ""):
                by_name.append(root)
    if by_name:
        return by_name[0]
    for root, cand in candidates.items():
        if cand.pyproject_path:
            return root
    return sorted(candidates.keys(), key=lambda s: (len(Path(s).parts), s))[0]


def asv_finder(commit: Commit) -> list[Path]:
    mats = _materialize_blobs(commit, lambda rel: bool(_ASV_REGEX.search(rel)), out_dirname="_asv_blobs")
    return list(mats.values())


def _run_uv(
    args: list[str],
    *,
    input_text: str | None = None,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.setdefault("UV_COLOR", "never")  # avoid ANSI in output
    env.setdefault("NO_COLOR", "1")
    if extra_env:
        env.update(extra_env)
    cp = subprocess.run(  # noqa: S603
        ["uv", *args],  # noqa: S607
        input=input_text.encode("utf-8") if input_text is not None else None,
        capture_output=True,
        cwd=str(cwd) if cwd else None,
        env=env,
    )
    if check and cp.returncode != 0:
        raise RuntimeError(
            f"uv {' '.join(args)} failed with code {cp.returncode}\nSTDOUT:\n{cp.stdout.decode()}\nSTDERR:\n{cp.stderr.decode()}"
        )
    return cp


def _uv_compile(requirements: Iterable[str], *, python_version: str | None, cutoff_rfc3339: str | None) -> list[str]:
    """
    Use `uv pip compile` to resolve to pinned requirements.
    Reads from stdin (using '-') and prints the compiled file to stdout.
    """
    reqs = sorted({r.strip() for r in requirements if r and r.strip()})
    if not reqs:
        return []
    req_text = "\n".join(reqs) + "\n"
    args = ["pip", "compile", "-"]
    if python_version:
        args.extend(["--python", python_version])
    extra_env: dict[str, str] = {}
    if cutoff_rfc3339:
        extra_env["UV_EXCLUDE_NEWER"] = cutoff_rfc3339
    cp = _run_uv(args, input_text=req_text, extra_env=extra_env)
    if cp.returncode != 0:
        # Bubble up the actual error text
        raise RuntimeError(f"uv pip compile failed:\n{cp.stderr.decode() or cp.stdout.decode()}")
    out: list[str] = []
    for raw in cp.stdout.decode().splitlines():
        s = _strip_ansi(raw).strip()
        # ignore comments (including those that had ANSI colours)
        if s and not s.startswith("#"):
            out.append(s)
    return out


def _uv_dry_run_install(
    pinned: Iterable[str], *, python_version: str | None, venv_path: Path | None = None
) -> tuple[bool, str]:
    text_lines = [x for x in pinned if x.strip()]
    if not text_lines:
        # Nothing to install; treat as OK but say why.
        return True, "No runtime dependencies."
    text = "\n".join(text_lines) + "\n"
    args = ["pip", "install", "--dry-run", "-r", "-"]

    if venv_path and venv_path.exists():
        # Use the virtual environment
        args.extend(["--prefix", str(venv_path)])
    elif python_version:
        # Fallback: use --python with --system
        args.extend(["--python", python_version, "--system"])

    cp = _run_uv(args, input_text=text)
    ok = cp.returncode == 0
    log = _strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
    return ok, log


def _uv_build_and_read_metadata(project_dir: Path) -> tuple[str | None, str | None, list[str], str | None]:
    """
    Run `uv build` in the project directory, then read Name/Version/Requires-Dist/Requires-Python
    from the wheel METADATA.
    """
    cp = _run_uv(["build"], cwd=project_dir)
    if cp.returncode != 0:
        return None, None, [], None
    dist_dir = project_dir / "dist"
    if not dist_dir.exists():
        return None, None, [], None
    wheels = sorted(dist_dir.glob("*.whl"))
    if not wheels:
        return None, None, [], None
    name, version = None, None
    requires_dist: list[str] = []
    requires_python: str | None = None
    with zipfile.ZipFile(wheels[-1]) as zf:
        meta_name = next((n for n in zf.namelist() if n.endswith(".dist-info/METADATA")), None)
        if not meta_name:
            return None, None, [], None
        content = zf.read(meta_name).decode("utf-8", errors="replace")
        for line in content.splitlines():
            if line.startswith("Name: "):
                name = line.split("Name:", 1)[1].strip()
            elif line.startswith("Version: "):
                version = line.split("Version:", 1)[1].strip()
            elif line.startswith("Requires-Dist: "):
                requires_dist.append(line.split("Requires-Dist:", 1)[1].strip())
            elif line.startswith("Requires-Python: "):
                requires_python = line.split("Requires-Python:", 1)[1].strip()
    return name, version, requires_dist, requires_python


try:
    _STDLIB = set(sys.stdlib_module_names)  # Python 3.10+
except Exception:  # pragma: no cover
    _STDLIB = set()


def _top_level_imports_under(root: Path) -> set[str]:  # noqa: C901
    """
    Parse all .py files under root (excluding common non-runtime dirs) and
    return top-level imported module names (first segment).
    """
    skip_dirs = {"tests", "test", "testing", "benchmarks", "doc", "docs", ".eggs", ".tox", "build", "dist"}
    names: set[str] = set()
    for path in root.rglob("*.py"):
        rel_parts = set(path.parts)
        if skip_dirs & rel_parts:
            continue
        try:
            src = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.debug(f"Failed to read {path}: {e}")
            continue
        try:
            tree = ast.parse(src, filename=str(path))
        except Exception as e:
            logger.debug(f"Failed to parse {path}: {e}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod = (alias.name or "").split(".", 1)[0]
                    if mod:
                        names.add(mod)
            elif isinstance(node, ast.ImportFrom):
                if getattr(node, "level", 0) and node.module is None:
                    continue  # relative import
                mod = (node.module or "").split(".", 1)[0]
                if mod:
                    names.add(mod)
    return names


def _infer_runtime_from_imports(project_dir: Path, own_import_name: str | None) -> set[str]:
    """
    Convert top-level imports to likely PyPI packages, filtering stdlib and self-import.
    """
    imports = _top_level_imports_under(project_dir)
    out: set[str] = set()
    own = set()
    if own_import_name:
        own.add(own_import_name)
        own.add(own_import_name.replace("-", "_"))
        own.add(own_import_name.replace("_", "-"))
    for mod in imports:
        if mod in _STDLIB:
            continue
        if mod in own:
            continue
        pkg = _SPECIAL_IMPORT_TO_PYPI.get(mod, mod)
        out.add(pkg)
    return out


def _extras_from_install_commands(install_cmds: Iterable[str], extras_available: set[str]) -> set[str]:
    requested: set[str] = set()
    for cmd in install_cmds:
        if not cmd:
            continue
        for token in shlex.split(cmd):
            for extra in _parse_extras_segment(token):
                if extra in extras_available:
                    requested.add(extra)
    return requested


def _extras_from_matrix(matrix: Mapping[str, set[str]] | None, extras_available: set[str]) -> set[str]:
    if not matrix:
        return set()
    requested: set[str] = set()
    for values in matrix.values():
        for value in values:
            if value in extras_available:
                requested.add(value)
    return requested


def _extract_requested_extras(
    install_cmds: Iterable[str],
    matrix: Mapping[str, set[str]] | None,
    available: Iterable[str],
) -> set[str]:
    extras_available = set(available)
    requested = _extras_from_install_commands(install_cmds, extras_available)
    requested.update(_extras_from_matrix(matrix, extras_available))
    return requested


def _resolve_requirements_file(commit: Commit, rel_path: str, seen: set[str]) -> set[str]:
    """
    Recursively resolve a requirements file from a commit, handling nested -r references.
    """
    # Avoid infinite loops
    if rel_path in seen:
        return set()
    seen.add(rel_path)

    requirements: set[str] = set()
    content = _read_blob_text(commit, rel_path)
    if not content:
        return requirements

    for line in content.splitlines():
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Handle nested -r references
        tokens = line.split()
        if len(tokens) >= 2 and tokens[0] in {"-r", "--requirement"}:
            nested_path = tokens[1]
            # Resolve relative to current file's directory
            if "/" in rel_path:
                base_dir = "/".join(rel_path.split("/")[:-1])
                nested_path = f"{base_dir}/{nested_path}"
            requirements.update(_resolve_requirements_file(commit, nested_path, seen))
            continue

        # Add non-nested requirements
        requirements.add(line)

    return requirements


def _normalize_requirement(req: str, commit: Commit | None = None) -> list[str]:
    """
    Attempt to normalize/recover a requirement string into valid PyPI requirements.
    Returns a list of valid requirements (may be empty if unrecoverable).
    """
    if not req or not req.strip():
        return []
    req = req.strip()

    # Quick validation check
    if not _is_valid_pypi_requirement(req):
        return []

    # If it passes validation, return it
    return [req]


def _split_shell_command(cmd: str) -> list[str]:
    """
    Split a shell command on operators like &&, ||, ; into separate commands.
    """
    # Split on shell command separators
    parts = re.split(r"\s*(?:&&|\|\||;)\s*", cmd)
    return [p.strip() for p in parts if p.strip()]


def _filter_pypi_packages(requirements: Iterable[str]) -> list[str]:
    """
    Filter out conda-only/system packages that don't exist on PyPI.
    Also removes python version specifiers.
    """
    filtered: list[str] = []
    for req in requirements:
        if not req or not req.strip():
            continue
        req = req.strip()
        # Extract package name (before any version specifier or extras)
        pkg_name = re.split(r"[<>=!;\s\[]", req, maxsplit=1)[0].strip().lower()

        # Skip python version specifiers
        if pkg_name in {"python", "pip", "setuptools", "wheel"}:
            continue

        # Skip conda-only/system packages
        if pkg_name in _CONDA_SYSTEM_PACKAGES:
            continue

        filtered.append(req)

    return filtered


def _is_valid_pypi_requirement(req: str) -> bool:
    """
    Validate if a string looks like a valid PyPI requirement per PEP 508,
    or a URL-based requirement.
    """
    if not req or not req.strip():
        return False
    req = req.strip()

    # Reject template variables
    if "{" in req or "}" in req or "$" in req:
        return False

    # Reject shell operators
    if any(op in req for op in ["&&", "||", ";;", "|", "&"]):
        return False

    # Reject pip options (start with --)
    if req.startswith("--"):
        return False

    # Allow URLs (git+, http://, https://, file://, etc.)
    if any(req.startswith(prefix) for prefix in ["http://", "https://", "git+", "hg+", "svn+", "bzr+", "file://"]):
        return True

    # Allow local paths with extras like ".[dev]" but reject bare "."
    if req.startswith("."):
        return False

    # Extract package name (before version specifiers, extras, etc.)
    pkg_match = re.match(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)", req)
    # Valid package name found
    return bool(pkg_match)


@cache_completion(CACHE_LOCATION, table_name="commit_analysis")
def analyze_commit(sha: str, repo_name: str) -> dict[str, Any] | None:  # noqa: C901
    commit_info: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        # Use cached base clone + worktree (fast). Fallback to reference clone if needed.
        repo, tmpfile_pth, _cleanup_checkout = _prepare_repo_checkout(repo_name, sha, tmp_path)
        try:
            commit = repo.commit(sha)
            # For worktrees we're already at `sha`; for ref-clones, ensure checkout but don't fail hard.
            with suppress(Exception):
                repo.git.checkout(sha)

            # A) ASV configs
            asv_cfg_files = asv_finder(commit)
            if not asv_cfg_files:
                return None

            # Load valid ASV configs, skip malformed ones
            asv_cfgs = []
            for cfg_file in asv_cfg_files:
                with contextlib.suppress(Exception):
                    asv_cfgs.append(Config.load(cfg_file))

            if not asv_cfgs:
                return None

            cfg_items = _ASVCfgAggregate()
            for cfg in asv_cfgs:
                pythons: set[tuple[int, ...]] = set()
                for py in getattr(cfg, "pythons", []) or []:
                    with contextlib.suppress(Exception):
                        pythons.add(tuple(map(int, str(py).split("."))))
                cfg_items.pythons.update(pythons)
                bc = getattr(cfg, "build_command", None)
                ic = getattr(cfg, "install_command", None)
                if bc:
                    if isinstance(bc, (list, tuple)):
                        bc = " && ".join(bc)
                    cfg_items.build_commands.add(str(bc))
                if ic:
                    if isinstance(ic, (list, tuple)):
                        ic = " && ".join(ic)
                    cfg_items.install_commands.add(str(ic))
                mx = getattr(cfg, "matrix", None) or {}
                for k, v in mx.items():
                    values = cfg_items.matrix.setdefault(k, set())
                    if isinstance(v, (list, tuple, set)):
                        values.update(map(str, v))
                    else:
                        values.add(str(v))

            # B) Choose Python version
            if (not cfg_items.pythons) or all(py < (3, 7) for py in cfg_items.pythons):
                return None
            python_version = ".".join(map(str, max(cfg_items.pythons)))

            # Create virtual environment for dry-run testing
            venv_path: Path | None = Path(tmpdir) / "venv"
            venv_cp = _run_uv(["venv", str(venv_path), "--python", python_version])
            if venv_cp.returncode != 0:
                # If venv creation fails, we can't do dry-run testing
                venv_path = None

            # C) Discover packaging candidates
            candidates = _discover_candidates(commit)
            if not candidates:
                return None
            analyzed: dict[str, CandidateMeta] = {root: _analyze_candidate_meta(c) for root, c in candidates.items()}
            primary_root = _select_primary_candidate(repo_name, candidates, cfg_items.install_commands, analyzed)
            primary_meta = analyzed[primary_root]

            # D) Aggregate base requirements (unresolved, human-intent)
            base_requirements: set[str] = set()

            # From packaging metadata (pyproject/setup.cfg/requirements, env yaml hints)
            base_requirements.update(primary_meta.core_deps)

            # Requested extras -> include their deps if declared
            requested_extras = _extract_requested_extras(
                cfg_items.install_commands, cfg_items.matrix, primary_meta.extras.keys()
            )
            for ex in requested_extras:
                base_requirements.update(primary_meta.extras.get(ex, set()))

            # From ASV install_command (-r files and direct tokens)
            for install_cmd in cfg_items.install_commands:
                # Split on shell operators first
                for cmd_part in _split_shell_command(install_cmd):
                    try:
                        tokens = shlex.split(cmd_part)
                    except Exception:
                        logger.exception("Failed to split command %s", {cmd_part})
                        continue

                    # -r includes - use recursive resolver
                    skip_next = False
                    for i, tok in enumerate(tokens):
                        if skip_next:
                            skip_next = False
                            continue
                        if tok in {"-r", "--requirement"} and i + 1 < len(tokens):
                            rel = tokens[i + 1]
                            skip_next = True
                            resolved = _resolve_requirements_file(commit, rel, set())
                            base_requirements.update(resolved)
                            continue

                    # direct tokens (skip flags and -r args)
                    skip_next = False
                    for tok in tokens:
                        if skip_next:
                            skip_next = False
                            continue
                        if tok in {"-r", "--requirement"}:
                            skip_next = True
                            continue
                        if tok.startswith("-"):
                            continue
                        # Normalize and validate
                        normalized = _normalize_requirement(tok, commit)
                        base_requirements.update(normalized)

            # matrix values that look like requirements
            for vals in cfg_items.matrix.values():
                for v in vals:
                    s = str(v).strip()
                    if s and not s.startswith("-"):
                        normalized = _normalize_requirement(s, commit)
                        base_requirements.update(normalized)

            # E) Build and read wheel metadata for authoritative runtime deps
            #    (and possibly updated name/version)
            project_dir = tmpfile_pth / primary_root
            pkg_name, pkg_version, wheel_requires, wheel_requires_python = _uv_build_and_read_metadata(project_dir)

            if not primary_meta.name and pkg_name:
                primary_meta.name = pkg_name
            if not primary_meta.version and pkg_version:
                primary_meta.version = pkg_version
            if wheel_requires_python and not primary_meta.requires_python:
                primary_meta.requires_python = wheel_requires_python

            runtime_candidates: set[str] = set(wheel_requires)

            # F) If wheel declares nothing, infer from source imports
            if not runtime_candidates:
                # Guess importable name to exclude self-imports
                own_import = None
                # Try canonical import name from project name
                if primary_meta.name:
                    own_import = primary_meta.name.replace("-", "_")
                runtime_inferred = _infer_runtime_from_imports(project_dir, own_import_name=own_import)

                # optionally promote build-system requirements that are actually imported
                build_names = {re.split(r"[<>=!; ]", breq, maxsplit=1)[0] for breq in primary_meta.build_requires}
                promote = {x for x in runtime_inferred if x in build_names}
                runtime_candidates.update(runtime_inferred)
                runtime_candidates.update(promote)

            # Add any base requirements the repo explicitly specified (e.g., in requirements.txt)
            runtime_candidates.update(base_requirements)

            # Filter out impossible entries (do not include python pins, conda/system packages)
            cleaned_unresolved = _filter_pypi_packages(runtime_candidates)

            # G) Resolve with uv pip compile (historical cutoff at authored time)
            authored = commit.authored_datetime
            cutoff = _rfc3339(authored)
            resolution_strategy = None
            try:
                resolved_dependencies = _uv_compile(
                    cleaned_unresolved,
                    python_version=python_version,
                    cutoff_rfc3339=cutoff,
                )
                resolution_strategy = "cutoff=strict, extras=on"
            except Exception:
                # Relax: allow latest, no cutoff, and drop extras-style tokens
                relaxed = _filter_pypi_packages([x for x in cleaned_unresolved if not re.search(r"\[.*\]$", x)])
                try:
                    resolved_dependencies = _uv_compile(
                        relaxed,
                        python_version=python_version,
                        cutoff_rfc3339=None,
                    )
                    resolution_strategy = "fallback: latest=relaxed, extras=off"
                except Exception as e:
                    # Last resort: leave unresolved (but not empty if we had inputs)
                    resolved_dependencies = relaxed
                    resolution_strategy = f"unresolved: {e.__class__.__name__}"

            # H) Validate via dry-run
            can_install, dry_run_log = _uv_dry_run_install(
                resolved_dependencies, python_version=python_version, venv_path=venv_path
            )

            # I) Final identity
            pkg_name_out = primary_meta.name
            pkg_version_out = primary_meta.version

            commit_info = {
                "sha": sha,
                "repo_name": repo_name,
                "package_name": pkg_name_out,
                "package_version": pkg_version_out,
                "python_version": python_version,
                "build_command": list(cfg_items.build_commands),
                "install_command": list(cfg_items.install_commands),
                "final_dependencies": list(resolved_dependencies),
                "can_install": can_install,
                "dry_run_log": dry_run_log,
                "primary_root": primary_root,
                "resolution_strategy": resolution_strategy,
            }

            return commit_info
        finally:
            _cleanup_checkout()


# Example usage:
# commit_info = analyze_commit(sha='3263e718a6cc2d10ae4e3e4ba4d4c7ed41ee12e8', repo_name='numpy/numpy-financial')
# print(commit_info)
