"""Parsing metadata from packaging files (pyproject.toml, setup.cfg, requirements.txt, etc.)."""

from __future__ import annotations

import configparser
import re
import shlex
from pathlib import Path
from typing import Any, cast

try:
    import tomllib as _toml
except ImportError:
    import tomli as _toml  # type: ignore[no-redef]

from git import Commit

from .constants import ENV_YML_NAMES, PYPROJECT, REQ_TXT_REGEX, SETUP_CFG, SETUP_PY
from .git_utils import materialize_blobs
from .models import Candidate, CandidateMeta


def parse_requirements_txt(path: Path) -> set[str]:
    """
    Parse a requirements.txt file and return a set of requirement strings.

    Args:
        path: Path to requirements.txt file

    Returns:
        Set of requirement strings
    """
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line)
    return out


def parse_pyproject(path: Path) -> CandidateMeta:
    """
    Parse a pyproject.toml file and extract metadata.

    Args:
        path: Path to pyproject.toml file

    Returns:
        CandidateMeta with extracted information
    """
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

    return meta


def parse_setup_cfg(path: Path) -> CandidateMeta:
    """
    Parse a setup.cfg file and extract metadata.

    Args:
        path: Path to setup.cfg file

    Returns:
        CandidateMeta with extracted information
    """
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


def parse_conda_env_yaml(path: Path) -> set[str]:  # noqa: C901
    """
    Extremely light parser for environment.yml/.yaml. We collect:
      - pip subsection strings as-is
      - top-level dependency strings, stripped of version/channel (best effort)

    Args:
        path: Path to environment.yml or environment.yaml file

    Returns:
        Set of dependency strings
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


def discover_candidates(commit: Commit) -> dict[str, Candidate]:  # noqa: C901
    """
    Discover packaging roots and requirement/conda files across the repo at this commit.

    Args:
        commit: Git commit object to search

    Returns:
        Dictionary mapping root relative paths to Candidate objects
    """

    def predicate(rel: str) -> bool:
        base = rel.rsplit("/", 1)[-1]
        if base in (PYPROJECT, SETUP_CFG, SETUP_PY):
            return True
        if base in ENV_YML_NAMES:
            return True
        return bool(REQ_TXT_REGEX.search(rel))

    blob_map = materialize_blobs(commit, predicate, out_dirname="_pkg_blobs")
    candidates: dict[str, Candidate] = {}

    def ensure_candidate(root_rel: str) -> Candidate:
        if root_rel not in candidates:
            candidates[root_rel] = Candidate(root_relpath=root_rel)
        return candidates[root_rel]

    for rel, local_path in blob_map.items():
        root = str(Path(rel).parent or ".")
        cand = ensure_candidate(root)
        name = local_path.name
        if name == PYPROJECT:
            cand.pyproject_path = local_path
        elif name == SETUP_CFG:
            cand.setup_cfg_path = local_path
        elif name == SETUP_PY:
            cand.setup_py_path = local_path
        elif REQ_TXT_REGEX.search(rel):
            cand.req_files.append(local_path)
        elif name in ENV_YML_NAMES:
            cand.env_yamls.append(local_path)

    return candidates


def analyze_candidate_meta(cand: Candidate) -> CandidateMeta:
    """
    Analyze a candidate to extract metadata from its packaging files.

    Args:
        cand: Candidate object with paths to packaging files

    Returns:
        CandidateMeta with combined metadata from all sources
    """
    meta = CandidateMeta()
    # Prefer pyproject for name/version/deps and build requires
    if cand.pyproject_path and cand.pyproject_path.exists():
        meta = parse_pyproject(cand.pyproject_path)
    # Merge setup.cfg info
    if cand.setup_cfg_path and cand.setup_cfg_path.exists():
        m2 = parse_setup_cfg(cand.setup_cfg_path)
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
        meta.core_deps.update(parse_requirements_txt(req))
    # environment.yml hints
    for y in cand.env_yamls:
        meta.core_deps.update(parse_conda_env_yaml(y))
    return meta


def select_primary_candidate(  # noqa: C901
    repo_name: str, candidates: dict[str, Candidate], install_cmds: set[str], analyzed: dict[str, CandidateMeta]
) -> str:
    """
    Heuristic to select the primary package root from multiple candidates.

    Args:
        repo_name: Full repository name (e.g., "owner/repo")
        candidates: Dictionary of candidate roots
        install_cmds: Set of install commands from ASV config
        analyzed: Dictionary mapping roots to their analyzed metadata

    Returns:
        Root path of the primary candidate

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
