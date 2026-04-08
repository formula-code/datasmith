"""Parsing metadata from packaging files (pyproject.toml, setup.cfg, requirements.txt, etc.)."""

from __future__ import annotations

import configparser
import re
import shlex
from pathlib import Path
from typing import Any, cast

try:
    import tomllib as _toml  # type: ignore[import-not-found,unused-ignore]
except ImportError:
    import tomli as _toml  # type: ignore[no-redef,import-not-found,unused-ignore]

from git import Commit

from .constants import ENV_YML_NAMES, PYPROJECT, REQ_TXT_REGEX, SETUP_CFG, SETUP_PY
from .git_utils import materialize_blobs
from .models import Candidate, CandidateMeta


def parse_requirements_txt(path: Path) -> set[str]:
    """Parse a requirements.txt file and return a set of requirement strings."""
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.add(line)
    return out


def parse_pyproject(path: Path) -> CandidateMeta:
    """Parse a pyproject.toml file and extract metadata."""
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

    bsys = data.get("build-system") or {}
    breq = bsys.get("requires") or []
    for x in breq:
        if isinstance(x, str):
            meta.build_requires.add(x)

    return meta


def parse_setup_cfg(path: Path) -> CandidateMeta:
    """Parse a setup.cfg file and extract metadata."""
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


def parse_setup_py(path: Path) -> CandidateMeta:  # noqa: C901
    """Heuristic, safe parser for setup.py (no code execution)."""
    import ast

    meta = CandidateMeta()

    try:
        src = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return meta

    try:
        tree = ast.parse(src, filename=str(path))
    except Exception:
        return meta

    env: dict[str, Any] = {}

    def safe_eval(node: ast.AST, depth: int = 0) -> Any:  # noqa: C901
        if depth > 100:
            raise ValueError("Too deep")

        if isinstance(node, ast.Constant):
            return node.value

        if hasattr(ast, "Str") and isinstance(node, ast.Str):
            return node.s
        if hasattr(ast, "Num") and isinstance(node, ast.Num):
            return node.n
        if hasattr(ast, "NameConstant") and isinstance(node, ast.NameConstant):
            return node.value

        if isinstance(node, ast.Name):
            if node.id in env:
                return env[node.id]
            raise ValueError(f"Unknown name {node.id}")

        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            elts = []
            for e in node.elts:
                elts.append(safe_eval(e, depth + 1))
            return list(elts)

        if isinstance(node, ast.Dict):
            out: dict[Any, Any] = {}
            for k, v in zip(node.keys, node.values):
                if k is None:
                    raise ValueError("Dict unpacking not allowed here")
                key = safe_eval(k, depth + 1)
                val = safe_eval(v, depth + 1)
                out[key] = val
            return out

        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            v = safe_eval(node.operand, depth + 1)
            if isinstance(v, (int, float)) and isinstance(node.op, ast.USub):
                return -v
            if isinstance(v, (int, float)) and isinstance(node.op, ast.UAdd):
                return +v
            raise ValueError("Unsupported unary op")

        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = safe_eval(node.left, depth + 1)
            right = safe_eval(node.right, depth + 1)
            if isinstance(left, str) and isinstance(right, str):
                return left + right
            if isinstance(left, list) and isinstance(right, list):
                return left + right
            if isinstance(left, tuple) and isinstance(right, tuple):
                return list(left + right)
            raise ValueError("Unsupported addition types")

        if isinstance(node, ast.Call):
            func = node.func
            func_name = None
            if isinstance(func, ast.Name):
                func_name = func.id
            elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                func_name = f"{func.value.id}.{func.attr}"
            if func_name in {"list", "tuple"} and not node.keywords and len(node.args) == 1:
                seq = safe_eval(node.args[0], depth + 1)
                if isinstance(seq, list):
                    return list(seq)
            if func_name == "dict" and not node.keywords:  # noqa: SIM102
                if len(node.args) == 1:
                    seq = safe_eval(node.args[0], depth + 1)
                    out2: dict[Any, Any] = {}
                    if isinstance(seq, list):
                        for item in seq:
                            if isinstance(item, (list, tuple)) and len(item) == 2:
                                out2[item[0]] = item[1]
                            else:
                                raise ValueError("Unsupported dict constructor form")
                        return out2
            raise ValueError("Calls are not safely evaluable")

        if isinstance(node, ast.JoinedStr):
            s: list[str] = []
            for v in node.values:
                if isinstance(v, ast.Str):
                    s.append(str(v.s))
                elif isinstance(v, ast.Constant) and isinstance(v.value, str):
                    s.append(v.value)
                else:
                    raise TypeError("Non-literal in f-string")
            return "".join(s)

        raise ValueError("Unsupported AST node")

    for node in tree.body:
        try:
            if isinstance(node, ast.Assign):
                val = safe_eval(node.value)
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        env[target.id] = val
                    elif isinstance(target, (ast.Tuple, ast.List)):  # noqa: SIM102
                        if isinstance(val, (list, tuple)) and len(target.elts) == len(val):
                            for elt, v in zip(target.elts, val):
                                if isinstance(elt, ast.Name):
                                    env[elt.id] = v
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                env[node.target.id] = safe_eval(node.value)
        except Exception:  # noqa: S112
            continue

    def is_setup_call(call: ast.Call) -> bool:
        f = call.func
        if isinstance(f, ast.Name):
            return f.id == "setup"
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
            return f.attr == "setup"
        return False

    def merge_kwargs_from_starstar(val: Any, into: dict[str, Any]) -> None:
        if isinstance(val, str) and val in env and isinstance(env[val], dict):
            for k, v in env[val].items():
                into.setdefault(k, v)
        elif isinstance(val, dict):
            for k, v in val.items():
                into.setdefault(k, v)

    setup_kwargs: dict[str, Any] = {}
    for ast_node in ast.walk(tree):
        if isinstance(ast_node, ast.Call) and is_setup_call(ast_node):
            kwargs: dict[str, Any] = {}
            for kw in ast_node.keywords:
                try:
                    if kw.arg is None:
                        v = safe_eval(kw.value)
                        merge_kwargs_from_starstar(v, kwargs)
                    else:
                        kwargs[kw.arg] = safe_eval(kw.value)
                except Exception:  # noqa: S112
                    continue
            setup_kwargs = kwargs

    if setup_kwargs:
        name = setup_kwargs.get("name")
        if isinstance(name, str):
            meta.name = name
        version = setup_kwargs.get("version")
        if isinstance(version, str):
            meta.version = version
        pyreq = setup_kwargs.get("python_requires")
        if isinstance(pyreq, str):
            meta.requires_python = pyreq
        install_requires = setup_kwargs.get("install_requires")
        if isinstance(install_requires, (list, tuple)):
            meta.core_deps.update([x for x in install_requires if isinstance(x, str)])
        extras_require = setup_kwargs.get("extras_require")
        if isinstance(extras_require, dict):
            for k, v in extras_require.items():
                if isinstance(k, str) and isinstance(v, (list, tuple)):
                    meta.extras[k] = {x for x in v if isinstance(x, str)}
        setup_requires = setup_kwargs.get("setup_requires")
        if isinstance(setup_requires, (list, tuple)):
            meta.build_requires.update([x for x in setup_requires if isinstance(x, str)])

    return meta


def parse_conda_env_yaml(path: Path) -> set[str]:  # noqa: C901
    """Light parser for environment.yml/.yaml."""
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
        if re.match(r"\s*-\s*pip\s*:\s*$", line):
            in_pip = True
            indent_pip = len(line) - len(line.lstrip(" "))
            continue
        if in_pip:
            if (len(line) - len(line.lstrip(" "))) > (indent_pip or 0):
                m = re.match(r"\s*-\s*([^\s].+)$", line)
                if m:
                    out.add(m.group(1).strip())
                continue
            else:
                in_pip = False
        m = re.match(r"\s*-\s*([A-Za-z0-9_.-]+)(?:[=<>!].*)?$", line)
        if m:
            name = m.group(1)
            if name.lower() not in {"python", "pip", "setuptools", "wheel"}:
                out.add(name)
    return out


def discover_candidates(commit: Commit) -> dict[str, Candidate]:  # noqa: C901
    """Discover packaging roots and requirement/conda files across the repo at this commit."""

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
    """Analyze a candidate to extract metadata from its packaging files."""
    meta = CandidateMeta()
    if cand.pyproject_path and cand.pyproject_path.exists():
        meta = parse_pyproject(cand.pyproject_path)
    if cand.setup_cfg_path and cand.setup_cfg_path.exists():
        m2 = parse_setup_cfg(cand.setup_cfg_path)
        meta.name = meta.name or m2.name
        meta.version = meta.version or m2.version
        meta.requires_python = meta.requires_python or m2.requires_python
        meta.core_deps.update(m2.core_deps)
        for k, v in m2.extras.items():
            meta.extras.setdefault(k, set()).update(v)
    if cand.setup_py_path and cand.setup_py_path.exists():
        m3 = parse_setup_py(cand.setup_py_path)
        meta.name = meta.name or m3.name
        meta.version = meta.version or m3.version
        meta.requires_python = meta.requires_python or m3.requires_python
        meta.core_deps.update(m3.core_deps)
        for k, v in m3.extras.items():
            meta.extras.setdefault(k, set()).update(v)
        meta.build_requires.update(m3.build_requires)
    for req in cand.req_files:
        meta.core_deps.update(parse_requirements_txt(req))
    for y in cand.env_yamls:
        meta.core_deps.update(parse_conda_env_yaml(y))
    return meta


def select_primary_candidate(  # noqa: C901
    repo_name: str, candidates: dict[str, Candidate], install_cmds: set[str], analyzed: dict[str, CandidateMeta]
) -> str:
    """Heuristic to select the primary package root from multiple candidates."""
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
