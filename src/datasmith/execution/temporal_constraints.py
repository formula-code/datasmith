from __future__ import annotations

import calendar
import contextlib
import dataclasses
import re
import shutil
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast

import requests
import tomllib as _toml
from git import Repo
from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from datasmith.utils import CACHE_LOCATION, cache_completion

# ----------------------------- helpers & types -----------------------------


@dataclass(frozen=True)
class ResolveConfig:
    cutoff_iso: str  # ISO 8601 UTC string (Z ok)
    python_version: str  # e.g. "3.10"
    allow_prerelease: bool = False
    allow_yanked: bool = False
    timeout_s: int = 8  # HTTP timeout per call
    user_agent: str = "temporal-constraints/1.0"


@dataclass
class ResolveResult:
    repo_name: str
    sha: str
    commit_iso: str
    python_version: str  # Python version used for resolution (from ASV config or provided)
    to_install: list[str]  # seeds you pass to pip install
    constraints: list[str]  # ["name==x.y.z", ...] to write to -c file
    published_major_minor: str | None = None


def _end_of_month_iso_utc(mm_yy: str) -> str:
    """Convert 'mm-yy' -> 'YYYY-MM-lastT23:59:59Z' (assumes 20YY)."""
    mm, yy = mm_yy.split("-")
    year = 2000 + int(yy)  # adjust if your data includes 1990s, etc.
    month = int(mm)
    last = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-{last:02d}T23:59:59Z"


def _z_to_offset(s: str) -> str:
    return s[:-1] + "+00:00" if s.endswith("Z") else s


def _cutoff_dt(cutoff_iso: str) -> datetime:
    return datetime.fromisoformat(_z_to_offset(cutoff_iso)).astimezone(timezone.utc)


def _is_direct_or_vcs(spec: str) -> bool:
    s = spec.strip()
    return "://" in s or s.startswith("git+") or s.startswith("file:")


def _clean_req_line(line: str) -> str:
    line = line.strip()
    if not line or line.startswith("#"):
        return ""
    if line.startswith(("-r ", "--", "-c ")):  # ignore includes/options/constraints
        return ""
    # drop trailing markers in parsing phase; we keep them inside Requirement
    return line


# @cache_completion(CACHE_LOCATION, "env_for_python")
def _env_for_python(python_version: str) -> dict[str, str]:
    """PEP 508 environment dict for a target CPython on Linux x86_64."""
    major, minor = [*python_version.split("."), "0"][:2]
    env = dict(default_environment())
    env["python_version"] = f"{major}.{minor}"
    env["python_full_version"] = f"{major}.{minor}.0"
    env["platform_system"] = "Linux"
    env["sys_platform"] = "linux"
    env["platform_machine"] = "x86_64"
    env["implementation_name"] = "cpython"
    return {k: str(v) for k, v in env.items()}


# ----------------------------- git operations -----------------------------


def _get_repo_url(repo: str) -> str:
    """Convert repo identifier to full git URL."""
    if re.fullmatch(r"[^/]+/[^/]+", repo):
        return f"https://github.com/{repo}.git"
    return repo


def _clone_repo(repo: str) -> tuple[Path, Repo]:
    """
    Clone a repo to a temp dir and return (path, Repo object).
    `repo` may be "owner/name" (GitHub) or a full git URL.
    """
    url = _get_repo_url(repo)
    tmp = Path(tempfile.mkdtemp(prefix="temporal-pin-"))
    try:
        # Use GitPython to clone
        repo_obj = Repo.clone_from(url, tmp, depth=1, filter="blob:none")
        return tmp, repo_obj  # noqa: TRY300
    except Exception:
        # Clean up on clone failure
        with contextlib.suppress(Exception):
            shutil.rmtree(tmp, ignore_errors=True)
        raise


# SOME CONTEXT:
# how to get the name of the asv config file
# def has_asv(repo: Repo, c: Commit) -> bool:
#     return any(obj.type == "blob" and re.match(r"asv\..*\.json", obj.name) for obj in c.tree.traverse())  # type: ignore[union-attr]

# how to get the python versions from the asv config file in a SHELL SCRIPT
# # Optional benchmark grafting left intact
# if [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/dask(\.git)?$ ]]; then
#     git clone https://github.com/dask/dask-benchmarks.git /tmp/repo
#     cp -r /tmp/repo/dask/* /workspace/repo/
# elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/dask/distributed(\.git)?$ ]]; then
#     git clone https://github.com/dask/dask-benchmarks.git /tmp/repo
#     cp -r /tmp/repo/distributed/* /workspace/repo/
# elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/joblib/joblib(\.git)?$ ]]; then
#     git clone https://github.com/pierreglaser/joblib_benchmarks.git /tmp/repo
#     cp -r /tmp/repo/* /workspace/repo/
# elif [[ "$URL" =~ ^(https://)?(www\.)?github\.com/astropy/astropy(\.git)?$ ]]; then
#     git clone -b main https://github.com/astropy/astropy-benchmarks.git --single-branch
# fi

# IMPORT_NAME="$(detect_import_name || true)"
# if [[ -z "$IMPORT_NAME" ]]; then
#     echo "WARN: Could not determine import name; the pkg stage will fall back to local detection."
# fi

# cd_asv_json_dir || { echo "No 'asv.*.json' file found." >&2; exit 1; }

# CONF_NAME="$(asv_conf_name || true)"
# if [[ -z "${CONF_NAME:-}" ]]; then
#     echo "No 'asv.*.json' file found." >&2
#     exit 1
# fi


def _find_asv_config_file(repo_path: Path) -> Path | None:
    """
    Find the ASV config file (asv.*.json) in the repository.
    Returns the path to the config file or None if not found.
    """
    for config_file in repo_path.glob("asv.*.json"):
        return config_file
    return None


def _get_python_versions_from_asv_config(repo_path: Path) -> str | None:
    """
    Extract Python versions from the ASV config file and return the first one.
    Returns None if no config file is found or if no valid Python versions exist.
    """
    try:
        import asv.config
    except ImportError:
        return None

    config_file = _find_asv_config_file(repo_path)
    if config_file is None:
        return None

    try:
        cfg = asv.config.Config.load(str(config_file))
        # Filter Python versions to only include 3.7+ (same as shell script)
        valid_pythons = [v for v in cfg.pythons if tuple(map(int, v.split("."))) >= (3, 7)]

        # Return the first Python version from the list
        return valid_pythons[0] if valid_pythons else None
    except Exception:
        return None


def _checkout_commit(repo_obj: Repo, sha: str) -> tuple[str, str | None]:
    """
    Checkout a specific commit in the repo and return (commit ISO timestamp, python_version).
    The python_version is extracted from ASV config if available, otherwise None.
    """
    try:
        # Fetch the specific commit if not already available
        repo_obj.git.fetch("origin", sha, depth=1)
        # Checkout the commit
        repo_obj.git.checkout(sha)
        # Get commit timestamp
        commit = repo_obj.commit(sha)
        # Normalize to UTC with an explicit offset; do not append an extra 'Z'
        iso = commit.committed_datetime.astimezone(timezone.utc).isoformat()

        # Ensure we have working tree files we care about
        needed = [
            "requirements.txt",
            "requirements-dev.txt",
            "requirements-test.txt",
            "pyproject.toml",
            "setup.cfg",
            "setup.py",
        ]
        for p in needed:
            with contextlib.suppress(Exception):
                repo_obj.git.checkout("HEAD", "--", p)

        # Also ensure we have ASV config files
        for asv_config in Path(repo_obj.working_dir).glob("asv.*.json"):
            with contextlib.suppress(Exception):
                repo_obj.git.checkout("HEAD", "--", asv_config.name)

        # Try to get Python version from ASV config
        python_version = _get_python_versions_from_asv_config(Path(repo_obj.working_dir))

        return iso, python_version  # noqa: TRY300
    except Exception as e:
        raise RuntimeError(f"Failed to checkout commit {sha}: {e}") from e


def _clone_at(repo: str, sha: str) -> tuple[Path, str, str | None]:
    """
    Clone a repo at a commit to a temp dir. Returns (path, commit_iso, python_version).
    `repo` may be "owner/name" (GitHub) or a full git URL.
    """
    tmp, repo_obj = _clone_repo(repo)
    try:
        iso, python_version = _checkout_commit(repo_obj, sha)
        return tmp, iso, python_version  # noqa: TRY300
    except Exception:
        # Clean up on failure
        with contextlib.suppress(Exception):
            shutil.rmtree(tmp, ignore_errors=True)
        raise


# ----------------------------- dependency discovery -----------------------------


def _read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return ""


def _project_name_from_repo(root: Path) -> str | None:
    # Try pyproject
    pp = root / "pyproject.toml"
    if pp.is_file():
        try:
            data = _toml.loads(_read_text(pp))
            name = data.get("project", {}).get("name")
            if isinstance(name, str) and name.strip():
                return canonicalize_name(name.strip())
        except Exception:  # noqa: S110
            pass
    # setup.cfg
    sc = root / "setup.cfg"
    if sc.is_file():
        try:
            import configparser

            cp = configparser.ConfigParser()
            cp.read_string(_read_text(sc))
            if cp.has_option("metadata", "name"):
                return canonicalize_name(cp.get("metadata", "name").strip())
        except Exception:  # noqa: S110
            pass
    return None


def _parse_requirements_files(root: Path, add_req: Callable) -> None:
    """Parse requirements from requirements*.txt files."""
    names = [
        "requirements.txt",
        "requirements-dev.txt",
        "requirements_test.txt",
        "dev-requirements.txt",
        "test-requirements.txt",
    ]
    for fn in list(dict.fromkeys(names + [p.name for p in root.glob("*requirements*.txt")])):
        p = root / fn
        if p.is_file():
            for raw in _read_text(p).splitlines():
                add_req(raw)


def _parse_pyproject_deps(root: Path, add_req: Callable, extras: Iterable[str] | None = None) -> None:
    """Parse dependencies from pyproject.toml."""
    pp = root / "pyproject.toml"
    if not pp.is_file():
        return
    try:
        data = _toml.loads(_read_text(pp))
        proj = data.get("project", {}) or {}

        # 1) Always include base dependencies declared under PEP 621
        for item in proj.get("dependencies") or []:
            add_req(item)

        # 2) Include ALL optional-dependencies (extras) by default, or a chosen subset
        opt = proj.get("optional-dependencies") or {}
        chosen = opt.keys() if extras is None else [k for k in extras if k in opt]
        for key in chosen:
            for item in opt.get(key) or []:
                add_req(item)
    except Exception:  # noqa: S110
        pass


def _parse_setup_cfg_deps(root: Path, add_req: Callable) -> None:
    """Parse dependencies from setup.cfg."""
    sc = root / "setup.cfg"
    if not sc.is_file():
        return
    try:
        import configparser

        cp = configparser.ConfigParser()
        cp.read_string(_read_text(sc))
        for sec in cp.sections():
            if sec.startswith("options.extras_require"):
                for _, v in cp.items(sec):
                    for line in v.splitlines():
                        add_req(line)
    except Exception:  # noqa: S110
        pass


def _filter_project_under_test(seeds: list[str], root: Path, repo_name: str) -> list[str]:
    """Filter out the project under test from the seeds list."""
    put = _project_name_from_repo(root) or canonicalize_name(Path(repo_name).name)

    def _is_put(s: str) -> bool:
        try:
            r = Requirement(s)
        except Exception:
            return False
        return canonicalize_name(r.name) == put

    return [s for s in seeds if not _is_put(s)]


def _collect_seed_requirements(root: Path, repo_name: str, extras: Iterable[str] | None = None) -> list[str]:
    """
    Parse requirements from requirements*.txt, pyproject (optional-dependencies: test/dev/all),
    setup.cfg (options.extras_require.*). Keep any version specifiers; skip URLs and includes.
    """
    seeds: list[str] = []
    seen: set[tuple] = set()

    def add_req(line: str) -> None:
        line = _clean_req_line(line)
        if not line or _is_direct_or_vcs(line):
            return
        try:
            req = Requirement(line)
        except Exception:
            return
        # keep as typed (with any specifiers)
        norm = canonicalize_name(req.name)
        if (norm, str(req.specifier), str(req.marker) if req.marker else "") in seen:
            return
        seeds.append(line.strip())
        seen.add((norm, str(req.specifier), str(req.marker) if req.marker else ""))

    _parse_requirements_files(root, add_req)
    _parse_pyproject_deps(root, add_req, extras)
    _parse_setup_cfg_deps(root, add_req)

    return _filter_project_under_test(seeds, root, repo_name)


# ----------------------------- PyPI API (cached) -----------------------------


@cache_completion(CACHE_LOCATION, "pypi_project_json")
def _pypi_project_json(name: str, timeout_s: int, ua: str) -> dict[str, Any]:
    url = f"https://pypi.org/pypi/{name}/json"
    r = requests.get(url, timeout=timeout_s, headers={"User-Agent": ua})
    r.raise_for_status()
    return cast(dict[str, Any], r.json())


@cache_completion(CACHE_LOCATION, "pypi_version_json")
def _pypi_version_json(name: str, version: str, timeout_s: int, ua: str) -> dict[str, Any]:
    url = f"https://pypi.org/pypi/{name}/{version}/json"
    r = requests.get(url, timeout=timeout_s, headers={"User-Agent": ua})
    r.raise_for_status()
    return cast(dict[str, Any], r.json())


@cache_completion(CACHE_LOCATION, "releases_for")
def _releases_for(name: str, timeout_s: int, ua: str) -> dict[Version, dict]:
    data = _pypi_project_json(name, timeout_s, ua)
    rels: dict[Version, dict] = {}
    for v, files in (data.get("releases") or {}).items():
        try:
            V = Version(v)
        except InvalidVersion:
            continue
        if not files:
            continue
        dates, yanked = [], False
        for f in files:
            t = f.get("upload_time_iso_8601") or f.get("upload_time")
            if t:
                dt = datetime.fromisoformat(_z_to_offset(t)).astimezone(timezone.utc)
                dates.append(dt)
            if f.get("yanked"):
                yanked = True
        if dates:
            rels[V] = {"date": min(dates), "yanked": yanked}
    return rels


def _nearest_major_minor_for_project(project_name: str, commit_iso: str, cfg: ResolveConfig) -> str | None:
    """
    Return the MAJOR.MINOR version on PyPI whose release date is closest to the
    given commit datetime. If the project is not found on PyPI or has no dated
    releases, return None.
    """
    try:
        commit_dt = datetime.fromisoformat(_z_to_offset(commit_iso)).astimezone(timezone.utc)
    except Exception:
        return None

    try:
        releases = _releases_for(project_name, cfg.timeout_s, cfg.user_agent)
    except Exception:
        return None

    best_pair: tuple[str, float] | None = None  # (major.minor, abs seconds)
    for ver, meta in releases.items():
        # Skip pre-releases and yanked if policy disallows
        if not cfg.allow_prerelease and ver.is_prerelease:
            continue
        if not cfg.allow_yanked and meta.get("yanked", False):
            continue
        dt = meta.get("date")
        if not isinstance(dt, datetime):
            continue
        # Compute distance and track by the specific version's timestamp
        delta_s = abs((dt - commit_dt).total_seconds())
        major_minor = f"{ver.major}.{ver.minor}"
        if best_pair is None:
            best_pair = (major_minor, delta_s)
        else:
            if delta_s < best_pair[1]:
                best_pair = (major_minor, delta_s)

    if best_pair is None:
        return None
    return best_pair[0]


@cache_completion(CACHE_LOCATION, "requires_dist")
def _requires_dist(name: str, version: str, timeout_s: int, ua: str) -> tuple[str, ...]:
    try:
        data = _pypi_version_json(name, version, timeout_s, ua)
    except Exception:
        # PyPI error (e.g., 404, network, etc.) -- treat as no requirements
        return ()
    rd = data.get("info", {}).get("requires_dist") or []
    out: list[str] = []
    for item in rd:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return tuple(out)


# ----------------------------- resolver -----------------------------


def _pick_version_for(pkg_name: str, spec: SpecifierSet, cfg: ResolveConfig) -> Version | None:
    rels = _releases_for(pkg_name, cfg.timeout_s, cfg.user_agent)
    cutoff = _cutoff_dt(cfg.cutoff_iso)
    cands: list[Version] = []
    for V, meta in rels.items():
        if meta["date"] > cutoff:
            continue
        if not cfg.allow_yanked and meta["yanked"]:
            continue
        if not cfg.allow_prerelease and V.is_prerelease:
            continue
        if spec and (V not in spec):
            continue
        cands.append(V)
    if not cands:
        # relax specifiers but keep cutoff/yanked/prerelease policy
        for V, meta in rels.items():
            if (
                meta["date"] <= cutoff
                and (cfg.allow_yanked or not meta["yanked"])
                and (cfg.allow_prerelease or not V.is_prerelease)
            ):
                cands.append(V)
    return max(cands) if cands else None


def _normalize_seed_requirements(seed_reqs: Sequence[str]) -> list[Requirement]:
    """Normalize and parse seed requirements into a queue."""
    queue: list[Requirement] = []
    for s in seed_reqs:
        try:
            r = Requirement(s)
            # seed markers are evaluated at install time; for resolution we enqueue regardless,
            # but marker-gated deps will be evaluated on requires_dist expansions.
            queue.append(r)
        except Exception:  # noqa: S112
            continue
    return queue


def _expand_transitive_deps(
    name: str, version: str, cfg: ResolveConfig, env: dict[str, str], pinned: dict[str, Version]
) -> list[Requirement]:
    """Expand transitive dependencies for a resolved package."""
    new_requirements: list[Requirement] = []
    for dep in _requires_dist(name, version, cfg.timeout_s, cfg.user_agent):
        if not dep:
            continue
        try:
            r = Requirement(dep)
        except Exception:  # noqa: S112
            continue
        if r.marker and not r.marker.evaluate(env):
            continue
        nm = canonicalize_name(r.name)
        if nm in pinned:
            continue
        new_requirements.append(r)
    return new_requirements


def _resolve_closure(seed_reqs: Sequence[str], cfg: ResolveConfig) -> dict[str, Version]:
    env = _env_for_python(cfg.python_version)
    pinned: dict[str, Version] = {}
    queue = _normalize_seed_requirements(seed_reqs)

    while queue:
        req = queue.pop(0)
        name = canonicalize_name(req.name)
        if name in pinned:
            continue
        chosen = _pick_version_for(name, req.specifier or SpecifierSet(), cfg)
        if not chosen:
            # cannot pin -> skip (pip will error later if required)
            continue
        pinned[name] = chosen

        # expand transitive deps for this chosen version
        queue.extend(_expand_transitive_deps(name, str(chosen), cfg, env, pinned))

    return pinned


# ----------------------------- public entrypoint -----------------------------


def _update_to_install_with_pinned_versions(seeds: list[str], pinned: dict[str, Version]) -> list[str]:
    """
    Update the to_install list to replace >= specifiers with == specifiers using resolved versions.
    """
    updated_seeds: list[str] = []

    for seed in seeds:
        try:
            req = Requirement(seed)
            name = canonicalize_name(req.name)

            # If we have a pinned version for this package, check if we should update the specifier
            if name in pinned:
                pinned_version = pinned[name]

                # Check if the original requirement has a >= specifier
                has_gte_specifier = any(spec.operator == ">=" for spec in req.specifier)

                if has_gte_specifier:
                    # Replace with == specifier using the pinned version
                    updated_req = f"{req.name}=={pinned_version}"
                    updated_seeds.append(updated_req)
                else:
                    # Keep the original requirement as-is
                    updated_seeds.append(seed)
            else:
                # No pinned version found, keep original
                updated_seeds.append(seed)
        except Exception:
            # If parsing fails, keep the original seed
            updated_seeds.append(seed)

    return updated_seeds


# @cache_completion(CACHE_LOCATION, "build_constraints_for_repo")
def build_constraints_for_repo(
    repo_name: str,
    cutoff_mm_yy: str,
    sha: str,
    *,
    python_version: str | None = None,
    allow_prerelease: bool = False,
    allow_yanked: bool = False,
    extras: Iterable[str] | None = None,
) -> ResolveResult:
    """
    Clone `repo_name` at `sha`, read dependency seeds, and resolve a constraints
    set such that each package is the latest release published on/before the end
    of `mm-yy`. Returns `ResolveResult` with `to_install` and `constraints`.

    - repo_name: "owner/repo" (GitHub), git URL, or local path.
    - cutoff_mm_yy: e.g. "05-22" for May 2022 (assumes 20YY).
    - sha: full or short commit SHA present in remote.
    - python_version: target env, default to the running interpreter (major.minor).
    """
    cutoff_iso = _end_of_month_iso_utc(cutoff_mm_yy)
    if python_version is None:
        import sys

        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

    repo_path, commit_iso, asv_python_version = _clone_at(repo_name, sha)
    try:
        seeds = _collect_seed_requirements(repo_path, repo_name, extras)

        # Use ASV config Python version if available, otherwise use provided/default
        effective_python_version = asv_python_version or python_version

        cfg = ResolveConfig(
            cutoff_iso=cutoff_iso,
            python_version=effective_python_version,
            allow_prerelease=allow_prerelease,
            allow_yanked=allow_yanked,
        )
        pinned = _resolve_closure(seeds, cfg)
        constraints = [f"{name}=={ver}" for name, ver in sorted(pinned.items())]
        updated_to_install = _update_to_install_with_pinned_versions(seeds, pinned)
        # Determine the project name and map to nearest PyPI major.minor
        project_name = _project_name_from_repo(repo_path) or canonicalize_name(Path(repo_name).name)
        published_major_minor = _nearest_major_minor_for_project(project_name, commit_iso, cfg)
        return ResolveResult(
            repo_name=repo_name,
            sha=sha,
            commit_iso=commit_iso,
            python_version=effective_python_version,
            to_install=updated_to_install,
            constraints=constraints,
            published_major_minor=published_major_minor,
        )
    finally:
        # always clean up the temp clone
        with contextlib.suppress(Exception):
            shutil.rmtree(repo_path, ignore_errors=True)


def build_constraints_for_repo_batch(
    repo_name: str,
    cutoff_mm_yy: str,
    sha_list: list[str],
    *,
    allow_prerelease: bool = False,
    allow_yanked: bool = False,
    extras: Iterable[str] | None = None,
) -> list[ResolveResult]:
    """
    Clone `repo_name` once and process multiple SHAs, read dependency seeds, and resolve
    constraints sets such that each package is the latest release published on/before the end
    of `mm-yy`. Returns a list of `ResolveResult` objects.

    - repo_name: "owner/repo" (GitHub), git URL, or local path.
    - cutoff_mm_yy: e.g. "05-22" for May 2022 (assumes 20YY).
    - sha_list: list of full or short commit SHAs present in remote.
    - python_version: target env, default to the running interpreter (major.minor).
    """
    cutoff_iso = _end_of_month_iso_utc(cutoff_mm_yy)
    repo_path, repo_obj = _clone_repo(repo_name)
    results: list[ResolveResult] = []

    try:
        for sha in sha_list:
            try:
                # Checkout the specific commit
                commit_iso, asv_python_version = _checkout_commit(repo_obj, sha)

                # Collect requirements for this commit
                seeds = _collect_seed_requirements(repo_path, repo_name, extras)

                # Use ASV config Python version if available, otherwise use default
                import sys

                effective_python_version = asv_python_version or f"{sys.version_info.major}.{sys.version_info.minor}"

                cfg = ResolveConfig(
                    cutoff_iso=cutoff_iso,
                    python_version=effective_python_version,
                    allow_prerelease=allow_prerelease,
                    allow_yanked=allow_yanked,
                )

                # Resolve dependencies
                pinned = _resolve_closure(seeds, cfg)
                constraints = [f"{name}=={ver}" for name, ver in sorted(pinned.items())]
                updated_to_install = _update_to_install_with_pinned_versions(seeds, pinned)
                # Determine the project name and map to nearest PyPI major.minor
                project_name = _project_name_from_repo(repo_path) or canonicalize_name(Path(repo_name).name)
                published_major_minor = _nearest_major_minor_for_project(project_name, commit_iso, cfg)

                results.append(
                    ResolveResult(
                        repo_name=repo_name,
                        sha=sha,
                        commit_iso=commit_iso,
                        python_version=effective_python_version,
                        to_install=updated_to_install,
                        constraints=constraints,
                        published_major_minor=published_major_minor,
                    )
                )
            except Exception as e:
                # Log error but continue with other SHAs
                print(f"Warning: Failed to process SHA {sha}: {e}")
                continue

    finally:
        # always clean up the temp clone
        with contextlib.suppress(Exception):
            shutil.rmtree(repo_path, ignore_errors=True)

    return results


# ----------------------------- convenience for pandas -----------------------------


def apply_over_group(row: Any) -> dict[str, object]:
    """
    Helper for use in a pandas .apply across grouped rows containing
    columns: repo_name, mm-yy, sha.
    """
    res = build_constraints_for_repo(
        repo_name=row["repo_name"],
        cutoff_mm_yy=row["mm-yy"],
        sha=row["sha"],
    )
    return dataclasses.asdict(res)


def apply_over_group_batch(group_df: Any) -> list[dict[str, object]]:
    """
    Helper for use with pandas groupby operations. Processes all SHAs for a group
    (same repo_name and mm-yy) in a single batch operation.

    Expected columns: repo_name, mm-yy, sha
    """
    if group_df.empty:
        return []

    repo_name = group_df["repo_name"]
    cutoff_mm_yy = group_df["mm-yy"]
    sha_list = group_df["sha"]

    results = build_constraints_for_repo_batch(
        repo_name=repo_name,
        cutoff_mm_yy=cutoff_mm_yy,
        sha_list=sha_list,
    )

    return [dataclasses.asdict(res) for res in results]
