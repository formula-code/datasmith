"""Main orchestration for commit analysis."""

from __future__ import annotations

import contextlib
import re
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import Any

import json5

from datasmith.utils import get_logger

from .cache import cache_completion
from .constants import ALLOWLIST_COMMON_PYPI, CACHE_LOCATION
from .dependency_resolver import (
    rfc3339,
    uv_build_and_read_metadata,
    uv_compile,
    uv_compile_from_pyproject,
    uv_dry_run_install,
    uv_install_real,
)
from .git_utils import asv_finder, prepare_repo_checkout
from .import_analyzer import infer_runtime_from_imports
from .metadata_parser import analyze_candidate_meta, discover_candidates, select_primary_candidate
from .models import ASVCfgAggregate
from .package_filters import (
    clean_pinned,
    extract_pkg_name,
    extract_requested_extras,
    filter_requirements_for_pypi,
    normalize_requirement,
    resolve_requirements_file,
    split_shell_command,
)
from .python_manager import (
    SUPPORTED_PYTHON_VERSIONS,
    ensure_python_version_available,
    filter_python_versions_by_commit_date,
    run_uv,
)

logger = get_logger("resolution.orchestrator")


def _asv_matrix_entries(matrix: dict) -> dict[str, set[str]]:
    """Normalise an ASV `matrix` block into ``{package: {versions}}``.

    ASV accepts two shapes. The legacy shape maps a package name straight to a
    list of versions. The shape used since ASV 0.5 groups entries under ``req``,
    ``env`` and ``env_nobuild``. Only ``req`` names packages, so the grouped
    shape is read through that key and the rest is ignored.

    A ``None`` version means "do not install this package in this combination".
    It is dropped rather than converted, because ``str(None)`` would put the
    literal string ``"None"`` into the requirement set.
    """
    if not isinstance(matrix, dict):
        return {}

    # Grouped shape: any value that is itself a dict means ASV 0.5 or later.
    if any(isinstance(v, dict) for v in matrix.values()):
        matrix = matrix.get("req") or {}
        if not isinstance(matrix, dict):
            return {}

    out: dict[str, set[str]] = {}
    for pkg, raw in matrix.items():
        if not isinstance(pkg, str) or not pkg.strip():
            continue
        values = raw if isinstance(raw, list | tuple | set) else [raw]
        out[pkg.strip()] = {str(v).strip() for v in values if v is not None and str(v).strip()}
    return out


def collect_asv_cfg(cfgs: list) -> ASVCfgAggregate:
    """Aggregate every ASV config found for one commit.

    `json5.loads` returns a plain dict, so every field must be read with
    ``dict.get``. The previous code used ``getattr``, which is attribute access
    and always returned the default, making the whole read a no-op.
    """
    agg = ASVCfgAggregate()
    for cfg in cfgs:
        if not isinstance(cfg, dict):
            continue

        for py in cfg.get("pythons") or []:
            with contextlib.suppress(Exception):
                agg.pythons.add(tuple(int(part) for part in str(py).split(".")))

        bc = cfg.get("build_command")
        if bc:
            if isinstance(bc, list | tuple):
                bc = " && ".join(str(x) for x in bc)
            agg.build_commands.add(str(bc).replace("-mpip", "-m pip"))

        ic = cfg.get("install_command")
        if ic:
            if isinstance(ic, list | tuple):
                ic = " && ".join(str(x) for x in ic)
            agg.install_commands.add(str(ic))

        for pkg, versions in _asv_matrix_entries(cfg.get("matrix") or {}).items():
            agg.matrix.setdefault(pkg, set()).update(versions)

    return agg


def matrix_requirements(matrix: dict[str, set[str]]) -> set[str]:
    """Turn an ASV matrix into requirement strings.

    ASV maps a package name to the versions its benchmarks require. An empty
    version set means "require this package at any version". A non-empty set
    means "pin it".

    The keys carry the package names, so they must not be discarded. Passing a
    bare version such as ``0.29.21`` to the resolver treats it as a package
    name, which either fails resolution or is silently dropped.
    """
    out: set[str] = set()
    for pkg, versions in (matrix or {}).items():
        name = str(pkg).strip().lower()
        if not name or name.startswith("-"):
            continue
        if versions:
            for version in versions:
                value = str(version).strip()
                if value and not value.startswith("-"):
                    out.update(normalize_requirement(f"{name}=={value}"))
        else:
            out.update(normalize_requirement(name))
    return out


@cache_completion(CACHE_LOCATION, table_name="commit_analysis")
def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> dict[str, Any] | None:  # noqa: C901
    """Analyze a commit to extract build/runtime information for benchmarking.

    Returns a dictionary with resolution results, or None if analysis failed.
    """
    commit_info: dict[str, Any] | None = None

    python_version: str | None = None
    resolved_dependencies: list[str] = []
    resolution_strategy: str | None = None
    can_install: bool = False
    dry_run_log: str = ""
    excluded_missing_on_pypi: dict[str, str] = {}
    excluded_exists_incompatible: dict[str, str] = {}
    excluded_other: dict[str, str] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        repo, tmpfile_pth, _cleanup_checkout = prepare_repo_checkout(repo_name, sha, tmp_path)
        try:
            commit = repo.commit(sha)
            with contextlib.suppress(Exception):
                repo.git.checkout(sha)

            # A) ASV configs (optional — repos without ASV can still be resolved)
            asv_cfg_files = asv_finder(commit)

            asv_cfgs = []
            for cfg_file in asv_cfg_files:
                with contextlib.suppress(Exception):
                    asv_cfgs.append(json5.loads(cfg_file.read_text()))

            cfg_items = collect_asv_cfg(asv_cfgs)

            if not cfg_items.pythons:
                cfg_items.pythons.update(SUPPORTED_PYTHON_VERSIONS)

            # B) Choose Python version candidates
            if (not cfg_items.pythons) or all(py < (3, 8) for py in cfg_items.pythons):
                logger.debug("No Python >=3.8 available in ASV config: %s", cfg_items.pythons)
                return None

            authored = commit.authored_datetime
            cutoff = rfc3339(authored)
            candidate_python_versions = filter_python_versions_by_commit_date(cfg_items.pythons, authored)

            if not candidate_python_versions:
                logger.debug("No suitable Python versions after temporal filtering from %s", cfg_items.pythons)
                return None

            # C) Discover packaging candidates
            candidates = discover_candidates(commit)
            if not candidates:
                return None
            analyzed: dict[str, Any] = {root: analyze_candidate_meta(c) for root, c in candidates.items()}
            primary_root = select_primary_candidate(repo_name, candidates, cfg_items.install_commands, analyzed)
            primary_meta = analyzed[primary_root]
            primary_cand = candidates[primary_root]
            project_dir = tmpfile_pth / primary_root
            all_sources = [
                s
                for s in (primary_cand.setup_py_path, primary_cand.pyproject_path, primary_cand.setup_cfg_path)
                if s and s.exists()
            ]
            if len(all_sources):
                for source in all_sources:
                    skip_source = False
                    for py_ver in (".".join(map(str, t)) for t in candidate_python_versions[:3]):
                        if skip_source:
                            break
                        for strict_cutoff in (True, False):
                            source_name = source.name.replace(".", "_")
                            candidate_venv_path = Path(tmpdir) / f"venv_{source_name}_{py_ver.replace('.', '_')}"
                            try:
                                venv_cp = run_uv(["venv", str(candidate_venv_path), "--python", py_ver])
                                if venv_cp.returncode != 0:
                                    logger.debug(
                                        "Failed to create venv with Python %s: %s", py_ver, venv_cp.stderr.decode()
                                    )
                                    continue

                                python_exe = candidate_venv_path / "bin" / "python"
                                if not python_exe.exists():
                                    python_exe = candidate_venv_path / "Scripts" / "python.exe"

                                if not python_exe.exists():
                                    logger.debug("Venv created but Python executable not found for version %s", py_ver)
                                    shutil.rmtree(candidate_venv_path, ignore_errors=True)
                                    continue

                                resolved = uv_compile_from_pyproject(
                                    project_dir / source.name,
                                    python_version=python_exe.as_posix(),
                                    cutoff_rfc3339=cutoff if strict_cutoff else None,
                                )
                            except Exception as e:
                                shutil.rmtree(candidate_venv_path, ignore_errors=True)
                                logger.debug(
                                    "uv_compile_from_pyproject failed for Python %s with cutoff %s: %s",
                                    py_ver,
                                    "strict" if strict_cutoff else "none",
                                    e,
                                )
                                if "--no-build-isolation" in str(e):
                                    skip_source = True
                                    break
                                continue
                            strat = f"{'cutoff=strict' if strict_cutoff else 'cutoff=none'}, extras=on, python={py_ver}, source={source.name}"

                            if len(resolved) > 0:
                                candidate_can_install, candidate_dry_run_log = uv_dry_run_install(
                                    resolved, python_version=py_ver, venv_path=candidate_venv_path
                                )

                                if candidate_can_install:
                                    ok_real, real_log = uv_install_real(
                                        resolved, python_executable=python_exe.as_posix()
                                    )
                                    if ok_real:
                                        shutil.rmtree(candidate_venv_path, ignore_errors=True)
                                        commit_info = {
                                            "sha": sha,
                                            "repo_name": repo_name,
                                            "package_name": primary_meta.name,
                                            "package_version": primary_meta.version,
                                            "python_version": py_ver,
                                            "build_command": list(cfg_items.build_commands),
                                            "install_command": list(cfg_items.install_commands),
                                            "final_dependencies": list(dict.fromkeys(resolved)),
                                            "can_install": True,
                                            "dry_run_log": candidate_dry_run_log,
                                            "primary_root": primary_root,
                                            "resolution_strategy": strat,
                                            "excluded_missing_on_pypi": {},
                                            "excluded_exists_incompatible": {},
                                            "excluded_other": {},
                                        }
                                        return commit_info
                                    else:
                                        logger.debug(
                                            "Preflight install failed for Python %s (source=%s); trying next.\n%s",
                                            py_ver,
                                            source.name,
                                            real_log[-800:],
                                        )
                                        shutil.rmtree(candidate_venv_path, ignore_errors=True)
                                else:
                                    shutil.rmtree(candidate_venv_path, ignore_errors=True)

            # D) Aggregate base requirements (unresolved, human-intent)
            base_requirements: set[str] = set()
            base_requirements.update(primary_meta.core_deps)
            base_requirements.add("pytest")
            base_requirements.add("setuptools")
            base_requirements.add("hypothesis")

            requested_extras = extract_requested_extras(
                cfg_items.install_commands, cfg_items.matrix, primary_meta.extras.keys()
            )
            for ex in requested_extras:
                base_requirements.update(primary_meta.extras.get(ex, set()))

            for install_cmd in cfg_items.install_commands:
                for cmd_part in split_shell_command(install_cmd):
                    try:
                        tokens = shlex.split(cmd_part)
                    except Exception:
                        logger.exception("Failed to split command %s", cmd_part)
                        continue

                    skip_next = False
                    for i, tok in enumerate(tokens):
                        if skip_next:
                            skip_next = False
                            continue
                        if tok in {"-r", "--requirement"} and i + 1 < len(tokens):
                            rel = tokens[i + 1]
                            skip_next = True
                            requirements_from_file = resolve_requirements_file(commit, rel, set())
                            base_requirements.update(requirements_from_file)
                            continue

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
                        normalized = normalize_requirement(tok)
                        base_requirements.update(normalized)

            base_requirements.update(matrix_requirements(cfg_items.matrix))

            # E) Build and read wheel metadata
            project_dir = tmpfile_pth / primary_root
            pkg_name, pkg_version, wheel_requires, wheel_requires_python = uv_build_and_read_metadata(project_dir)

            if not primary_meta.name and pkg_name:
                primary_meta.name = pkg_name
            if not primary_meta.version and pkg_version:
                primary_meta.version = pkg_version
            if wheel_requires_python and not primary_meta.requires_python:
                primary_meta.requires_python = wheel_requires_python

            own_import = None
            if primary_meta.name:
                own_import = primary_meta.name.replace("-", "_")

            # F) Candidate runtime requirements (unresolved)
            runtime_candidates: set[str] = set(wheel_requires)
            if not runtime_candidates:
                runtime_inferred = infer_runtime_from_imports(project_dir, own_import_name=own_import)
                build_names = {re.split(r"[~<>=!; ]", breq, maxsplit=1)[0] for breq in primary_meta.build_requires}
                promote = {x for x in runtime_inferred if x in build_names}
                runtime_candidates.update(runtime_inferred)
                runtime_candidates.update(promote)

            runtime_candidates.update(base_requirements)

            cleaned_unresolved = filter_requirements_for_pypi(
                runtime_candidates,
                project_dir=project_dir,
                own_import_name=own_import,
            )

            if not cleaned_unresolved and runtime_candidates:
                cleaned_unresolved = sorted({
                    r for r in runtime_candidates if extract_pkg_name(r) in ALLOWLIST_COMMON_PYPI
                })

            found_flag = False

            for use_cleaned_pinned in (False, True):
                for py_tuple in candidate_python_versions:
                    if found_flag:
                        break
                    candidate_version = ".".join(map(str, py_tuple))
                    logger.debug("Trying Python %s", candidate_version)

                    if not ensure_python_version_available(candidate_version):
                        logger.debug("Python %s not available, trying next", candidate_version)
                        continue

                    candidate_venv_path = Path(tmpdir) / f"venv_{candidate_version.replace('.', '_')}"
                    venv_cp = run_uv(["venv", str(candidate_venv_path), "--python", candidate_version])

                    if venv_cp.returncode != 0:
                        logger.debug(
                            "Failed to create venv with Python %s: %s", candidate_version, venv_cp.stderr.decode()
                        )
                        continue

                    python_exe = candidate_venv_path / "bin" / "python"
                    if not python_exe.exists():
                        python_exe = candidate_venv_path / "Scripts" / "python.exe"

                    if not python_exe.exists():
                        logger.debug("Venv created but Python executable not found for version %s", candidate_version)
                        continue

                    def _compile_or_pass_through(
                        reqs: list[str], *, strict_cutoff: bool, py_ver: str
                    ) -> tuple[list[str], str]:
                        from .blocklist import (
                            add_to_blocklist,
                            extract_failing_package,
                            remove_package_from_requirements,
                        )

                        current_reqs = list(reqs)
                        max_compile_retries = 3
                        compile_retry_count = 0

                        while compile_retry_count <= max_compile_retries:
                            try:
                                resolved = uv_compile(
                                    current_reqs,
                                    python_version=py_ver,
                                    cutoff_rfc3339=cutoff if strict_cutoff else None,
                                )
                                strat = (
                                    f"{'cutoff=strict' if strict_cutoff else 'cutoff=none'}, extras=on, python={py_ver}"
                                )
                                if compile_retry_count > 0:
                                    strat = f"{strat} (compile-healed: {compile_retry_count} pkgs)"
                                return resolved, strat
                            except Exception as e:
                                error_msg = str(e)

                                if compile_retry_count < max_compile_retries and (
                                    "was not found in the package registry" in error_msg
                                    or "Because there are no versions of" in error_msg
                                ):
                                    failing_pkg = extract_failing_package(error_msg)
                                    if failing_pkg:
                                        if add_to_blocklist(failing_pkg):
                                            logger.info(
                                                "Compile self-healing: Blocking '%s' (retry %d/%d)",
                                                failing_pkg,
                                                compile_retry_count + 1,
                                                max_compile_retries,
                                            )
                                        current_reqs, was_removed = remove_package_from_requirements(
                                            current_reqs, failing_pkg
                                        )
                                        if was_removed:
                                            compile_retry_count += 1
                                            continue

                                return list(current_reqs), f"unresolved(pass-through): {e.__class__.__name__}"

                        return list(current_reqs), "unresolved(max-retries-exceeded)"

                    if use_cleaned_pinned:
                        cleaned_unresolved = clean_pinned(cleaned_unresolved)
                    candidate_resolved, candidate_strategy = _compile_or_pass_through(
                        cleaned_unresolved, strict_cutoff=True, py_ver=candidate_version
                    )

                    if candidate_resolved == cleaned_unresolved and candidate_resolved:
                        relaxed = [x for x in cleaned_unresolved if not re.search(r"\[.*\]$", x)]
                        if relaxed and relaxed != cleaned_unresolved:
                            resolved2, strat2 = _compile_or_pass_through(
                                relaxed, strict_cutoff=False, py_ver=candidate_version
                            )
                            if resolved2 != relaxed or "cutoff=none" in strat2:
                                candidate_resolved, candidate_strategy = resolved2, strat2

                    if not candidate_resolved and cleaned_unresolved:
                        candidate_resolved = list(cleaned_unresolved)

                    # H) Validate via dry-run with self-healing retry
                    from .blocklist import (
                        add_to_blocklist,
                        extract_failing_package,
                        remove_package_from_requirements,
                        should_retry_without_package,
                    )

                    candidate_can_install, candidate_dry_run_log = uv_dry_run_install(
                        candidate_resolved, python_version=candidate_version, venv_path=candidate_venv_path
                    )

                    max_retries = 3
                    retry_count = 0
                    current_deps = list(candidate_resolved)

                    while (
                        not candidate_can_install
                        and retry_count < max_retries
                        and should_retry_without_package(candidate_dry_run_log)
                    ):
                        failing_pkg = extract_failing_package(candidate_dry_run_log)
                        if not failing_pkg:
                            break

                        if add_to_blocklist(failing_pkg):
                            logger.info(
                                "Self-healing: Blocking '%s' and retrying (attempt %d/%d)",
                                failing_pkg,
                                retry_count + 1,
                                max_retries,
                            )

                        current_deps, was_removed = remove_package_from_requirements(current_deps, failing_pkg)
                        if not was_removed:
                            break

                        candidate_can_install, candidate_dry_run_log = uv_dry_run_install(
                            current_deps, python_version=candidate_version, venv_path=candidate_venv_path
                        )
                        retry_count += 1

                    if retry_count > 0:
                        candidate_resolved = current_deps
                        if retry_count > 0 and candidate_can_install:
                            candidate_strategy = f"{candidate_strategy} (self-healed: {retry_count} pkgs removed)"

                    python_version = candidate_version
                    resolved_dependencies = candidate_resolved
                    resolution_strategy = candidate_strategy
                    can_install = candidate_can_install
                    dry_run_log = candidate_dry_run_log

                    if can_install:
                        ok_real, real_log = uv_install_real(candidate_resolved, python_executable=python_exe.as_posix())
                        if ok_real:
                            found_flag = True
                            logger.debug("Success with Python %s (preflight install ok)!", candidate_version)
                            break
                        else:
                            logger.debug(
                                "Dry-run ok but real install failed on Python %s; trying older version.\n%s",
                                candidate_version,
                                real_log[-800:],
                            )
                            can_install = False
                            dry_run_log = real_log

                    log_lower = dry_run_log.lower()
                    is_abi_error = (
                        "python abi tag" in log_lower
                        or "cp3" in dry_run_log
                        or ("no wheels" in log_lower and "python" in log_lower)
                        or "cannot install on python version" in log_lower
                        or "only versions" in log_lower
                    )

                    if is_abi_error:
                        logger.debug("ABI incompatibility with Python %s, trying older version", candidate_version)
                        continue
                    else:
                        logger.debug("Non-ABI error with Python %s, stopping attempts", candidate_version)
                        break

            if not python_version:
                logger.debug("No Python version succeeded")
                return None

            # I) Final identity
            excluded_missing_on_pypi = {}
            excluded_exists_incompatible = {}
            excluded_other = {}

            commit_info = {
                "sha": sha,
                "repo_name": repo_name,
                "package_name": primary_meta.name,
                "package_version": primary_meta.version,
                "python_version": python_version,
                "build_command": list(cfg_items.build_commands),
                "install_command": list(cfg_items.install_commands),
                "final_dependencies": list(dict.fromkeys(resolved_dependencies)),
                "can_install": can_install,
                "dry_run_log": dry_run_log,
                "primary_root": primary_root,
                "resolution_strategy": resolution_strategy,
                "excluded_missing_on_pypi": excluded_missing_on_pypi,
                "excluded_exists_incompatible": excluded_exists_incompatible,
                "excluded_other": excluded_other,
            }

            return commit_info
        finally:
            _cleanup_checkout()
