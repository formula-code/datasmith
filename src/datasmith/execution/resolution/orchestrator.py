"""Main orchestration for commit analysis."""

from __future__ import annotations

import contextlib
import re
import shlex
import tempfile
from pathlib import Path
from typing import Any

from asv.config import Config

from datasmith.core.cache import CACHE_LOCATION, cache_completion
from datasmith.logging_config import get_logger

from .constants import ALLOWLIST_COMMON_PYPI
from .dependency_resolver import rfc3339, uv_build_and_read_metadata, uv_compile, uv_dry_run_install
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
from .python_manager import ensure_python_version_available, filter_python_versions_by_commit_date, run_uv

logger = get_logger(__name__)


@cache_completion(CACHE_LOCATION, table_name="commit_analysis")
def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> dict[str, Any] | None:  # noqa: C901
    """
    Analyze a commit to extract build/runtime information for benchmarking.

    Args:
        sha: Git commit SHA
        repo_name: Full repository name (e.g., "owner/repo")
        bypass_cache: Whether to bypass cache (used by cache decorator)

    Returns:
        Dictionary with commit analysis results, or None if analysis failed

    The returned dictionary contains:
        - sha: commit SHA
        - repo_name: repository name
        - package_name: PyPI package name
        - package_version: package version
        - python_version: Python version to use
        - build_command: list of build commands
        - install_command: list of install commands
        - final_dependencies: list of resolved dependencies
        - can_install: whether dependencies can be installed
        - dry_run_log: log from dry-run installation
        - primary_root: root directory of the primary package
        - resolution_strategy: strategy used for dependency resolution
        - excluded_missing_on_pypi: packages excluded because they're not on PyPI
        - excluded_exists_incompatible: packages excluded due to incompatibility
        - excluded_other: other excluded packages
    """
    commit_info: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        # Use cached base clone + worktree (fast). Fallback to reference clone if needed.
        repo, tmpfile_pth, _cleanup_checkout = prepare_repo_checkout(repo_name, sha, tmp_path)
        try:
            commit = repo.commit(sha)
            # For worktrees we're already at `sha`; for ref-clones, ensure checkout but don't fail hard.
            with contextlib.suppress(Exception):
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

            cfg_items = ASVCfgAggregate()
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
                        bc = " && ".join(bc).replace("-mpip", "-m pip")
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

            # B) Choose Python version candidates (filtered by commit date)
            # Python 3.7 and below are excluded (EOL, not available in uv)
            if (not cfg_items.pythons) or all(py < (3, 8) for py in cfg_items.pythons):
                logger.debug(f"No Python >=3.8 available in ASV config: {cfg_items.pythons}")
                return None

            # Filter Python versions based on commit date to avoid anachronisms
            authored = commit.authored_datetime
            candidate_python_versions = filter_python_versions_by_commit_date(cfg_items.pythons, authored)

            if not candidate_python_versions:
                logger.debug(
                    f"No suitable Python versions after temporal filtering from {cfg_items.pythons}. "
                    f"Note: Python 3.7 is excluded (EOL, not available in uv)."
                )
                return None

            # C) Discover packaging candidates
            candidates = discover_candidates(commit)
            if not candidates:
                return None
            analyzed: dict[str, Any] = {root: analyze_candidate_meta(c) for root, c in candidates.items()}
            primary_root = select_primary_candidate(repo_name, candidates, cfg_items.install_commands, analyzed)
            primary_meta = analyzed[primary_root]

            # D) Aggregate base requirements (unresolved, human-intent)
            base_requirements: set[str] = set()

            # From packaging metadata (pyproject/setup.cfg/requirements, env yaml hints)
            base_requirements.update(primary_meta.core_deps)
            base_requirements.add("pytest")
            base_requirements.add("setuptools")
            base_requirements.add("hypothesis")

            # Requested extras -> include their deps if declared
            requested_extras = extract_requested_extras(
                cfg_items.install_commands, cfg_items.matrix, primary_meta.extras.keys()
            )
            for ex in requested_extras:
                base_requirements.update(primary_meta.extras.get(ex, set()))

            # From ASV install_command (-r files and direct tokens)
            for install_cmd in cfg_items.install_commands:
                # Split on shell operators first
                for cmd_part in split_shell_command(install_cmd):
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
                            resolved = resolve_requirements_file(commit, rel, set())
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
                        normalized = normalize_requirement(tok)
                        base_requirements.update(normalized)

            # matrix values that look like requirements
            for vals in cfg_items.matrix.values():
                for v in vals:
                    s = str(v).strip()
                    if s and not s.startswith("-"):
                        normalized = normalize_requirement(s)
                        base_requirements.update(normalized)

            # E) Build and read wheel metadata for authoritative runtime deps
            #    (and possibly updated name/version)
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
                # optionally promote build-system requirements that are actually imported
                build_names = {re.split(r"[~<>=!; ]", breq, maxsplit=1)[0] for breq in primary_meta.build_requires}
                promote = {x for x in runtime_inferred if x in build_names}
                # there might be some members of build_names that exist in runtime_inferred
                # e.g. `conans` is the name of the package called `conan`.
                # `conan` is in build_names but not in runtime_inferred
                # in such cases, we should prefer the build name
                runtime_candidates.update(runtime_inferred)
                runtime_candidates.update(promote)
                # runtime_candidates.update(build_names)

            # Add any base requirements the repo explicitly specified (e.g., in requirements.txt)
            runtime_candidates.update(base_requirements)

            # Filter out things that are clearly not PyPI packages
            cleaned_unresolved = filter_requirements_for_pypi(
                runtime_candidates,
                project_dir=project_dir,
                own_import_name=own_import,
            )

            # If filtering nuked everything but we had some candidates, do not lie with "no deps"
            if not cleaned_unresolved and runtime_candidates:
                # Keep only the allowlisted well-known packages from the candidates as a minimal set
                cleaned_unresolved = sorted({
                    r for r in runtime_candidates if extract_pkg_name(r) in ALLOWLIST_COMMON_PYPI
                })

            # G) Try resolution with multiple Python versions, fallback on ABI errors
            cutoff = rfc3339(authored)

            # We'll try each Python version until we find one that works
            python_version = None
            resolved_dependencies: list[str] = []
            resolution_strategy = None
            can_install = False
            dry_run_log = ""

            found_flag = False

            for use_cleaned_pinned in (False, True):
                for py_tuple in candidate_python_versions:
                    if found_flag:
                        break
                    candidate_version = ".".join(map(str, py_tuple))
                    logger.debug(f"Trying Python {candidate_version}")

                    # Ensure Python version is available
                    if not ensure_python_version_available(candidate_version):
                        logger.debug(f"Python {candidate_version} not available, trying next")
                        continue

                    # Create venv for this Python version
                    candidate_venv_path = Path(tmpdir) / f"venv_{candidate_version.replace('.', '_')}"
                    venv_cp = run_uv(["venv", str(candidate_venv_path), "--python", candidate_version])

                    if venv_cp.returncode != 0:
                        logger.debug(
                            f"Failed to create venv with Python {candidate_version}: {venv_cp.stderr.decode()}"
                        )
                        continue

                    # Verify the venv has a working Python executable
                    python_exe = candidate_venv_path / "bin" / "python"
                    if not python_exe.exists():
                        python_exe = candidate_venv_path / "Scripts" / "python.exe"  # Windows

                    if not python_exe.exists():
                        logger.debug(f"Venv created but Python executable not found for version {candidate_version}")
                        continue

                    # Try resolution with this Python version
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
                                return resolved, strat  # noqa: TRY300
                            except Exception as e:
                                error_msg = str(e)

                                # Try self-healing if this is a "not found" error
                                if compile_retry_count < max_compile_retries and (
                                    "was not found in the package registry" in error_msg
                                    or "Because there are no versions of" in error_msg
                                ):
                                    failing_pkg = extract_failing_package(error_msg)
                                    if failing_pkg:
                                        # Add to blocklist
                                        if add_to_blocklist(failing_pkg):
                                            logger.info(
                                                f"Compile self-healing: Blocking '{failing_pkg}' "
                                                f"(retry {compile_retry_count + 1}/{max_compile_retries})"
                                            )

                                        # Remove from requirements and retry
                                        current_reqs, was_removed = remove_package_from_requirements(
                                            current_reqs, failing_pkg
                                        )

                                        if was_removed:
                                            compile_retry_count += 1
                                            continue

                                # If we can't heal or max retries reached, pass through
                                return list(current_reqs), f"unresolved(pass-through): {e.__class__.__name__}"

                        return list(current_reqs), "unresolved(max-retries-exceeded)"

                    if use_cleaned_pinned:
                        cleaned_unresolved = clean_pinned(cleaned_unresolved)
                    # First try strict cutoff
                    candidate_resolved, candidate_strategy = _compile_or_pass_through(
                        cleaned_unresolved, strict_cutoff=True, py_ver=candidate_version
                    )

                    # If we got exactly the same list back and it's not empty, try a relaxed attempt that drops extras syntax
                    if candidate_resolved == cleaned_unresolved and candidate_resolved:
                        relaxed = [x for x in cleaned_unresolved if not re.search(r"\[.*\]$", x)]
                        # If nothing changed, keep as-is; else try again
                        if relaxed and relaxed != cleaned_unresolved:
                            resolved2, strat2 = _compile_or_pass_through(
                                relaxed, strict_cutoff=False, py_ver=candidate_version
                            )
                            # prefer the one that produced a different (compiled) output; else keep the pass-through
                            if resolved2 != relaxed or "cutoff=none" in strat2:
                                candidate_resolved, candidate_strategy = resolved2, strat2

                    # Ensure non-empty if we had candidates
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

                    # Self-healing: If failed due to missing package, add to blocklist and retry
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

                        # Add to blocklist for future runs
                        if add_to_blocklist(failing_pkg):
                            logger.info(
                                f"Self-healing: Blocking '{failing_pkg}' and retrying "
                                f"(attempt {retry_count + 1}/{max_retries})"
                            )

                        # Remove the failing package from current dependencies
                        current_deps, was_removed = remove_package_from_requirements(current_deps, failing_pkg)

                        if not was_removed:
                            # Package not in our list, can't fix by removal
                            break

                        # Retry dry-run without the failing package
                        candidate_can_install, candidate_dry_run_log = uv_dry_run_install(
                            current_deps, python_version=candidate_version, venv_path=candidate_venv_path
                        )

                        retry_count += 1

                    # Update resolved dependencies if we removed any packages during retries
                    if retry_count > 0:
                        candidate_resolved = current_deps
                        if retry_count > 0 and candidate_can_install:
                            candidate_strategy = f"{candidate_strategy} (self-healed: {retry_count} pkgs removed)"

                    # Store the results for this attempt
                    python_version = candidate_version
                    resolved_dependencies = candidate_resolved
                    resolution_strategy = candidate_strategy
                    can_install = candidate_can_install
                    dry_run_log = candidate_dry_run_log

                    # Check if we succeeded
                    if can_install:
                        found_flag = True
                        logger.debug(f"Success with Python {candidate_version}!")
                        break

                    # Check if this is an ABI/Python version error (should try older Python)
                    log_lower = dry_run_log.lower()
                    is_abi_error = (
                        "python abi tag" in log_lower
                        or "cp3" in dry_run_log
                        or ("no wheels" in log_lower and "python" in log_lower)
                    )

                    if is_abi_error:
                        logger.debug(f"ABI incompatibility with Python {candidate_version}, trying older version")
                        continue
                    else:
                        # Some other error that won't be fixed by changing Python version
                        logger.debug(f"Non-ABI error with Python {candidate_version}, stopping attempts")
                        break

            # If we tried all versions and none worked, keep the last attempt's results
            if not python_version:
                logger.debug("No Python version succeeded")
                return None

            # I) Final identity
            pkg_name_out = primary_meta.name
            pkg_version_out = primary_meta.version

            # Classification dicts are intentionally conservative now
            excluded_missing_on_pypi: dict[str, str] = {}
            excluded_exists_incompatible: dict[str, str] = {}
            excluded_other: dict[str, str] = {}

            commit_info = {
                "sha": sha,
                "repo_name": repo_name,
                "package_name": pkg_name_out,
                "package_version": pkg_version_out,
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
