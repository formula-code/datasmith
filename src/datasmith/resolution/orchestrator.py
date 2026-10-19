"""Compose the resolution units for one commit.

The flow is one path of small units, each testable alone: ``discover`` finds the
packaging roots, ``declare`` reads what the project states it needs,
``interpreter`` picks the Python, ``pin`` compiles it once, ``probe`` dry-runs the
result (it orders the queue, it does not filter), and this module emits the row.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import json5

from datasmith.utils import get_logger
from git import Commit

from .cache import cache_completion
from .constants import CACHE_LOCATION
from .declare import declare
from .git_utils import asv_finder, prepare_repo_checkout
from .interpreter import select_interpreter, trove_versions_from_classifiers
from .metadata_parser import analyze_candidate_meta, discover_candidates, select_primary_candidate
from .models import ASVCfgAggregate, CandidateMeta
from .pin import pin
from .probe import probe

logger = get_logger("resolution.orchestrator")

__all__ = ["RESOLVER_VERSION", "ResolutionResult", "analyze_commit", "collect_asv_cfg"]

#: Stamped on every row this resolver writes; ``legacy`` rows came from an older resolver.
RESOLVER_VERSION = "2026.08.23"

#: Keys of an ASV ``matrix`` that name a section rather than a package.  Modern
#: asv nests requirements under ``req`` and environment variables under ``env`` /
#: ``env_nobuild``; older configs put the packages at the top level.
_ASV_MATRIX_SECTIONS = ("req", "env", "env_nobuild")

#: ``environment_type`` values whose matrix names PyPI distributions.  Everything
#: else -- ``conda``, ``mamba``, ``rattler``, and site plugins such as
#: ``oggm_conda`` -- names conda packages, which is a different namespace.
DATASMITH_ASV_PIP_ENV_TYPES: frozenset[str] = frozenset(
    part.strip().lower()
    for part in os.environ.get("DATASMITH_ASV_PIP_ENV_TYPES", "virtualenv,venv,existing").split(",")
    if part.strip()
)

#: asv's escape hatch inside a non-pip matrix: ``pip+foo`` states that ``foo`` is
#: installed with pip, so it is a PyPI name whatever the environment type is.
_ASV_PIP_PREFIX = "pip+"


@dataclass(frozen=True)
class ResolutionResult:
    """One commit's resolved environment, and how it was reached."""

    owner_repo: str
    sha: str
    package_name: str | None
    package_version: str | None
    primary_root: str
    requires_python: str | None
    python_version: str
    interpreter_source: str
    env_payload: list[str] = field(default_factory=list)
    probe_status: str = "empty"
    probe_log: str = ""
    cutoff_used: str | None = None
    cutoff_relaxed: bool = False
    dropped_requirements: list[dict[str, str]] = field(default_factory=list)
    resolver_version: str = RESOLVER_VERSION


def _matrix_entries(matrix: object, environment_type: object = None) -> dict[str, list[str]]:
    """Read the PyPI packages an ASV ``matrix`` names, in whichever shape it is written.

    asv >= 0.6 nests them under ``req``; older configs name them at the top level.
    Under a conda-family ``environment_type`` the keys are conda names (some match
    unrelated PyPI projects), so only ``pip+`` entries are kept.
    """
    if not isinstance(matrix, dict):
        return {}
    pip_native = str(environment_type or "").strip().lower() in DATASMITH_ASV_PIP_ENV_TYPES or not environment_type
    req = matrix.get("req")
    entries = req if isinstance(req, dict) else {k: v for k, v in matrix.items() if k not in _ASV_MATRIX_SECTIONS}
    out: dict[str, list[str]] = {}
    for raw_name, value in entries.items():
        name = str(raw_name)
        if name.lower().startswith(_ASV_PIP_PREFIX):
            name = name[len(_ASV_PIP_PREFIX) :]
        elif not pip_native:
            continue
        if not name:
            continue
        # ``null`` is kept as ``"None"`` (do not install), which differs from an empty
        # list (any version); ``_matrix_requirements`` resolves it.
        values = value if isinstance(value, list | tuple | set) else [value]
        out[name] = [str(v) for v in values]
    return out


def collect_asv_cfg(commit: Commit) -> ASVCfgAggregate:
    """Aggregate every ASV config in the commit.

    An absent ``pythons`` stays empty, so the asv rung only fires when the project
    declares versions.
    """
    aggregate = ASVCfgAggregate()
    for cfg_file in asv_finder(commit):
        cfg: Any
        try:
            cfg = json5.loads(cfg_file.read_text())
        except Exception:  # noqa: S112
            continue
        if not isinstance(cfg, dict):
            continue

        for py in cfg.get("pythons") or []:
            with contextlib.suppress(Exception):
                aggregate.pythons.add(tuple(int(part) for part in str(py).split(".")))

        build_command = cfg.get("build_command")
        if build_command:
            if isinstance(build_command, list | tuple):
                build_command = " && ".join(map(str, build_command)).replace("-mpip", "-m pip")
            aggregate.build_commands.add(str(build_command))

        install_command = cfg.get("install_command")
        if install_command:
            if isinstance(install_command, list | tuple):
                install_command = " && ".join(map(str, install_command))
            aggregate.install_commands.add(str(install_command))

        for name, versions in _matrix_entries(cfg.get("matrix"), cfg.get("environment_type")).items():
            aggregate.matrix.setdefault(name, set()).update(versions)

    return aggregate


@cache_completion(CACHE_LOCATION, table_name="commit_analysis_v2")
def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> ResolutionResult | None:
    """Resolve one commit's environment seed.

    Returns ``None`` only when the repository declares no packaging root at this
    commit.  A project that declares no dependencies gets an empty seed.  The cache
    table is ``commit_analysis_v2`` because older rows have a different shape.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        repo, _checkout_dir, cleanup_checkout = prepare_repo_checkout(repo_name, sha, Path(tmpdir))
        try:
            commit = repo.commit(sha)
            with contextlib.suppress(Exception):
                repo.git.checkout(sha)

            asv = collect_asv_cfg(commit)

            candidates = discover_candidates(commit)
            if not candidates:
                logger.debug("No packaging root at %s@%s; nothing to resolve", repo_name, sha[:8])
                return None
            analyzed: dict[str, CandidateMeta] = {
                root: analyze_candidate_meta(cand) for root, cand in candidates.items()
            }
            primary_root = select_primary_candidate(repo_name, candidates, asv.install_commands, analyzed)
            primary_meta = analyzed[primary_root]

            declared = declare(primary_meta, asv.matrix)

            commit_date = commit.authored_datetime
            choice = select_interpreter(
                requires_python=primary_meta.requires_python,
                trove_versions=trove_versions_from_classifiers(primary_meta.classifiers),
                asv_pythons=asv.pythons,
                commit_date=commit_date,
            )

            # Per-repository ``extras`` and ``operator_pins`` are not read yet, so extras stay out of the seed.
            pinned = pin(declared, python_version=choice.version, commit_date=commit_date)
            probed = probe(pinned, python_version=choice.version)

            dropped = [{"req": d.raw, "reason": d.reason} for d in (*declared.dropped, *pinned.dropped)]

            logger.debug(
                "Resolved %s@%s: python=%s (%s) deps=%d probe=%s dropped=%d",
                repo_name,
                sha[:8],
                choice.version,
                choice.source,
                len(pinned.requirements),
                probed.status,
                len(dropped),
            )

            return ResolutionResult(
                owner_repo=repo_name,
                sha=sha,
                package_name=primary_meta.name,
                package_version=primary_meta.version,
                primary_root=primary_root,
                requires_python=primary_meta.requires_python,
                python_version=choice.version,
                interpreter_source=choice.source,
                env_payload=list(pinned.requirements),
                probe_status=probed.status,
                probe_log=probed.log,
                cutoff_used=pinned.cutoff_used,
                cutoff_relaxed=pinned.cutoff_relaxed,
                dropped_requirements=dropped,
            )
        finally:
            cleanup_checkout()
