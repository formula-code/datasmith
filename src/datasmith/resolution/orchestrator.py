"""Compose the resolution units for one commit.

The predecessor was one 669-line function with two paths through it.  The first
compiled the project's packaging file directly and returned early on success; the
second aggregated requirements from every file it could find, healed whatever
failed, and injected test tooling.  Two commits of one repository could therefore
be resolved by different halves and get different environments for no principled
reason, and neither half recorded which one had answered.

Here the flow is one path made of six small units, each testable alone:

``discover`` finds the packaging roots, ``declare`` reads what the project states
it needs, ``interpreter`` picks the Python from that declaration, ``pin`` compiles
it once, ``probe`` dry-runs the result advisorily, and this module emits the row.

Two things the predecessor did are gone rather than repaired.  It gated: a failed
dry-run set ``can_install = False`` and stages 5 and 6 then skipped the PR, so
3,217 performance PRs were never attempted.  The probe now orders the queue
instead.  And it installed the resolved set for real on the host, which proves
nothing about the container -- different interpreter, different base image, no
compilers -- while dominating the stage's runtime.
"""

from __future__ import annotations

import contextlib
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

#: Stamped on every row this resolver writes.  Rows carrying ``legacy`` came from
#: the predecessor and carry no provenance at all.
RESOLVER_VERSION = "2026.08.23"

#: Keys of an ASV ``matrix`` that name a section rather than a package.  Modern
#: asv nests requirements under ``req`` and environment variables under ``env`` /
#: ``env_nobuild``; older configs put the packages at the top level.
_ASV_MATRIX_SECTIONS = ("req", "env", "env_nobuild")


@dataclass(frozen=True)
class ResolutionResult:
    """One commit's resolved environment, and the story of how it was reached."""

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


def _matrix_entries(matrix: object) -> dict[str, list[str]]:
    """Read the packages an ASV ``matrix`` names, whichever shape it is written in.

    asv >= 0.6 nests them: ``{"req": {"numpy": ["1.20", ""]}, "env": {...}}``.
    Reading such a config flat offers ``req`` and ``env`` to the resolver as
    package names, so the section keys are stepped through rather than into.
    Older configs name the packages at the top level, which is the fallback.
    """
    if not isinstance(matrix, dict):
        return {}
    req = matrix.get("req")
    entries = req if isinstance(req, dict) else {k: v for k, v in matrix.items() if k not in _ASV_MATRIX_SECTIONS}
    out: dict[str, list[str]] = {}
    for name, value in entries.items():
        if isinstance(value, list | tuple | set):
            out[str(name)] = [str(v) for v in value]
        else:
            out[str(name)] = [str(value)]
    return out


def collect_asv_cfg(commit: Commit) -> ASVCfgAggregate:
    """Aggregate every ASV config in the commit.

    The predecessor read each parsed config with ``getattr(cfg, "pythons", [])``.
    ``json5.loads`` returns a ``dict``, and a dict has no such attribute, so every
    field came back empty: the asv rung of the interpreter ladder never fired and
    ``matrix`` never reached ``declare``.  Reads here go through ``dict.get``.

    An absent ``pythons`` stays absent.  The predecessor substituted the full
    supported set, which is a declaration the project never made -- and with the
    reads fixed it would make the asv rung fire for every repository and override
    the two rungs above it.
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

        for name, versions in _matrix_entries(cfg.get("matrix")).items():
            aggregate.matrix.setdefault(name, set()).update(versions)

    return aggregate


@cache_completion(CACHE_LOCATION, table_name="commit_analysis_v2")
def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> ResolutionResult | None:
    """Resolve one commit's environment seed.

    Returns ``None`` only when the repository declares no packaging root at this
    commit -- there is then nothing to resolve.  Everything else is an answer,
    including an empty seed: a project that declares no dependencies gets ``[]``
    and says so, where the predecessor would invent a list by guessing PyPI names
    from import statements.

    The cache table is ``commit_analysis_v2``.  The predecessor's rows hold a
    ``dict`` of a different shape, and unpickling one into this dataclass would
    fail far from here.
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

            # ``extras`` and ``operator_pins`` are the per-repository opt-ins the
            # design gives ``formulacode_task_overrides``. Nothing reads that
            # table yet, so the defaults stand: extras stay out of the seed.
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
