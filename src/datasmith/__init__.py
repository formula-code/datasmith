"""DataSmith — toolchain for building the FormulaCode benchmark dataset."""

from __future__ import annotations

import importlib
import os
from typing import TYPE_CHECKING

import dotenv

__version__ = "0.1.0"


def setup_environment() -> None:
    """Load environment variables from tokens.env if present."""
    if os.path.exists("tokens.env"):
        dotenv.load_dotenv("tokens.env")


setup_environment()

# ---------------------------------------------------------------------------
# PEP 562 lazy loading
# ---------------------------------------------------------------------------

_SUBMODULES: set[str] = {
    "github",
    "agents",
    "docker",
    "runners",
    "utils",
    "update",
    "publish",
    "filters",
    "preflight",
}

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    # --- github ---
    "PR": ("datasmith.github", "PR"),
    "Anonymizer": ("datasmith.github", "Anonymizer"),
    "FormulaCodeRecord": ("datasmith.github", "FormulaCodeRecord"),
    "GitHubClient": ("datasmith.github", "GitHubClient"),
    "HookRegistry": ("datasmith.github", "HookRegistry"),
    "Issue": ("datasmith.github", "Issue"),
    "IssueExpanded": ("datasmith.github", "IssueExpanded"),
    "PRChangeSummary": ("datasmith.github", "PRChangeSummary"),
    "PRFileChange": ("datasmith.github", "PRFileChange"),
    "extract_references": ("datasmith.github", "extract_references"),
    "render_problem_statement": ("datasmith.github", "render_problem_statement"),
    "scrape_links": ("datasmith.github", "scrape_links"),
    # --- utils ---
    "Settings": ("datasmith.utils", "Settings"),
    "TokenPool": ("datasmith.utils", "TokenPool"),
    "abatch_upsert": ("datasmith.utils", "abatch_upsert"),
    "afetch_all": ("datasmith.utils", "afetch_all"),
    "batch_upsert": ("datasmith.utils", "batch_upsert"),
    "fetch_all": ("datasmith.utils", "fetch_all"),
    "get_async_client": ("datasmith.utils", "get_async_client"),
    "get_client": ("datasmith.utils", "get_client"),
    "get_logger": ("datasmith.utils", "get_logger"),
    "stable_hash": ("datasmith.utils", "stable_hash"),
    "supabase_cached": ("datasmith.utils", "supabase_cached"),
    "with_backoff": ("datasmith.utils", "with_backoff"),
    # --- update ---
    "Pipeline": ("datasmith.update", "Pipeline"),
    # --- docker ---
    "DockerContext": ("datasmith.docker", "DockerContext"),
    "DockerHubPublisher": ("datasmith.docker", "DockerHubPublisher"),
    "ImageManager": ("datasmith.docker", "ImageManager"),
    # --- agents ---
    "AgentConfig": ("datasmith.agents", "AgentConfig"),
    "ClassificationDecision": ("datasmith.agents", "ClassificationDecision"),
    "ClassifyJudge": ("datasmith.agents", "ClassifyJudge"),
    "CodexResult": ("datasmith.agents", "CodexResult"),
    "OptimizationType": ("datasmith.agents", "OptimizationType"),
    "PerfClassifier": ("datasmith.agents", "PerfClassifier"),
    "ProblemExtraction": ("datasmith.agents", "ProblemExtraction"),
    "ProblemExtractor": ("datasmith.agents", "ProblemExtractor"),
    "SynthesisState": ("datasmith.agents", "SynthesisState"),
    "Synthesizer": ("datasmith.agents", "Synthesizer"),
    "codex_exec": ("datasmith.agents", "codex_exec"),
    "configure_dspy": ("datasmith.agents", "configure_dspy"),
    "ensure_configured": ("datasmith.agents", "ensure_configured"),
    # --- runners ---
    "BaseRunner": ("datasmith.runners", "BaseRunner"),
    "ClassifyPRsRunner": ("datasmith.runners", "ClassifyPRsRunner"),
    "ScrapeCommitsRunner": ("datasmith.runners", "ScrapeCommitsRunner"),
    "ScrapeReposRunner": ("datasmith.runners", "ScrapeReposRunner"),
    "SynthesizeImagesRunner": ("datasmith.runners", "SynthesizeImagesRunner"),
    # --- publish ---
    "HuggingFacePublisher": ("datasmith.publish", "HuggingFacePublisher"),
    "publish_pipeline": ("datasmith.publish", "publish_pipeline"),
    "records_from_parquet": ("datasmith.publish", "records_from_parquet"),
    "records_from_supabase": ("datasmith.publish", "records_from_supabase"),
    "records_to_parquet": ("datasmith.publish", "records_to_parquet"),
    # --- filters ---
    "symbolic_compliance": ("datasmith.filters", "symbolic_compliance"),
    "message_filter": ("datasmith.filters", "message_filter"),
    "has_core_file": ("datasmith.filters", "has_core_file"),
    "check_patch_size": ("datasmith.filters", "check_patch_size"),
    "check_file_compliance": ("datasmith.filters", "check_file_compliance"),
    "estimate_tokens": ("datasmith.filters", "estimate_tokens"),
    # --- preflight ---
    "run_preflight": ("datasmith.preflight", "run_preflight"),
}

__all__ = [
    "__version__",
    "setup_environment",
    *sorted(_SUBMODULES),
    *sorted(_LAZY_IMPORTS),
]


def __getattr__(name: str) -> object:
    if name in _SUBMODULES:
        mod = importlib.import_module(f"datasmith.{name}")
        globals()[name] = mod
        return mod

    if name in _LAZY_IMPORTS:
        module_path, attr_name = _LAZY_IMPORTS[name]
        mod = importlib.import_module(module_path)
        val = getattr(mod, attr_name)
        globals()[name] = val
        return val

    raise AttributeError(f"module 'datasmith' has no attribute {name!r}")


def __dir__() -> list[str]:
    return __all__


# ---------------------------------------------------------------------------
# Static type-checking imports (never executed at runtime)
# ---------------------------------------------------------------------------
if TYPE_CHECKING:
    from datasmith import agents as agents
    from datasmith import docker as docker
    from datasmith import filters as filters
    from datasmith import github as github
    from datasmith import preflight as preflight
    from datasmith import publish as publish
    from datasmith import runners as runners
    from datasmith import update as update
    from datasmith import utils as utils
    from datasmith.agents import (
        AgentConfig as AgentConfig,
    )
    from datasmith.agents import (
        ClassificationDecision as ClassificationDecision,
    )
    from datasmith.agents import (
        ClassifyJudge as ClassifyJudge,
    )
    from datasmith.agents import (
        CodexResult as CodexResult,
    )
    from datasmith.agents import (
        OptimizationType as OptimizationType,
    )
    from datasmith.agents import (
        PerfClassifier as PerfClassifier,
    )
    from datasmith.agents import (
        ProblemExtraction as ProblemExtraction,
    )
    from datasmith.agents import (
        ProblemExtractor as ProblemExtractor,
    )
    from datasmith.agents import (
        SynthesisState as SynthesisState,
    )
    from datasmith.agents import (
        Synthesizer as Synthesizer,
    )
    from datasmith.agents import (
        codex_exec as codex_exec,
    )
    from datasmith.agents import (
        configure_dspy as configure_dspy,
    )
    from datasmith.agents import (
        ensure_configured as ensure_configured,
    )
    from datasmith.docker import (
        DockerContext as DockerContext,
    )
    from datasmith.docker import (
        DockerHubPublisher as DockerHubPublisher,
    )
    from datasmith.docker import (
        ImageManager as ImageManager,
    )
    from datasmith.filters import (
        check_file_compliance as check_file_compliance,
    )
    from datasmith.filters import (
        check_patch_size as check_patch_size,
    )
    from datasmith.filters import (
        estimate_tokens as estimate_tokens,
    )
    from datasmith.filters import (
        has_core_file as has_core_file,
    )
    from datasmith.filters import (
        message_filter as message_filter,
    )
    from datasmith.filters import (
        symbolic_compliance as symbolic_compliance,
    )
    from datasmith.github import (
        PR as PR,
    )
    from datasmith.github import (
        Anonymizer as Anonymizer,
    )
    from datasmith.github import (
        FormulaCodeRecord as FormulaCodeRecord,
    )
    from datasmith.github import (
        GitHubClient as GitHubClient,
    )
    from datasmith.github import (
        HookRegistry as HookRegistry,
    )
    from datasmith.github import (
        Issue as Issue,
    )
    from datasmith.github import (
        IssueExpanded as IssueExpanded,
    )
    from datasmith.github import (
        PRChangeSummary as PRChangeSummary,
    )
    from datasmith.github import (
        PRFileChange as PRFileChange,
    )
    from datasmith.github import (
        extract_references as extract_references,
    )
    from datasmith.github import (
        render_problem_statement as render_problem_statement,
    )
    from datasmith.github import (
        scrape_links as scrape_links,
    )
    from datasmith.preflight import run_preflight as run_preflight
    from datasmith.publish import (
        HuggingFacePublisher as HuggingFacePublisher,
    )
    from datasmith.publish import (
        publish_pipeline as publish_pipeline,
    )
    from datasmith.publish import (
        records_from_parquet as records_from_parquet,
    )
    from datasmith.publish import (
        records_from_supabase as records_from_supabase,
    )
    from datasmith.publish import (
        records_to_parquet as records_to_parquet,
    )
    from datasmith.runners import (
        BaseRunner as BaseRunner,
    )
    from datasmith.runners import (
        ClassifyPRsRunner as ClassifyPRsRunner,
    )
    from datasmith.runners import (
        ScrapeCommitsRunner as ScrapeCommitsRunner,
    )
    from datasmith.runners import (
        ScrapeReposRunner as ScrapeReposRunner,
    )
    from datasmith.runners import (
        SynthesizeImagesRunner as SynthesizeImagesRunner,
    )
    from datasmith.update import Pipeline as Pipeline
    from datasmith.utils import (
        Settings as Settings,
    )
    from datasmith.utils import (
        TokenPool as TokenPool,
    )
    from datasmith.utils import (
        abatch_upsert as abatch_upsert,
    )
    from datasmith.utils import (
        afetch_all as afetch_all,
    )
    from datasmith.utils import (
        batch_upsert as batch_upsert,
    )
    from datasmith.utils import (
        fetch_all as fetch_all,
    )
    from datasmith.utils import (
        get_async_client as get_async_client,
    )
    from datasmith.utils import (
        get_client as get_client,
    )
    from datasmith.utils import (
        get_logger as get_logger,
    )
    from datasmith.utils import (
        stable_hash as stable_hash,
    )
    from datasmith.utils import (
        supabase_cached as supabase_cached,
    )
    from datasmith.utils import (
        with_backoff as with_backoff,
    )
