"""Type stub for datasmith — keeps mypy happy with PEP 562 lazy loading."""

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
from datasmith.docker import (
    MultiObjVerifier as MultiObjVerifier,
)
from datasmith.docker import (
    ProfileVerifier as ProfileVerifier,
)
from datasmith.docker import (
    PytestVerifier as PytestVerifier,
)
from datasmith.docker import (
    SmokeVerifier as SmokeVerifier,
)
from datasmith.docker import (
    VerifyResult as VerifyResult,
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

__version__: str

def setup_environment() -> None: ...
