from __future__ import annotations

import datetime
import enum
import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from datasmith.agents.rate_limit import RateLimitError
from datasmith.agents.rate_limit import check as check_rate_limit
from datasmith.agents.sandbox import SandboxResult, verify_context
from datasmith.agents.tamper_audit import TamperResult, classify_context
from datasmith.docker.context import DockerContext
from datasmith.docker.manifest import evaluate_invariants
from datasmith.utils import get_client, get_logger

if TYPE_CHECKING:  # pragma: no cover - annotations only
    from datasmith.agents.reflexive.loop import LoopOutcome
    from datasmith.agents.reflexive.schema import ProducerPlan
    from datasmith.agents.reflexive.severity import GradedReport

logger = get_logger("agents.synthesizer")

# Skip the TRY_SIMILAR state, so a build can only come from the stock template.
#
# TRY_SIMILAR runs BEFORE TRY_DEFAULT and reuses a context that succeeded for
# another commit of the same repository. Those stored contexts are
# agent-authored: 128 repositories' contexts install a sitecustomize shim into
# site-packages, which then runs inside the measured benchmark process.
#
# Two situations need it off. Measuring how often the stock template alone
# works, because a TRY_SIMILAR success means TRY_DEFAULT never runs and the
# repository silently leaves the denominator. And rebuilding a container that
# must not inherit an older agent's environment edits.
DATASMITH_SKIP_SIMILAR_CONTEXTS: bool = os.environ.get("DATASMITH_SKIP_SIMILAR_CONTEXTS", "") not in ("", "0")


class SynthesisState(str, enum.Enum):
    CHECK_CACHE = "check_cache"
    FIND_SIMILAR = "find_similar"
    TRY_SIMILAR = "try_similar"
    TRY_DEFAULT = "try_default"
    LLM_GENERATE = "llm_generate"
    PRODUCE_VERIFY = "produce_verify"
    FAIL = "fail"


def _load_default_context() -> DockerContext:
    """Read the stock template docker_build_*.sh scripts into a DockerContext.

    Used by TRY_DEFAULT to attempt an unmodified build before any agent work,
    and to persist the working scripts so future PRs for the same repo can
    reuse them via TRY_SIMILAR.
    """
    templates = Path(__file__).parents[1] / "docker" / "templates"

    def _read(name: str) -> str:
        path = templates / name
        return path.read_text() if path.exists() else ""

    return DockerContext(
        build_env_sh=_read("docker_build_env.sh"),
        build_pkg_sh=_read("docker_build_pkg.sh"),
        build_run_sh=_read("docker_build_run.sh"),
    )


def _default_failure_message(failure: dict[str, Any]) -> str:
    """Build a diagnosable message from a TRY_DEFAULT failure.

    `stage` and `error_message` give "run: rc=1", which identifies nothing.
    `failure_json["stdout"]` holds what the build actually printed, so a tail of
    it travels with the row. Failures are grouped by cause when scaling, and a
    return code cannot be grouped.
    """
    stage = failure.get("stage") or "unknown"
    message = failure.get("error_message") or ""
    stdout = (failure.get("stdout") or "").strip()
    stderr = (failure.get("stderr") or "").strip()
    parts = [f"{stage}: {message}".strip()]
    if stderr:
        parts.append(f"--- stderr (tail) ---\n{stderr[-4000:]}")
    if stdout:
        parts.append(f"--- stdout (tail) ---\n{stdout[-6000:]}")
    return "\n".join(parts)[-14_000:]


class Synthesizer:
    """State machine for synthesizing Docker build contexts."""

    def __init__(
        self,
        max_attempts: int = 2,
        dry_run: bool = False,
        agent: str | None = None,
        force: bool = False,
        max_aborts: int = 2,
        max_default_failures_per_repo: int = 3,
    ) -> None:
        self._max_attempts = max_attempts
        self._max_aborts = max_aborts
        self._dry_run = dry_run
        self._agent = agent
        self._force = force
        self._trace: list[SynthesisState] = []
        # Repos for which TRY_DEFAULT has already SUCCEEDED in this run.
        # Once any PR succeeds with the default template for a repo, the row
        # is in `candidate_containers` and later PRs should hit TRY_SIMILAR
        # (which re-verifies the saved context against a new SHA) instead of
        # redundantly rebuilding the default template from scratch.
        self._tried_default_repos: set[tuple[str, str]] = set()
        # Failure counter per repo. If TRY_DEFAULT fails `max_default_failures_per_repo`
        # times for the same repo, we stop retrying and fall through to LLM_GENERATE
        # (or fail if agent=none). Prevents burning hours on repos where every PR's
        # base commit is structurally broken.
        self._max_default_failures_per_repo = max_default_failures_per_repo
        self._default_failures: dict[tuple[str, str], int] = {}

    @property
    def trace(self) -> list[SynthesisState]:
        return list(self._trace)

    def run(  # noqa: C901
        self,
        owner: str,
        repo: str,
        issue_number: int,
        pr_context: str,
        sha: str = "",
        repo_image: str = "",
        env_payload: str = "",
        python_version: str = "",
        force: bool = False,
        base_sha: str = "",
        solution_patch: str = "",
    ) -> DockerContext | None:
        """Run the synthesis state machine. Returns DockerContext on success, None on failure."""
        self._trace = []
        force = force or self._force

        # State: CHECK_CACHE
        self._trace.append(SynthesisState.CHECK_CACHE)
        cached = self._check_cache(owner, repo, sha)
        if (not force) and (cached is not None):
            logger.info("Cache hit for %s/%s@%s", owner, repo, sha[:12] if sha else "?")
            return cached

        # State: FIND_SIMILAR
        self._trace.append(SynthesisState.FIND_SIMILAR)
        if DATASMITH_SKIP_SIMILAR_CONTEXTS:
            logger.info("TRY_SIMILAR disabled via DATASMITH_SKIP_SIMILAR_CONTEXTS")
            similar_contexts = []
        else:
            similar_contexts = self._find_similar(owner, repo, issue_number)

        # State: TRY_SIMILAR
        failed_attempts: list[tuple[DockerContext, SandboxResult]] = []
        if similar_contexts:
            self._trace.append(SynthesisState.TRY_SIMILAR)
            for ctx in similar_contexts:
                result = verify_context(
                    owner=owner,
                    repo=repo,
                    sha=sha,
                    repo_image=repo_image,
                    env_payload=env_payload,
                    python_version=python_version,
                    context=ctx,
                    base_sha=base_sha,
                    solution_patch=solution_patch,
                )
                if result.success:
                    logger.info("Similar context passed for %s/%s#%d", owner, repo, issue_number)
                    self._save_context(
                        owner,
                        repo,
                        sha,
                        issue_number,
                        ctx,
                        resource_metrics=result.resource_metrics,
                        build_manifest=result.build_manifest,
                    )
                    return ctx
                failed_attempts.append((ctx, result))

        # State: TRY_DEFAULT — attempt a build with the stock template scripts.
        #
        # Concurrency note: a single Synthesizer is shared across N concurrent
        # workers. The old design checked-and-marked `_tried_default_repos`
        # before `verify_context` ran, so in a race all workers but one would
        # short-circuit TRY_DEFAULT even when the first worker's attempt was
        # still in flight — and a single unlucky SHA failure would doom every
        # subsequent PR in the same repo.
        #
        # New semantics:
        #   - `_tried_default_repos` marks only SUCCESS. Once any PR succeeds
        #     with the default template for this repo, the row is in
        #     `candidate_containers` and later PRs hit TRY_SIMILAR.
        #   - `_default_failures` counts failures. After
        #     `max_default_failures_per_repo` consecutive failures we stop
        #     retrying to avoid burning hours on structurally broken repos
        #     (e.g. every PR's env_payload is incompatible with the base image).
        fail_count = self._default_failures.get((owner, repo), 0)
        already_succeeded = (owner, repo) in self._tried_default_repos
        too_many_failures = fail_count >= self._max_default_failures_per_repo
        if already_succeeded:
            logger.debug(
                "Skipping TRY_DEFAULT for %s/%s#%d — default already succeeded for this repo",
                owner,
                repo,
                issue_number,
            )
        elif too_many_failures:
            logger.info(
                "Skipping TRY_DEFAULT for %s/%s#%d — %d prior failures (cap=%d)",
                owner,
                repo,
                issue_number,
                fail_count,
                self._max_default_failures_per_repo,
            )
        if (not already_succeeded) and (not too_many_failures):
            self._trace.append(SynthesisState.TRY_DEFAULT)
            default_ctx = _load_default_context()
            default_started = time.monotonic()
            result = verify_context(
                owner=owner,
                repo=repo,
                sha=sha,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                context=default_ctx,
                base_sha=base_sha,
                solution_patch=solution_patch,
            )
            default_failure = result.failure_json or {}
            self._log_default_attempt(
                owner=owner,
                repo=repo,
                sha=sha,
                issue_number=issue_number,
                success=bool(result.success),
                duration_s=round(time.monotonic() - default_started, 2),
                # The stage and message alone say "run stage, rc=1", which is
                # not enough to diagnose anything. failure_json carries the
                # build's own output, so keep a tail of it. At a few hundred
                # repositories, grouping failures by cause is the whole game,
                # and a cause cannot be grouped from a return code.
                error_message=(None if result.success else _default_failure_message(default_failure)),
            )
            if result.success:
                tamper = classify_context(default_ctx)
                if tamper.tampered:
                    # Default template should never tamper — this is a red flag
                    # that one of our committed templates drifted.  Log and
                    # refuse to cache.
                    logger.error(
                        "Default template tamper-audit failed for %s/%s#%d: %s",
                        owner,
                        repo,
                        issue_number,
                        tamper.as_list(),
                    )
                    self._log_tamper(owner, repo, sha, issue_number, 0, tamper, "try_default")
                    self._default_failures[(owner, repo)] = fail_count + 1
                    failed_attempts.append((default_ctx, result))
                else:
                    logger.info("Default template build succeeded for %s/%s#%d", owner, repo, issue_number)
                    self._tried_default_repos.add((owner, repo))
                    self._save_context(
                        owner,
                        repo,
                        sha,
                        issue_number,
                        default_ctx,
                        resource_metrics=result.resource_metrics,
                        build_manifest=result.build_manifest,
                    )
                    return default_ctx
            else:
                self._default_failures[(owner, repo)] = fail_count + 1
                failed_attempts.append((default_ctx, result))
            logger.info(
                "Default template build failed for %s/%s#%d — feeding trace into LLM priors",
                owner,
                repo,
                issue_number,
            )

        # State: PRODUCE_VERIFY — the reflexive producer/verifier loop.
        #
        # Replaces LLM_GENERATE when DATASMITH_PV_ENABLED is set. The states
        # above are untouched: a repo the stock template already builds never
        # reaches here, and that path costs nothing.
        #
        # In this path pytest runs only in the verifier's battery and
        # severity.py grades the verdict. The legacy `rc != 0` gate in
        # run_tests applies to TRY_SIMILAR and TRY_DEFAULT only -- otherwise a
        # container would be rejected on an exit code before the verifier could
        # weigh it, and the entire soft column would be unreachable.
        from datasmith.agents.reflexive.loop import DATASMITH_PV_ENABLED

        if DATASMITH_PV_ENABLED and self._agent != "none":
            self._trace.append(SynthesisState.PRODUCE_VERIFY)
            outcome = self._run_produce_verify(
                owner=owner,
                repo=repo,
                sha=sha,
                issue_number=issue_number,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                base_sha=base_sha,
                solution_patch=solution_patch,
            )
            if outcome.accepted and outcome.context is not None:
                self._save_context(
                    owner,
                    repo,
                    sha,
                    issue_number,
                    outcome.context,
                    resource_metrics=outcome.resource_metrics,
                    build_manifest=outcome.build_manifest,
                )
                return outcome.context
            logger.info(
                "PRODUCE_VERIFY failed for %s/%s#%d after %d round(s): %s",
                owner,
                repo,
                issue_number,
                outcome.rounds,
                outcome.stop_reason,
            )
            self._trace.append(SynthesisState.FAIL)
            return None

        # State: LLM_GENERATE (sandbox-based)
        # Skip LLM generation when using the "none" agent — rely only on similar contexts.
        if self._agent == "none":
            self._trace.append(SynthesisState.FAIL)
            logger.info(
                "Agent is 'none' — skipping LLM generation for %s/%s#%d",
                owner,
                repo,
                issue_number,
            )
            return None

        self._trace.append(SynthesisState.LLM_GENERATE)
        prior_attempts = _format_prior_attempts(failed_attempts) if failed_attempts else ""
        attempt_idx = 0
        abort_count = 0
        while attempt_idx < self._max_attempts:
            generated, metrics, build_manifest, aborted = self._sandbox_generate(
                owner=owner,
                repo=repo,
                sha=sha,
                pr_context=pr_context,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                prior_attempts=prior_attempts,
                issue_number=issue_number,
                attempt_index=attempt_idx,
                base_sha=base_sha,
                solution_patch=solution_patch,
            )
            if generated is not None:
                tamper = classify_context(generated)
                if tamper.tampered:
                    # Local CI passed but the agent reached the pass state via
                    # a pattern we consider fraudulent (forged validator
                    # artifacts, fabricated benchmarks, etc.).  Log the tags
                    # to error_logs and treat the attempt as failed so the
                    # next iteration re-prompts the agent from scratch.  The
                    # agent is *not* told which specific pattern was caught —
                    # the cooperative framing stays intact.
                    logger.warning(
                        "Sandbox synthesis attempt %d for %s/%s#%d passed local CI but failed tamper audit: %s",
                        attempt_idx + 1,
                        owner,
                        repo,
                        issue_number,
                        tamper.as_list(),
                    )
                    self._log_tamper(owner, repo, sha, issue_number, attempt_idx, tamper, "llm_generate")
                    # Fail-fast: an agent that fabricates artifacts on one
                    # attempt will almost certainly do it again on the next.
                    # Break immediately instead of burning another multi-hour
                    # session on the same PR.
                    break
                logger.info(
                    "Sandbox synthesis succeeded for %s/%s#%d (attempt %d)",
                    owner,
                    repo,
                    issue_number,
                    attempt_idx + 1,
                )
                self._save_context(
                    owner,
                    repo,
                    sha,
                    issue_number,
                    generated,
                    resource_metrics=metrics,
                    build_manifest=build_manifest,
                )
                return generated
            if aborted and abort_count < self._max_aborts:
                abort_count += 1
                logger.warning(
                    "Sandbox synthesis attempt for %s/%s#%d aborted (verifier never ran) "
                    "— retrying without consuming attempt budget (%d/%d aborts used)",
                    owner,
                    repo,
                    issue_number,
                    abort_count,
                    self._max_aborts,
                )
                continue
            logger.warning(
                "Sandbox synthesis attempt %d failed for %s/%s#%d",
                attempt_idx + 1,
                owner,
                repo,
                issue_number,
            )
            attempt_idx += 1

        # State: FAIL
        self._trace.append(SynthesisState.FAIL)
        logger.warning("All synthesis attempts failed for %s/%s#%d", owner, repo, issue_number)
        return None

    def _run_produce_verify(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        repo_image: str,
        env_payload: str,
        python_version: str,
        base_sha: str,
        solution_patch: str,
    ) -> LoopOutcome:
        """Drive the reflexive loop for one task.

        Producer and verifier get SEPARATE agent instances so their contexts
        cannot merge, even when both resolve to the same backend.
        """
        import tempfile

        from datasmith.agents.installed.base import get_agent
        from datasmith.agents.reflexive.loop import run_loop
        from datasmith.agents.reflexive.producer import revise as producer_revise
        from datasmith.agents.reflexive.verifier import verify as verifier_verify

        producer_agent = get_agent([os.environ.get("DATASMITH_PV_PRODUCER_AGENT", "codex")])
        verifier_agent = get_agent([os.environ.get("DATASMITH_PV_VERIFIER_AGENT", "codex")])

        # `verify_context` runs local_ci.py, which runs run_tests, which fails
        # on `rc != 0`. Using it here would re-create the exact contradiction
        # the spec settled: a fluids-shaped container would come back
        # success=False from the LEGACY pytest gate, mode would become
        # build_failed, and the verifier would never receive an image to run
        # its battery against -- so it could never waive anything and the whole
        # soft column would be unreachable.
        #
        # So PRODUCE_VERIFY builds the image WITHOUT the test gate. In this
        # path pytest runs only in the verifier's battery.
        last: dict[str, SandboxResult | None] = {"result": None}

        def build(context: DockerContext) -> tuple[bool, str | None, str]:
            result = verify_context(
                owner=owner,
                repo=repo,
                sha=sha,
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                context=context,
                base_sha=base_sha,
                solution_patch=solution_patch,
                run_tests_gate=False,
            )
            # Keep the whole result. The manifest is what stage 8 gates on, and
            # a PV-accepted container landing in candidate_containers with a
            # NULL build_manifest is indistinguishable from one built before
            # manifests existed.
            last["result"] = result
            log = json.dumps(result.failure_json or {})[:200000]
            # The tag comes from the result, never assumed. verify_context
            # serves TRY_SIMILAR and does not necessarily tag what this caller
            # would guess.
            tag = result.image_tag if result.success else None
            return bool(result.success), tag, log

        def on_accept() -> tuple[dict | None, dict]:
            result = last["result"]
            if result is None:
                return None, {}
            return result.build_manifest, result.resource_metrics

        with tempfile.TemporaryDirectory(prefix="fc-pv-") as tmp:
            workdir = Path(tmp)
            context = _load_default_context()
            for filename, field_name in DockerContext._FILE_MAP.items():
                (workdir / filename).write_text(getattr(context, field_name), encoding="utf-8")

            def revise_and_audit(
                ctx: DockerContext, graded: GradedReport
            ) -> tuple[DockerContext | None, ProducerPlan | None]:
                """Producer edit, then the tamper audit on what it produced.

                The legacy path runs classify_context after TRY_DEFAULT and
                after every LLM_GENERATE attempt. PRODUCE_VERIFY must too, or
                producer-side tampering is checked by nobody: the battery
                collects functional facts, and `tamper_audit` would be a check
                id nothing can ever emit. The producer is the agent with motive
                here.
                """
                revised, plan = producer_revise(ctx, graded, producer_agent, workdir)
                if revised is None:
                    return None, plan
                tamper = classify_context(revised)
                if tamper.tampered:
                    logger.error(
                        "PRODUCE_VERIFY tamper audit failed for %s/%s#%d: %s",
                        owner,
                        repo,
                        issue_number,
                        tamper.as_list(),
                    )
                    self._log_tamper(owner, repo, sha, issue_number, 0, tamper, "produce_verify")
                    return None, plan
                return revised, plan

            return run_loop(
                context=context,
                build=build,
                verify=lambda image, log, mode: verifier_verify(image, log, verifier_agent, mode),
                revise=revise_and_audit,
                workdir=workdir,
                on_accept=on_accept,
            )

    def _check_cache(self, owner: str, repo: str, sha: str) -> DockerContext | None:
        if not sha:
            return None
        try:
            client = get_client()
            resp = (
                client.table("candidate_containers")
                .select("*")
                .eq("owner", owner)
                .eq("repo", repo)
                .eq("sha", sha)
                .execute()
            )
            if resp.data:
                row = cast(dict[str, Any], resp.data[0])
                return DockerContext(
                    dockerfile=row.get("dockerfile", ""),
                    build_base_sh=row.get("build_base_sh", ""),
                    build_env_sh=row.get("build_env_sh", ""),
                    build_pkg_sh=row.get("build_pkg_sh", ""),
                    build_run_sh=row.get("build_run_sh", ""),
                    build_final_sh=row.get("build_final_sh", ""),
                    profile_sh=row.get("profile_sh", ""),
                    run_tests_sh=row.get("run_tests_sh", ""),
                    entrypoint_sh=row.get("entrypoint_sh", ""),
                )
        except Exception:
            logger.debug("Cache check failed, proceeding")
        return None

    def _find_similar(self, owner: str, repo: str, issue_number: int) -> list[DockerContext]:
        """Find previously successful build contexts for the same repository.

        Results are ordered by chronological proximity to the given PR so that
        the most temporally adjacent contexts — most likely to share the same
        dependency environment — are tried first.
        """
        try:
            client = get_client()

            # Step 1: look up the current PR's creation date.
            pr_resp = (
                client.table("pull_requests")
                .select("created_at")
                .eq("owner", owner)
                .eq("repo", repo)
                .eq("issue_number", issue_number)
                .execute()
            )
            current_date: datetime.datetime | None = None
            if pr_resp.data:
                raw = cast(dict[str, Any], pr_resp.data[0]).get("created_at")
                if raw:
                    current_date = _parse_ts(raw)

            # Step 2: fetch all non-empty contexts for this repo.
            ctx_resp = (
                client.table("candidate_containers")
                .select("issue_number,build_pkg_sh,build_run_sh")
                .eq("owner", owner)
                .eq("repo", repo)
                .execute()
            )
            rows = cast(list[dict[str, Any]], ctx_resp.data)
            rows = [r for r in rows if r.get("build_pkg_sh")]

            if not rows:
                return []

            # Step 3: if we have a reference date, sort by proximity.
            if current_date is not None:
                context_issue_numbers = [r["issue_number"] for r in rows if r.get("issue_number") is not None]
                pr_dates: dict[int, datetime.datetime] = {}
                if context_issue_numbers:
                    dates_resp = (
                        client.table("pull_requests")
                        .select("issue_number,created_at")
                        .eq("owner", owner)
                        .eq("repo", repo)
                        .in_("issue_number", context_issue_numbers)
                        .execute()
                    )
                    for p in cast(list[dict[str, Any]], dates_resp.data):
                        iss = p.get("issue_number")
                        raw_date = p.get("created_at")
                        if iss is not None and raw_date:
                            pr_dates[iss] = _parse_ts(raw_date)

                _sentinel = datetime.timedelta.max

                def _proximity(row: dict[str, Any]) -> datetime.timedelta:
                    iss = row.get("issue_number")
                    d = pr_dates.get(iss) if iss is not None else None
                    return abs(d - current_date) if d is not None else _sentinel

                rows.sort(key=_proximity)

            rows = rows[:5]
            return [
                DockerContext(
                    build_pkg_sh=r.get("build_pkg_sh", ""),
                    build_run_sh=r.get("build_run_sh", ""),
                )
                for r in rows
            ]
        except Exception:
            logger.debug("Similar context lookup failed")
            return []

    def _sandbox_generate(
        self,
        owner: str,
        repo: str,
        sha: str,
        pr_context: str,
        repo_image: str,
        env_payload: str,
        python_version: str,
        prior_attempts: str = "",
        issue_number: int = 0,
        attempt_index: int = 0,
        base_sha: str = "",
        solution_patch: str = "",
    ) -> tuple[DockerContext | None, dict, dict | None, bool]:
        from datasmith.agents.sandbox import SandboxRunner

        runner = SandboxRunner(agent=self._agent)
        result = runner.run(
            owner=owner,
            repo=repo,
            sha=sha,
            repo_image=repo_image,
            env_payload=env_payload,
            python_version=python_version,
            pr_context=pr_context,
            prior_attempts=prior_attempts,
            dry_run=self._dry_run,
            base_sha=base_sha,
            solution_patch=solution_patch,
        )
        self._log_attempt(
            owner=owner,
            repo=repo,
            sha=sha,
            issue_number=issue_number,
            attempt_index=attempt_index,
            result=result,
        )
        # Surface budget exhaustion as a typed exception so the runner can
        # pause *all* workers until the reset time instead of burning the
        # remaining attempt budget on what will just be more ~2s failures.
        is_rl, reset_at = check_rate_limit(result.agent_name, result.raw_agent_output)
        if is_rl:
            raise RateLimitError(
                agent_name=result.agent_name,
                reset_at=reset_at,
                message=(f"{result.agent_name} hit usage limit during synthesis for {owner}/{repo}@{sha[:12]}"),
            )
        ctx = result.docker_context if result.success else None
        return ctx, result.resource_metrics, result.build_manifest, result.aborted

    def _log_attempt(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        attempt_index: int,
        result: SandboxResult,
    ) -> None:
        """Persist agent output to the ``error_logs`` Supabase table."""
        timestamp = datetime.datetime.now(tz=datetime.UTC).isoformat()

        failure = result.failure_json or {}
        # Cap raw output at 100 KB for Supabase storage
        raw_output = result.raw_agent_output
        if len(raw_output) > 100_000:
            raw_output = raw_output[-100_000:]

        # Aborted attempts (agent never produced failure.json or
        # verification_success.json) get a sentinel stage so they're
        # distinguishable from real verifier failures in error_logs.
        is_rl, rl_reset = check_rate_limit(result.agent_name, result.raw_agent_output)
        rl_reset_iso: str | None = rl_reset.isoformat() if (is_rl and rl_reset) else None
        if is_rl:
            failure_stage: str | None = "rate_limited"
            error_message: str | None = (
                f"{result.agent_name} hit weekly/periodic usage limit; reset_at={rl_reset_iso or 'unknown'}"
            )
        elif result.aborted:
            failure_stage = "aborted"
            error_message = (
                "Agent exited without running local_ci.py to completion "
                "(no failure.json or verification_success.json found)."
            )
        else:
            failure_stage = failure.get("stage") or None
            error_message = (failure.get("error_message") or "")[-10_000:] or None

        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "attempt_index": attempt_index,
            "agent_name": result.agent_name,
            "success": result.success,
            "duration_s": result.duration_s,
            "failure_stage": failure_stage,
            "failure_return_code": failure.get("return_code") or None,
            "error_message": error_message,
            "agent_output": raw_output or None,
            "files_changed": json.dumps(result.files_changed),
            "resource_metrics": result.resource_metrics or None,
            "rate_limit_reset_at": rl_reset_iso,
            "created_at": timestamp,
        }
        try:
            client = get_client()
            client.table("error_logs").insert(row).execute()
            logger.info(
                "Logged synthesis attempt to error_logs for %s/%s@%s attempt %d", owner, repo, sha[:12], attempt_index
            )
        except Exception:
            logger.debug("Failed to log synthesis attempt to Supabase", exc_info=True)

    def _log_default_attempt(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        success: bool,
        duration_s: float,
        error_message: str | None,
    ) -> None:
        """Record one TRY_DEFAULT outcome in ``error_logs``.

        The no-agent path is the only one that can build without spending agent
        time, so its success rate decides how much agent work the pipeline
        needs. Rows carry ``agent_name="default_template"`` so the rate is a
        single query, and they never carry an agent transcript.

        A logging failure must never fail a build, so every error is swallowed.
        """
        row: dict[str, Any] = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "attempt_index": 0,
            "agent_name": "default_template",
            "success": success,
            "duration_s": duration_s,
            "failure_stage": None if success else "default_template",
            "error_message": (error_message or "")[-10_000:] or None,
            "created_at": datetime.datetime.now(tz=datetime.UTC).isoformat(),
        }
        try:
            get_client().table("error_logs").insert(row).execute()
        except Exception:
            logger.debug("Failed to log default-template attempt", exc_info=True)

    def _log_tamper(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        attempt_index: int,
        tamper: TamperResult,
        source: str,
    ) -> None:
        """Persist a post-verification tamper detection to ``error_logs``.

        The row uses ``failure_stage="tamper_detected"`` and stores the
        detected tag set in ``error_message`` so we can audit later without
        grepping raw agent output.
        """
        timestamp = datetime.datetime.now(tz=datetime.UTC).isoformat()
        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "attempt_index": attempt_index,
            "agent_name": self._agent,
            "success": False,
            "failure_stage": "tamper_detected",
            "error_message": f"source={source} tags={','.join(tamper.as_list())}",
            "created_at": timestamp,
        }
        try:
            client = get_client()
            client.table("error_logs").insert(row).execute()  # type: ignore[arg-type]
            logger.info(
                "Logged tamper detection for %s/%s@%s attempt %d: %s",
                owner,
                repo,
                sha[:12],
                attempt_index,
                tamper.as_list(),
            )
        except Exception:
            logger.debug("Failed to log tamper detection to Supabase", exc_info=True)

    def _save_context(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        ctx: DockerContext,
        resource_metrics: dict | None = None,
        env_payload_override: str | None = None,
        build_manifest: dict | None = None,
    ) -> None:
        """Persist the agent-edited scripts to the ``candidate_containers`` table.

        Saves ``build_pkg_sh``, ``build_run_sh``, and ``build_env_sh``.
        When the agent also modified the env payload, ``env_payload_override``
        is persisted to the ``env_payload`` column. ``build_manifest`` — the
        sealed build facts merged with verify-time observations — is
        persisted as-is; ``manifest_warnings`` is derived from it via
        ``evaluate_invariants`` (warn-severity invariant ids only, since the
        manifest itself carries no precomputed invariant report).
        """
        if not sha:
            return
        client = get_client()
        row: dict = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "build_pkg_sh": ctx.build_pkg_sh,
            "build_run_sh": ctx.build_run_sh,
            "build_env_sh": ctx.build_env_sh,
        }
        if resource_metrics:
            row["resource_metrics"] = resource_metrics
        if env_payload_override:
            row["env_payload"] = env_payload_override
        if build_manifest:
            row["build_manifest"] = build_manifest
            warnings = evaluate_invariants(build_manifest).warnings
            if warnings:
                row["manifest_warnings"] = warnings
        client.table("candidate_containers").upsert(row).execute()
        logger.info("Saved context for %s/%s@%s", owner, repo, sha[:12])


def _parse_ts(ts: str) -> datetime.datetime:
    """Parse an ISO-8601 timestamp string to a timezone-aware datetime.

    Handles both ``Z`` and ``+HH:MM`` UTC offset suffixes, which is
    necessary for Python 3.9/3.10 compatibility where ``fromisoformat``
    does not accept the trailing ``Z``.
    """
    return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _format_prior_attempts(attempts: list[tuple[DockerContext, SandboxResult]]) -> str:
    """Format failed TRY_SIMILAR attempts into context for the LLM agent."""
    lines = [
        "# Prior Attempts",
        "",
        "The following build contexts were tried and failed.",
        "Use these failures to inform your approach — avoid repeating the same mistakes.",
        "",
    ]
    for i, (ctx, result) in enumerate(attempts, 1):
        failure = result.failure_json or {}
        stage = failure.get("stage", "unknown")
        rc = failure.get("return_code", 1)
        error = failure.get("error_message", "")

        lines.append(f"## Attempt {i}")
        lines.append("")
        lines.append(f"**Stage**: {stage}")
        lines.append(f"**Return code**: {rc}")
        lines.append("")

        lines.append("### docker_build_pkg.sh")
        lines.append("```bash")
        pkg = ctx.build_pkg_sh
        if pkg and len(pkg) > 3000:
            lines.append(pkg[:3000])
            lines.append("# ... (truncated)")
        else:
            lines.append(pkg or "(empty)")
        lines.append("```")
        lines.append("")

        lines.append("### docker_build_run.sh")
        lines.append("```bash")
        run = ctx.build_run_sh
        if run and len(run) > 3000:
            lines.append(run[:3000])
            lines.append("# ... (truncated)")
        else:
            lines.append(run or "(empty)")
        lines.append("```")
        lines.append("")

        if error:
            lines.append("### Error output")
            lines.append("```")
            lines.append(error[-3000:])
            lines.append("```")
            lines.append("")

        if result.agent_output:
            stdout_tail = result.agent_output[-3000:]
            lines.append("### Build output (last 3000 chars)")
            lines.append("```")
            lines.append(stdout_tail)
            lines.append("```")
            lines.append("")
    return "\n".join(lines)
