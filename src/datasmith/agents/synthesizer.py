from __future__ import annotations

import datetime
import enum
import json
from typing import Any, cast

from datasmith.agents.sandbox import SandboxResult, verify_context
from datasmith.docker.context import DockerContext
from datasmith.utils import get_client, get_logger

logger = get_logger("agents.synthesizer")


class SynthesisState(str, enum.Enum):
    CHECK_CACHE = "check_cache"
    FIND_SIMILAR = "find_similar"
    TRY_SIMILAR = "try_similar"
    LLM_GENERATE = "llm_generate"
    FAIL = "fail"


class Synthesizer:
    """State machine for synthesizing Docker build contexts."""

    def __init__(
        self,
        max_attempts: int = 2,
        dry_run: bool = False,
        agent: str | None = None,
        force: bool = False,
    ) -> None:
        self._max_attempts = max_attempts
        self._dry_run = dry_run
        self._agent = agent
        self._force = force
        self._trace: list[SynthesisState] = []

    @property
    def trace(self) -> list[SynthesisState]:
        return list(self._trace)

    def run(
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
                    )
                    return ctx
                failed_attempts.append((ctx, result))

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
        for attempt_idx in range(self._max_attempts):
            generated, metrics = self._sandbox_generate(
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
            )
            if generated is not None:
                logger.info(
                    "Sandbox synthesis succeeded for %s/%s#%d (attempt %d)",
                    owner,
                    repo,
                    issue_number,
                    attempt_idx + 1,
                )
                self._save_context(owner, repo, sha, issue_number, generated, resource_metrics=metrics)
                return generated
            logger.warning(
                "Sandbox synthesis attempt %d failed for %s/%s#%d",
                attempt_idx + 1,
                owner,
                repo,
                issue_number,
            )

        # State: FAIL
        self._trace.append(SynthesisState.FAIL)
        logger.warning("All synthesis attempts failed for %s/%s#%d", owner, repo, issue_number)
        return None

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
    ) -> tuple[DockerContext | None, dict]:
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
        )
        self._log_attempt(
            owner=owner,
            repo=repo,
            sha=sha,
            issue_number=issue_number,
            attempt_index=attempt_index,
            result=result,
        )
        ctx = result.docker_context if result.success else None
        return ctx, result.resource_metrics

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
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

        failure = result.failure_json or {}
        # Cap raw output at 100 KB for Supabase storage
        raw_output = result.raw_agent_output
        if len(raw_output) > 100_000:
            raw_output = raw_output[-100_000:]

        row = {
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "issue_number": issue_number,
            "attempt_index": attempt_index,
            "agent_name": result.agent_name,
            "success": result.success,
            "duration_s": result.duration_s,
            "failure_stage": failure.get("stage") or None,
            "failure_return_code": failure.get("return_code") or None,
            "error_message": (failure.get("error_message") or "")[-10_000:] or None,
            "agent_output": raw_output or None,
            "files_changed": json.dumps(result.files_changed),
            "resource_metrics": result.resource_metrics or None,
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

    def _save_context(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        ctx: DockerContext,
        resource_metrics: dict | None = None,
        env_payload_override: str | None = None,
    ) -> None:
        """Persist the agent-edited scripts to the ``candidate_containers`` table.

        Saves ``build_pkg_sh``, ``build_run_sh``, and ``build_env_sh``.
        When the agent also modified the env payload, ``env_payload_override``
        is persisted to the ``env_payload`` column.
        """
        if not sha:
            return
        try:
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
            client.table("candidate_containers").upsert(row).execute()
            logger.info("Saved context for %s/%s@%s", owner, repo, sha[:12])
        except Exception:
            logger.warning("Failed to save context for %s/%s@%s", owner, repo, sha[:12])


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
