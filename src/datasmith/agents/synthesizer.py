from __future__ import annotations

import enum
from typing import Any, cast

from datasmith.docker.context import DockerContext
from datasmith.docker.verifiers import Verifier, VerifyResult
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
    ) -> None:
        self._max_attempts = max_attempts
        self._dry_run = dry_run
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
        verifier: Verifier,
        sha: str = "",
        base_context: DockerContext | None = None,
        env_payload: str = "",
        python_version: str = "",
    ) -> DockerContext | None:
        """Run the synthesis state machine. Returns DockerContext on success, None on failure."""
        self._trace = []

        # State: CHECK_CACHE
        self._trace.append(SynthesisState.CHECK_CACHE)
        cached = self._check_cache(owner, repo, sha)
        if cached is not None:
            logger.info("Cache hit for %s/%s@%s", owner, repo, sha[:12] if sha else "?")
            return cached

        # State: FIND_SIMILAR
        self._trace.append(SynthesisState.FIND_SIMILAR)
        similar_scripts = self._find_similar(owner, repo)

        # State: TRY_SIMILAR
        if similar_scripts:
            self._trace.append(SynthesisState.TRY_SIMILAR)
            for script in similar_scripts:
                ctx: DockerContext | None = DockerContext(build_pkg_sh=script)
                image_name = f"formulacode/{owner}-{repo}:{issue_number}-test"
                result = verifier.verify(image_name)
                if result.ok:
                    logger.info("Similar script passed for %s/%s#%d", owner, repo, issue_number)
                    self._save_attempt(owner, repo, issue_number, sha, 0, script, result)
                    self._save_context(owner, repo, sha, issue_number, ctx)
                    return ctx

        # State: LLM_GENERATE (sandbox-based)
        self._trace.append(SynthesisState.LLM_GENERATE)
        for attempt_idx in range(self._max_attempts):
            ctx = self._sandbox_generate(
                owner=owner,
                repo=repo,
                sha=sha,
                pr_context=pr_context,
                base_context=base_context or DockerContext(),
                env_payload=env_payload,
                python_version=python_version,
            )
            if ctx is not None:
                logger.info(
                    "Sandbox synthesis succeeded for %s/%s#%d (attempt %d)",
                    owner,
                    repo,
                    issue_number,
                    attempt_idx + 1,
                )
                self._save_context(owner, repo, sha, issue_number, ctx)
                return ctx
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
                client.table("docker_contexts").select("*").eq("owner", owner).eq("repo", repo).eq("sha", sha).execute()
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

    def _find_similar(self, owner: str, repo: str) -> list[str]:
        try:
            client = get_client()
            resp = (
                client.table("build_attempts")
                .select("script")
                .eq("owner", owner)
                .eq("repo", repo)
                .eq("ok", True)
                .limit(5)
                .execute()
            )
            rows = cast(list[dict[str, Any]], resp.data)
            return [r["script"] for r in rows if r.get("script")]
        except Exception:
            logger.debug("Similar script lookup failed")
            return []

    def _sandbox_generate(
        self,
        owner: str,
        repo: str,
        sha: str,
        pr_context: str,
        base_context: DockerContext,
        env_payload: str,
        python_version: str,
    ) -> DockerContext | None:
        from datasmith.agents.sandbox import SandboxRunner

        runner = SandboxRunner()
        result = runner.run(
            owner=owner,
            repo=repo,
            sha=sha,
            base_context=base_context,
            env_payload=env_payload,
            python_version=python_version,
            pr_context=pr_context,
            dry_run=self._dry_run,
        )
        return result.docker_context if result.success else None

    def _save_context(
        self,
        owner: str,
        repo: str,
        sha: str,
        issue_number: int,
        ctx: DockerContext,
    ) -> None:
        """Persist the full DockerContext to the ``docker_contexts`` table."""
        if not sha:
            return
        try:
            client = get_client()
            client.table("docker_contexts").upsert({
                "owner": owner,
                "repo": repo,
                "sha": sha,
                "issue_number": issue_number,
                "dockerfile": ctx.dockerfile,
                "build_base_sh": ctx.build_base_sh,
                "build_env_sh": ctx.build_env_sh,
                "build_pkg_sh": ctx.build_pkg_sh,
                "build_run_sh": ctx.build_run_sh,
                "build_final_sh": ctx.build_final_sh,
                "profile_sh": ctx.profile_sh,
                "run_tests_sh": ctx.run_tests_sh,
                "entrypoint_sh": ctx.entrypoint_sh,
            }).execute()
            logger.info("Saved context for %s/%s@%s", owner, repo, sha[:12])
        except Exception:
            logger.warning("Failed to save context for %s/%s@%s", owner, repo, sha[:12])

    def _save_attempt(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        sha: str,
        attempt_idx: int,
        script: str,
        result: VerifyResult,
    ) -> None:
        try:
            client = get_client()
            row: dict[str, Any] = {
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "attempt_idx": attempt_idx,
                "script": script,
                "ok": result.ok,
                "rc": result.rc,
                "duration_s": result.duration_s,
                "stderr_tail": result.stderr[-2000:] if result.stderr else "",
                "stdout_tail": result.stdout[-2000:] if result.stdout else "",
            }
            if sha:
                row["sha"] = sha
            client.table("build_attempts").insert(row).execute()
        except Exception:
            logger.warning("Failed to save build attempt")
