from __future__ import annotations

import enum

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
        max_attempts: int = 3,
        models: list[str] | None = None,
    ) -> None:
        self._max_attempts = max_attempts
        self._models = models or ["gpt-oss-120b"]
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
    ) -> DockerContext | None:
        """Run the synthesis state machine. Returns DockerContext on success, None on failure."""
        self._trace = []

        # State: CHECK_CACHE
        self._trace.append(SynthesisState.CHECK_CACHE)
        cached = self._check_cache(owner, repo, issue_number)
        if cached is not None:
            logger.info("Cache hit for %s/%s#%d", owner, repo, issue_number)
            return cached

        # State: FIND_SIMILAR
        self._trace.append(SynthesisState.FIND_SIMILAR)
        similar_scripts = self._find_similar(owner, repo)

        # State: TRY_SIMILAR
        if similar_scripts:
            self._trace.append(SynthesisState.TRY_SIMILAR)
            for script in similar_scripts:
                ctx = DockerContext(build_pkg_sh=script)
                image_name = f"formulacode/{owner}-{repo}:{issue_number}-test"
                result = verifier.verify(image_name)
                if result.ok:
                    logger.info("Similar script passed for %s/%s#%d", owner, repo, issue_number)
                    self._save_attempt(owner, repo, issue_number, 0, "similar", script, result)
                    return ctx

        # State: LLM_GENERATE
        self._trace.append(SynthesisState.LLM_GENERATE)
        for attempt_idx, model in enumerate(self._models[: self._max_attempts]):
            ctx = self._llm_generate(pr_context, model)
            if ctx is not None:
                image_name = f"formulacode/{owner}-{repo}:{issue_number}-test"
                result = verifier.verify(image_name)
                self._save_attempt(owner, repo, issue_number, attempt_idx + 1, model, ctx.build_pkg_sh, result)
                if result.ok:
                    logger.info("LLM-generated script passed for %s/%s#%d (model=%s)", owner, repo, issue_number, model)
                    return ctx

        # State: FAIL
        self._trace.append(SynthesisState.FAIL)
        logger.warning("All synthesis attempts failed for %s/%s#%d", owner, repo, issue_number)
        return None

    def _check_cache(self, owner: str, repo: str, issue_number: int) -> DockerContext | None:
        try:
            client = get_client()
            resp = (
                client.table("hook_cache")
                .select("result_json")
                .eq("entity_key", f"{owner}/{repo}:{issue_number}")
                .eq("hook_name", "synthesize")
                .execute()
            )
            if resp.data:
                data = resp.data[0]["result_json"]
                return DockerContext(**data)
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
            return [r["script"] for r in resp.data if r.get("script")]
        except Exception:
            logger.debug("Similar script lookup failed")
            return []

    def _llm_generate(self, pr_context: str, model: str) -> DockerContext | None:
        from datasmith.agents.codex import codex_exec

        prompt = f"Generate a Docker build script (build_pkg.sh) for this PR:\n\n{pr_context}"
        result = codex_exec(prompt, model=model, timeout=300)
        if result.success and result.output:
            return DockerContext(build_pkg_sh=result.output)
        return None

    def _save_attempt(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        attempt_idx: int,
        model: str,
        script: str,
        result: VerifyResult,
    ) -> None:
        try:
            client = get_client()
            client.table("build_attempts").insert({
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "attempt_idx": attempt_idx,
                "model": model,
                "script": script,
                "ok": result.ok,
                "rc": result.rc,
                "duration_s": result.duration_s,
                "stderr_tail": result.stderr[-2000:] if result.stderr else "",
                "stdout_tail": result.stdout[-2000:] if result.stdout else "",
            }).execute()
        except Exception:
            logger.warning("Failed to save build attempt")
