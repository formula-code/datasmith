from __future__ import annotations

from typing import Any, cast

from datasmith.utils import get_client, get_logger

logger = get_logger("update.pipeline")

STAGES = ["scrape_repos", "scrape_commits", "classify_prs", "resolve_packages", "synthesize_images", "publish"]


def _build_file_change_summary(file_changes: list[dict[str, Any]] | None) -> str:
    if not file_changes:
        return ""
    lines = [
        "| File | Lines Added | Lines Removed |",
        "|------|-------------|----------------|",
    ]
    for f in file_changes:
        lines.append(f"| {f.get('filename', '')} | {f.get('additions', 0)} | {f.get('deletions', 0)} |")
    return "\n".join(lines)


def _format_description(title: str, body: str) -> str:
    parts = [p for p in (title.strip(), body.strip()) if p]
    return "\n\n".join(parts)


class Pipeline:
    """Orchestrate the full FormulaCode update pipeline."""

    def __init__(self, dry_run: bool = False, n_concurrent: int | None = None) -> None:
        self._dry_run = dry_run
        self._n_concurrent = n_concurrent
        self._completed_stages: list[str] = []

    async def run(
        self,
        start_date: str,
        end_date: str,
        resume: bool = False,
        stage: int | None = None,
    ) -> None:
        """Execute pipeline stages in order.

        Args:
            start_date: ISO date string (YYYY-MM-DD)
            end_date: ISO date string (YYYY-MM-DD)
            resume: If True, skip already-completed stages
            stage: If set, run only this stage (1-based index)
        """
        stages_to_run = STAGES

        if stage is not None:
            if stage < 1 or stage > len(STAGES):
                raise ValueError(f"Stage must be 1-{len(STAGES)}, got {stage}")
            stages_to_run = [STAGES[stage - 1]]
        elif resume:
            completed = self._get_completed_stages()
            stages_to_run = [s for s in STAGES if s not in completed]
            if not stages_to_run:
                logger.info("All stages already completed")
                return

        logger.info(
            "Running pipeline stages: %s (dry_run=%s)",
            ", ".join(stages_to_run),
            self._dry_run,
        )

        for stage_name in stages_to_run:
            if self._dry_run:
                logger.info("[DRY RUN] Would run stage: %s", stage_name)
                continue

            logger.info("Starting stage: %s", stage_name)
            try:
                await self._run_stage(stage_name, start_date, end_date)
                self._completed_stages.append(stage_name)
                self._mark_stage_completed(stage_name)
                logger.info("Completed stage: %s", stage_name)
            except Exception:
                logger.exception("Stage %s failed", stage_name)
                raise

    async def _run_stage(self, stage_name: str, start_date: str, end_date: str) -> None:
        if stage_name == "scrape_repos":
            await self._scrape_repos()
        elif stage_name == "scrape_commits":
            await self._scrape_commits(start_date, end_date)
        elif stage_name == "classify_prs":
            await self._classify_prs()
        elif stage_name == "resolve_packages":
            await self._resolve_packages(start_date, end_date)
        elif stage_name == "synthesize_images":
            await self._synthesize_images()
        elif stage_name == "publish":
            await self._publish(start_date, end_date)

    async def _scrape_repos(self) -> None:
        from datasmith.github.client import GitHubClient
        from datasmith.runners.scrape_repos import ScrapeReposRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)
        runner = ScrapeReposRunner(gh, **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}))

        # Get repos from repositories table
        client = get_client()
        resp = client.table("repositories").select("owner, repo").execute()
        items = [(r["owner"], r["repo"]) for r in resp.data]
        await runner.run(items)
        await gh.close()

    async def _scrape_commits(self, start_date: str, end_date: str) -> None:
        from datasmith.github.client import GitHubClient
        from datasmith.runners.scrape_commits import ScrapeCommitsRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)
        kwargs: dict[str, Any] = {"since": start_date, "until": end_date}
        if self._n_concurrent:
            kwargs["n_concurrent"] = self._n_concurrent
        runner = ScrapeCommitsRunner(gh, **kwargs)

        client = get_client()
        resp = client.table("repositories").select("owner, repo").execute()
        items = [(r["owner"], r["repo"]) for r in resp.data]
        await runner.run(items)
        await gh.close()

    async def _classify_prs(self) -> None:
        from datasmith.agents.classifiers import ClassifyJudge, PerfClassifier
        from datasmith.agents.config import AgentConfig, configure_dspy
        from datasmith.runners.classify_prs import ClassifyPRsRunner

        configure_dspy(AgentConfig.from_env())

        classifier = PerfClassifier()
        judge = ClassifyJudge()
        runner = ClassifyPRsRunner(
            classifier,
            judge,
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )

        client = get_client()
        resp = (
            client.table("pull_requests")
            .select("owner, repo, issue_number, title, body, patch, file_changes")
            .eq("is_performance_commit_symbolic", True)
            .is_("is_performance_commit", "null")
            .execute()
        )
        rows = cast(list[dict[str, Any]], resp.data)
        items = [
            {
                "owner": r["owner"],
                "repo": r["repo"],
                "issue_number": r["issue_number"],
                "description": _format_description(r.get("title", ""), r.get("body", "")),
                "patch": r.get("patch", ""),
                "file_change_summary": _build_file_change_summary(r.get("file_changes")),
            }
            for r in rows
        ]
        await runner.run(items)

    async def _resolve_packages(self, start_date: str, end_date: str) -> None:
        from datasmith.runners.resolve_packages import ResolvePackagesRunner

        runner = ResolvePackagesRunner(
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )

        client = get_client()
        # Get performance-classified PRs within the date range
        resp = (
            client.table("pull_requests")
            .select("owner, repo, merge_commit_sha")
            .eq("is_performance_commit", True)
            .gte("created_at", start_date)
            .lte("created_at", end_date)
            .execute()
        )
        rows = cast(list[dict[str, Any]], resp.data)

        # Deduplicate by (owner, repo, sha) — multiple PRs may share the same commit
        seen: set[tuple[str, str, str]] = set()
        items: list[dict[str, Any]] = []
        for r in rows:
            sha = r.get("merge_commit_sha", "")
            if not sha:
                continue
            key = (r["owner"], r["repo"], sha)
            if key in seen:
                continue
            seen.add(key)
            items.append({"owner": r["owner"], "repo": r["repo"], "sha": sha})

        # Skip items already in the packages table
        if items:
            existing_resp = client.table("packages").select("owner, repo, sha").execute()
            existing_keys = {(e["owner"], e["repo"], e["sha"]) for e in existing_resp.data}
            items = [it for it in items if (it["owner"], it["repo"], it["sha"]) not in existing_keys]

        logger.info("Resolving packages for %d commits", len(items))
        await runner.run(items)

    async def _synthesize_images(self) -> None:
        from datasmith.agents.synthesizer import Synthesizer
        from datasmith.docker.verifiers import MultiObjVerifier, SmokeVerifier
        from datasmith.github.client import GitHubClient
        from datasmith.runners.synthesize_images import SynthesizeImagesRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)

        synth = Synthesizer()
        verifier = MultiObjVerifier(verifiers=[SmokeVerifier("test")])
        runner = SynthesizeImagesRunner(
            synth,
            verifier,
            gh=gh,
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )

        client = get_client()
        resp = (
            client.table("pull_requests")
            .select("owner, repo, issue_number, merge_commit_sha, title, body, created_at, rendered_problem")
            .eq("is_performance_commit", True)
            .is_("container_name", "null")
            .execute()
        )
        rows = cast(list[dict[str, Any]], resp.data)

        # Join with packages table for env_payload and python_version
        pkg_resp = (
            client.table("packages")
            .select("owner, repo, sha, env_payload, python_version")
            .eq("can_install", True)
            .execute()
        )
        pkg_lookup: dict[tuple[str, str, str], dict[str, Any]] = {
            (p["owner"], p["repo"], p["sha"]): p for p in cast(list[dict[str, Any]], pkg_resp.data)
        }

        # Batch-fetch repo descriptions for rendering
        repo_keys = {(r["owner"], r["repo"]) for r in rows}
        repo_descriptions: dict[tuple[str, str], str] = {}
        if repo_keys:
            desc_resp = client.table("repositories").select("owner, repo, description").execute()
            for rd in cast(list[dict[str, Any]], desc_resp.data):
                key = (rd["owner"], rd["repo"])
                if key in repo_keys:
                    repo_descriptions[key] = rd.get("description") or ""

        items = []
        for r in rows:
            sha = r.get("merge_commit_sha", "")
            pkg = pkg_lookup.get((r["owner"], r["repo"], sha), {})
            # Skip PRs without resolved packages
            if not pkg:
                logger.debug(
                    "Skipping %s/%s#%d: no resolved packages for sha %s",
                    r["owner"],
                    r["repo"],
                    r["issue_number"],
                    sha[:8] if sha else "?",
                )
                continue
            items.append({
                "owner": r["owner"],
                "repo": r["repo"],
                "issue_number": r["issue_number"],
                "sha": sha,
                "title": r.get("title", ""),
                "body": r.get("body", ""),
                "created_at": r.get("created_at"),
                "pr_context": r.get("rendered_problem") or r.get("body", ""),
                "repo_description": repo_descriptions.get((r["owner"], r["repo"]), ""),
                "env_payload": pkg.get("env_payload", ""),
                "python_version": pkg.get("python_version", ""),
            })
        logger.info("Synthesizing images for %d PRs", len(items))
        await runner.run(items)
        await gh.close()

    async def _publish(self, start_date: str, end_date: str) -> None:
        from datasmith.publish.pipeline import publish_pipeline

        await publish_pipeline(start_date, end_date)

    def _get_completed_stages(self) -> list[str]:
        try:
            client = get_client()
            resp = client.table("runner_progress").select("runner_name, completed, total").execute()
            completed = []
            for r in resp.data:
                if r["total"] > 0 and r["completed"] >= r["total"]:
                    completed.append(r["runner_name"])
        except Exception:
            return []
        else:
            return completed

    def _mark_stage_completed(self, stage_name: str) -> None:
        try:
            client = get_client()
            client.table("runner_progress").upsert({
                "runner_id": f"pipeline-{stage_name}",
                "runner_name": stage_name,
                "total": 1,
                "completed": 1,
                "failed": 0,
            }).execute()
        except Exception:
            logger.warning("Failed to mark stage %s as completed", stage_name)
