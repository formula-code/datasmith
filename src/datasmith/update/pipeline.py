from __future__ import annotations

from typing import Any

from datasmith.utils import get_client, get_logger
from datasmith.utils.db import fetch_all

logger = get_logger("update.pipeline")


def _cap_per_repo(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Return at most *limit* randomly-sampled items per (owner, repo)."""
    import random
    from collections import defaultdict

    by_repo: defaultdict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for it in items:
        by_repo[(it["owner"], it["repo"])].append(it)

    capped: list[dict[str, Any]] = []
    for group in by_repo.values():
        capped.extend(random.sample(group, min(limit, len(group))))
    logger.info("Capped to %d tasks (%d per repo) from %d total", len(capped), limit, len(items))
    return capped


def _fetch_repo_descriptions(rows: list[dict[str, Any]]) -> dict[tuple[str, str], str]:
    """Batch-fetch repo descriptions for a set of rows with owner/repo keys."""
    repo_keys = {(r["owner"], r["repo"]) for r in rows}
    descriptions: dict[tuple[str, str], str] = {}
    if repo_keys:
        desc_rows = fetch_all("repositories", select="owner, repo, description")
        for rd in desc_rows:
            key = (rd["owner"], rd["repo"])
            if key in repo_keys:
                descriptions[key] = rd.get("description") or ""
    return descriptions


STAGES = [
    "scrape_repos",
    "scrape_commits",
    "classify_prs",
    "resolve_packages",
    "render_problems",
    "synthesize_images",
    "publish",
]


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

    def __init__(
        self,
        dry_run: bool = False,
        n_concurrent: int | None = None,
        tasks_per_repo: int | None = None,
        agent: str | None = None,
        force: bool = False,
        offline_source: str | None = None,
        min_stars: int = 500,
    ) -> None:
        self._dry_run = dry_run
        self._n_concurrent = n_concurrent
        self._tasks_per_repo = tasks_per_repo
        self._agent = agent
        self._force = force
        self._offline_source = offline_source
        self._min_stars = min_stars
        self._completed_stages: list[str] = []

    async def run(
        self,
        start_date: str,
        end_date: str,
        resume: bool = False,
        stage: int | list[int] | None = None,
    ) -> None:
        """Execute pipeline stages in order.

        Args:
            start_date: ISO date string (YYYY-MM-DD)
            end_date: ISO date string (YYYY-MM-DD)
            resume: If True, skip already-completed stages
            stage: If set, run only these stages (1-based indices)
        """
        stages_to_run = STAGES

        if stage is not None:
            indices = [stage] if isinstance(stage, int) else stage
            for s in indices:
                if s < 1 or s > len(STAGES):
                    raise ValueError(f"Stage must be 1-{len(STAGES)}, got {s}")
            stages_to_run = [STAGES[s - 1] for s in sorted(indices)]
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
        elif stage_name == "render_problems":
            await self._render_problems()
        elif stage_name == "synthesize_images":
            await self._synthesize_images()
        elif stage_name == "publish":
            await self._publish(start_date, end_date)

    async def _scrape_repos(self) -> None:
        from datasmith.github.client import GitHubClient
        from datasmith.github.search import search_repos_by_file
        from datasmith.runners.scrape_repos import ScrapeReposRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)
        runner = ScrapeReposRunner(gh, **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}))

        seen: set[tuple[str, str]] = set()
        items: list[tuple[str, str]] = []

        # 1. Discover repos via GitHub code search
        discovered = await search_repos_by_file(gh, filename="asv.conf.json", min_stars=self._min_stars)
        for pair in discovered:
            if pair not in seen:
                seen.add(pair)
                items.append(pair)

        # 2. Import repos from offline source (parquet) if provided
        if self._offline_source:
            from datasmith.update.offline import load_offline_repo_names

            for pair in load_offline_repo_names(self._offline_source):
                if pair not in seen:
                    seen.add(pair)
                    items.append(pair)
            logger.info(
                "Imported repos from offline source: %d new (total %d)",
                len(items) - len(discovered),
                len(items),
            )

        # 3. Also include repos already in the DB (metadata refresh)
        rows = fetch_all("repositories", select="owner, repo")
        for r in rows:
            pair = (r["owner"], r["repo"])
            if pair not in seen:
                seen.add(pair)
                items.append(pair)

        logger.info("Total repos to process: %d", len(items))
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

        rows = fetch_all("repositories", select="owner, repo")
        items = [(r["owner"], r["repo"]) for r in rows]
        await runner.run(items)
        await gh.close()

        # Bulk-import from offline source (parquet) if provided
        if self._offline_source:
            from datasmith.update.offline import load_offline_pull_requests
            from datasmith.utils.db import batch_upsert

            records = load_offline_pull_requests(self._offline_source, start_date, end_date)
            n = batch_upsert("pull_requests", records)
            logger.info("Imported %d pull request records from offline source", n)

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

        classify_kwargs: dict[str, Any] = {
            "select": "owner, repo, issue_number, title, body, patch, file_changes",
            "filters": {"is_performance_commit_symbolic": True},
        }
        if not self._force:
            classify_kwargs["is_null"] = ["is_performance_commit"]
        rows = fetch_all("pull_requests", **classify_kwargs)
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

        # Get performance-classified PRs within the date range
        rows = fetch_all(
            "pull_requests",
            select="owner, repo, merge_commit_sha",
            filters={"is_performance_commit": True},
            gte_filters={"created_at": start_date},
            lte_filters={"created_at": end_date},
        )

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

        # Skip items already in the packages table (unless --force)
        if items and not self._force:
            existing_rows = fetch_all("packages", select="owner, repo, sha")
            existing_keys = {(e["owner"], e["repo"], e["sha"]) for e in existing_rows}
            items = [it for it in items if (it["owner"], it["repo"], it["sha"]) not in existing_keys]

        logger.info("Resolving packages for %d commits", len(items))
        await runner.run(items)

    async def _render_problems(self) -> None:
        from datasmith.agents.config import AgentConfig, configure_dspy
        from datasmith.github.client import GitHubClient
        from datasmith.runners.render_problems import RenderProblemsRunner
        from datasmith.utils.tokens import TokenPool

        configure_dspy(AgentConfig.from_env())

        pool = TokenPool()
        gh = GitHubClient(pool)
        runner = RenderProblemsRunner(
            gh=gh,
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )

        # Fetch performance-classified PRs
        rows = fetch_all(
            "pull_requests",
            select="owner, repo, issue_number, merge_commit_sha, title, body, created_at",
            filters={"is_performance_commit": True, "is_performance_commit_symbolic": True},
            neq_filters={"merge_commit_sha": ""},
        )

        # Only process PRs whose commit has can_install=True resolved packages
        pkg_rows = fetch_all(
            "packages",
            select="owner, repo, sha",
            filters={"can_install": True},
        )
        installable: set[tuple[str, str, str]] = {(p["owner"], p["repo"], p["sha"]) for p in pkg_rows}

        repo_descriptions = _fetch_repo_descriptions(rows)

        # Skip PRs already processed (have a pr_contexts row) unless --force
        existing_keys: set[tuple[str, str, int]] = set()
        if not self._force:
            existing_rows = fetch_all("pr_contexts", select="owner, repo, issue_number")
            existing_keys = {(e["owner"], e["repo"], e["issue_number"]) for e in existing_rows}

        items = []
        for r in rows:
            sha = r.get("merge_commit_sha", "")
            if not sha:
                continue
            if (r["owner"], r["repo"], sha) not in installable:
                logger.debug(
                    "Skipping %s/%s#%d: no can_install package for sha %s",
                    r["owner"],
                    r["repo"],
                    r["issue_number"],
                    sha[:8],
                )
                continue
            if (r["owner"], r["repo"], r["issue_number"]) in existing_keys:
                continue
            items.append({
                "owner": r["owner"],
                "repo": r["repo"],
                "issue_number": r["issue_number"],
                "merge_commit_sha": sha,
                "title": r.get("title", ""),
                "body": r.get("body", ""),
                "created_at": r.get("created_at"),
                "repo_description": repo_descriptions.get((r["owner"], r["repo"]), ""),
            })

        if self._tasks_per_repo is not None:
            items = _cap_per_repo(items, self._tasks_per_repo)

        logger.info("Rendering problem contexts for %d PRs", len(items))
        await runner.run(items)
        await gh.close()

    async def _synthesize_images(self) -> None:
        from datasmith.agents.synthesizer import Synthesizer
        from datasmith.github.client import GitHubClient
        from datasmith.runners.synthesize_images import SynthesizeImagesRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)

        synth = Synthesizer(agent=self._agent, force=self._force)
        runner = SynthesizeImagesRunner(
            synth,
            gh=gh,
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )

        query_kwargs: dict[str, Any] = {
            "select": "owner, repo, issue_number, merge_commit_sha, title, body, created_at, rendered_problem",
            "filters": {"is_performance_commit": True, "is_performance_commit_symbolic": True},
            "neq_filters": {"merge_commit_sha": ""},
        }
        if not self._force:
            query_kwargs["is_null"] = ["container_name"]
        rows = fetch_all("pull_requests", **query_kwargs)

        # Join with packages table for env_payload and python_version
        pkg_rows = fetch_all(
            "packages",
            select="owner, repo, sha, env_payload, python_version",
            filters={"can_install": True},
        )
        pkg_lookup: dict[tuple[str, str, str], dict[str, Any]] = {
            (p["owner"], p["repo"], p["sha"]): p for p in pkg_rows
        }

        repo_descriptions = _fetch_repo_descriptions(rows)

        # Only synthesize PRs that have a rendered context with linked issues
        # and a non-empty extracted problem statement (from stage 5).
        ctx_rows = fetch_all(
            "pr_contexts",
            select="owner, repo, issue_number, issues_json, initial_observations",
        )
        eligible_prs: set[tuple[str, str, int]] = {
            (c["owner"], c["repo"], c["issue_number"])
            for c in ctx_rows
            if c.get("issues_json") or c.get("initial_observations")
        }

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
            # Skip PRs without a rendered context (non-empty issues + observations)
            if (r["owner"], r["repo"], r["issue_number"]) not in eligible_prs:
                logger.debug(
                    "Skipping %s/%s#%d: no eligible pr_context (empty issues_json or initial_observations)",
                    r["owner"],
                    r["repo"],
                    r["issue_number"],
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
        if self._tasks_per_repo is not None:
            items = _cap_per_repo(items, self._tasks_per_repo)

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
