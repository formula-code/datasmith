from __future__ import annotations

from collections import Counter
from typing import Any

from datasmith.utils import get_client, get_logger
from datasmith.utils.db import fetch_all

logger = get_logger("update.pipeline")


def _parse_task_specs(specs: str) -> set[tuple[str, str, int]]:
    """Parse a --tasks value into a set of (owner, repo, pr_number) tuples.

    Accepts comma-separated specs in either ``owner/repo#N`` or
    ``owner/repo/N`` form. Invalid specs raise ``ValueError``.
    """
    out: set[tuple[str, str, int]] = set()
    for raw in specs.split(","):
        spec = raw.strip()
        if not spec:
            continue
        if "#" in spec:
            path, _, pr = spec.partition("#")
        else:
            parts = spec.rsplit("/", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid --tasks spec '{spec}' (expected owner/repo#N)")
            path, pr = parts
        if "/" not in path:
            raise ValueError(f"Invalid --tasks spec '{spec}' (missing owner/repo)")
        owner, repo = path.split("/", 1)
        try:
            pr_number = int(pr)
        except ValueError as exc:
            raise ValueError(f"Invalid PR number in --tasks spec '{spec}'") from exc
        out.add((owner, repo, pr_number))
    return out


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


def order_by_probe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order rows best-probe-first, keeping every one of them.

    ``can_install`` used to filter here. It blocked 3,217 performance PRs that
    were then never attempted — and it passed h5py on a single dependency while
    failing apache/arrow on a corrupted marker. Stage 6 is the only stage that
    can answer whether a task builds, so this decides order, not eligibility.

    ``sorted`` is stable, so rows sharing a status keep their incoming order.
    """
    from datasmith.resolution.probe import PROBE_RANK

    unknown = max(PROBE_RANK.values()) + 1
    return sorted(rows, key=lambda r: PROBE_RANK.get(r.get("probe_status") or "", unknown))


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
    "harbor_healthcheck",
    "publish",
    "scrape_benchmark_source",
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
        harbor_use_daytona: bool = False,
        harbor_rounds: int = 2,
        harbor_limit: int | None = None,
        harbor_tasks: str | None = None,
    ) -> None:
        self._dry_run = dry_run
        self._n_concurrent = n_concurrent
        self._tasks_per_repo = tasks_per_repo
        self._agent = agent
        self._force = force
        self._offline_source = offline_source
        self._min_stars = min_stars
        self._harbor_use_daytona = harbor_use_daytona
        self._harbor_rounds = harbor_rounds
        self._harbor_limit = harbor_limit
        self._harbor_tasks = _parse_task_specs(harbor_tasks) if harbor_tasks else None
        self._completed_stages: list[str] = []

    def _log_dry_run_summary(
        self,
        stage_name: str,
        items: list[Any],
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Log an informative dry-run summary for a stage."""
        lines = [f"[DRY RUN] Stage: {stage_name}", f"  Items to process: {len(items)}"]

        if not items:
            lines.append("  Nothing to do.")
            logger.info("\n".join(lines))
            return

        lines.extend(self._repo_breakdown(items))
        lines.extend(self._settings_lines())

        for k, v in (extra or {}).items():
            lines.append(f"  {k}: {v}")

        logger.info("\n".join(lines))

    def _repo_breakdown(self, items: list[Any]) -> list[str]:
        repo_counts: Counter[tuple[str, str]] = Counter()
        for it in items:
            if isinstance(it, dict):
                repo_counts[(it["owner"], it["repo"])] += 1
            elif isinstance(it, tuple) and len(it) >= 2:
                repo_counts[(it[0], it[1])] += 1

        lines = [f"  Unique repos: {len(repo_counts)}"]
        top = repo_counts.most_common(10)
        if top:
            lines.append("  Top repos:")
            for (owner, repo), count in top:
                lines.append(f"    {owner}/{repo}: {count}")
            if len(repo_counts) > 10:
                lines.append(f"    ... and {len(repo_counts) - 10} more")
        return lines

    def _settings_lines(self) -> list[str]:
        settings: list[str] = []
        if self._n_concurrent:
            settings.append(f"concurrency={self._n_concurrent}")
        if self._force:
            settings.append("force=True")
        if self._tasks_per_repo:
            settings.append(f"tasks_per_repo={self._tasks_per_repo}")
        return [f"  Settings: {', '.join(settings)}"] if settings else []

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
                logger.info("[DRY RUN] Collecting summary for stage: %s", stage_name)
            else:
                logger.info("Starting stage: %s", stage_name)
            try:
                await self._run_stage(stage_name, start_date, end_date)
                if not self._dry_run:
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
            await self._classify_prs(start_date, end_date)
        elif stage_name == "resolve_packages":
            await self._resolve_packages(start_date, end_date)
        elif stage_name == "render_problems":
            await self._render_problems(start_date, end_date)
        elif stage_name == "synthesize_images":
            await self._synthesize_images(start_date, end_date)
        elif stage_name == "harbor_healthcheck":
            await self._harbor_healthcheck(start_date, end_date)
        elif stage_name == "publish":
            await self._publish(start_date, end_date)
        elif stage_name == "scrape_benchmark_source":
            await self._scrape_benchmark_source(start_date, end_date)

    async def _scrape_repos(self) -> None:
        from datasmith.github.client import GitHubClient
        from datasmith.github.search import search_repos_by_file
        from datasmith.runners.scrape_repos import ScrapeReposRunner
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)

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

        if self._dry_run:
            self._log_dry_run_summary(
                "scrape_repos",
                items,
                extra={"From GitHub search": len(discovered), "From DB (refresh)": len(rows)},
            )
            await gh.close()
            return

        runner = ScrapeReposRunner(gh, **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}))
        await runner.run(items)
        await gh.close()

    async def _scrape_commits(self, start_date: str, end_date: str) -> None:
        from datasmith.github.client import GitHubClient
        from datasmith.runners.scrape_commits import ScrapeCommitsRunner
        from datasmith.utils.tokens import TokenPool

        rows = fetch_all("repositories", select="owner, repo")
        items = [(r["owner"], r["repo"]) for r in rows]

        if self._dry_run:
            self._log_dry_run_summary(
                "scrape_commits",
                items,
                extra={"Date range": f"{start_date} to {end_date}"},
            )
            return

        pool = TokenPool()
        gh = GitHubClient(pool)
        kwargs: dict[str, Any] = {"since": start_date, "until": end_date}
        if self._n_concurrent:
            kwargs["n_concurrent"] = self._n_concurrent
        runner = ScrapeCommitsRunner(gh, **kwargs)

        await runner.run(items)
        await gh.close()

        # Bulk-import from offline source (parquet) if provided
        if self._offline_source:
            from datasmith.update.offline import load_offline_pull_requests
            from datasmith.utils.db import batch_upsert

            records = load_offline_pull_requests(self._offline_source, start_date, end_date)
            n = batch_upsert("pull_requests", records)
            logger.info("Imported %d pull request records from offline source", n)

    async def _classify_prs(self, start_date: str, end_date: str) -> None:
        classify_kwargs: dict[str, Any] = {
            "select": "owner, repo, issue_number, title, body, patch, file_changes",
            "filters": {"is_performance_commit_symbolic": True},
            "gte_filters": {"created_at": start_date},
            "lte_filters": {"created_at": end_date},
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

        if self._dry_run:
            self._log_dry_run_summary(
                "classify_prs",
                items,
                extra={
                    "Date range": f"{start_date} to {end_date}",
                    "Filter": "unclassified only" if not self._force else "all (force=True)",
                },
            )
            return

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
        await runner.run(items)

    async def _resolve_packages(self, start_date: str, end_date: str) -> None:
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
        skipped = 0
        if items and not self._force:
            existing_rows = fetch_all("packages", select="owner, repo, sha")
            existing_keys = {(e["owner"], e["repo"], e["sha"]) for e in existing_rows}
            before = len(items)
            items = [it for it in items if (it["owner"], it["repo"], it["sha"]) not in existing_keys]
            skipped = before - len(items)

        logger.info("Resolving packages for %d commits", len(items))

        if self._dry_run:
            extra: dict[str, Any] = {
                "Date range": f"{start_date} to {end_date}",
                "Unique commits from PRs": len(seen),
            }
            if skipped:
                extra["Already resolved (skipped)"] = skipped
            self._log_dry_run_summary("resolve_packages", items, extra=extra)
            return

        from datasmith.runners.resolve_packages import ResolvePackagesRunner

        runner = ResolvePackagesRunner(
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )
        await runner.run(items)

    async def _render_problems(self, start_date: str, end_date: str) -> None:
        # Fetch performance-classified PRs
        rows = fetch_all(
            "pull_requests",
            select="owner, repo, issue_number, merge_commit_sha, title, body, created_at",
            filters={"is_performance_commit": True, "is_performance_commit_symbolic": True},
            neq_filters={"merge_commit_sha": ""},
            gte_filters={"created_at": start_date},
            lte_filters={"created_at": end_date},
        )

        # Every PR with a resolved package is eligible. The probe says who runs
        # first, not who runs at all — stage 6 is the only stage that can answer
        # whether a task builds.
        pkg_rows = fetch_all(
            "packages",
            select="owner, repo, sha, probe_status",
        )
        probe_by_commit: dict[tuple[str, str, str], str | None] = {
            (p["owner"], p["repo"], p["sha"]): p.get("probe_status") for p in pkg_rows
        }

        repo_descriptions = _fetch_repo_descriptions(rows)

        # Skip PRs already processed (have a candidate_prs row) unless --force
        existing_keys: set[tuple[str, str, int]] = set()
        if not self._force:
            existing_rows = fetch_all("candidate_prs", select="owner, repo, issue_number")
            existing_keys = {(e["owner"], e["repo"], e["issue_number"]) for e in existing_rows}

        skipped_no_pkg = 0
        skipped_existing = 0
        items = []
        for r in rows:
            sha = r.get("merge_commit_sha", "")
            if not sha:
                continue
            if (r["owner"], r["repo"], sha) not in probe_by_commit:
                skipped_no_pkg += 1
                logger.debug(
                    "Skipping %s/%s#%d: no resolved package for sha %s",
                    r["owner"],
                    r["repo"],
                    r["issue_number"],
                    sha[:8],
                )
                continue
            if (r["owner"], r["repo"], r["issue_number"]) in existing_keys:
                skipped_existing += 1
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
                "probe_status": probe_by_commit.get((r["owner"], r["repo"], sha)),
            })

        if self._tasks_per_repo is not None:
            items = _cap_per_repo(items, self._tasks_per_repo)
        # After the cap, not before: ``_cap_per_repo`` samples at random, so it
        # neither reads nor preserves an incoming order. Ordering here at least
        # runs the surest seeds first within whatever the cap kept.
        items = order_by_probe(items)

        logger.info("Rendering problem contexts for %d PRs", len(items))

        if self._dry_run:
            extra: dict[str, Any] = {
                "Date range": f"{start_date} to {end_date}",
                "Performance PRs in DB": len(rows),
            }
            if skipped_no_pkg:
                extra["Skipped (no installable package)"] = skipped_no_pkg
            if skipped_existing:
                extra["Skipped (already rendered)"] = skipped_existing
            self._log_dry_run_summary("render_problems", items, extra=extra)
            return

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
        await runner.run(items)
        await gh.close()

    async def _synthesize_images(self, start_date: str, end_date: str) -> None:
        query_kwargs: dict[str, Any] = {
            "select": "owner, repo, issue_number, merge_commit_sha, base_sha, title, body, created_at, rendered_problem",
            "filters": {"is_performance_commit": True, "is_performance_commit_symbolic": True},
            "neq_filters": {"merge_commit_sha": ""},
            "gte_filters": {"created_at": start_date},
            "lte_filters": {"created_at": end_date},
        }
        if not self._force:
            query_kwargs["is_null"] = ["container_name"]
        rows = fetch_all("pull_requests", **query_kwargs)

        # Join with packages table for env_payload and python_version. Every
        # resolved commit joins; the probe orders the queue rather than trimming
        # it, because this stage is the one that finds out whether it builds.
        pkg_rows = fetch_all(
            "packages",
            select="owner, repo, sha, env_payload, python_version, probe_status, primary_root",
        )
        pkg_lookup: dict[tuple[str, str, str], dict[str, Any]] = {
            (p["owner"], p["repo"], p["sha"]): p for p in pkg_rows
        }

        repo_descriptions = _fetch_repo_descriptions(rows)

        # Only synthesize PRs that have a rendered context with linked issues
        # and a non-empty extracted problem statement (from stage 5).
        ctx_rows = fetch_all(
            "candidate_prs",
            select="owner, repo, issue_number, issues_json, initial_observations",
        )
        eligible_prs: set[tuple[str, str, int]] = {
            (c["owner"], c["repo"], c["issue_number"])
            for c in ctx_rows
            if c.get("issues_json") or c.get("initial_observations")
        }

        skipped_no_pkg = 0
        skipped_no_ctx = 0
        items = []
        for r in rows:
            sha = r.get("merge_commit_sha", "")
            pkg = pkg_lookup.get((r["owner"], r["repo"], sha), {})
            # Skip PRs without resolved packages
            if not pkg:
                skipped_no_pkg += 1
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
                skipped_no_ctx += 1
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
                "base_sha": r.get("base_sha", ""),
                "title": r.get("title", ""),
                "body": r.get("body", ""),
                "created_at": r.get("created_at"),
                "pr_context": r.get("rendered_problem") or r.get("body", ""),
                "repo_description": repo_descriptions.get((r["owner"], r["repo"]), ""),
                "env_payload": pkg.get("env_payload", ""),
                "python_version": pkg.get("python_version", ""),
                # The package root inside the repository. Discovered by stage 4
                # and, until now, discarded: Dockerfile.repo hardcoded the
                # repository root, so arrow built in the wrong directory.
                "primary_root": pkg.get("primary_root") or ".",
                "probe_status": pkg.get("probe_status"),
            })
        if self._tasks_per_repo is not None:
            items = _cap_per_repo(items, self._tasks_per_repo)
        # After the cap: ``_cap_per_repo`` samples at random and discards order.
        items = order_by_probe(items)

        logger.info("Synthesizing images for %d PRs", len(items))

        if self._dry_run:
            extra: dict[str, Any] = {
                "Date range": f"{start_date} to {end_date}",
                "Candidate PRs in DB": len(rows),
            }
            if skipped_no_pkg:
                extra["Skipped (no resolved packages)"] = skipped_no_pkg
            if skipped_no_ctx:
                extra["Skipped (no rendered context)"] = skipped_no_ctx
            self._log_dry_run_summary("synthesize_images", items, extra=extra)
            return

        from datasmith.agents.synthesizer import Synthesizer
        from datasmith.github.client import GitHubClient
        from datasmith.runners.synthesize_images import SynthesizeImagesRunner
        from datasmith.utils.docker_prune import builder_prune_watcher
        from datasmith.utils.tokens import TokenPool

        pool = TokenPool()
        gh = GitHubClient(pool)

        synth = Synthesizer(agent=self._agent, force=self._force)
        runner = SynthesizeImagesRunner(
            synth,
            gh=gh,
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )
        with builder_prune_watcher():
            await runner.run(items)
        await gh.close()

    async def _harbor_healthcheck(self, start_date: str, end_date: str) -> None:
        # Pull every PR with a synthesized container from stage 6.
        rows = fetch_all(
            "pull_requests",
            select=(
                "owner, repo, issue_number, merge_commit_sha, base_sha, "
                "container_name, patch, rendered_problem, classification, "
                "difficulty, merged_at"
            ),
            filters={"is_performance_commit": True},
            neq_filters={"container_name": ""},
            gte_filters={"created_at": start_date},
            lte_filters={"created_at": end_date},
        )

        # Require non-empty container_name, merge sha, base sha.
        ready = [r for r in rows if r.get("container_name") and r.get("merge_commit_sha") and r.get("base_sha")]

        # Filter to the explicit --tasks spec set first, if provided. Each spec
        # names a specific PR the operator wants to triage, so an unmatched
        # spec is a hard error (fail fast before spinning up Harbor).
        if self._harbor_tasks:
            wanted = self._harbor_tasks
            matched = [r for r in ready if (r["owner"], r["repo"], int(r["issue_number"])) in wanted]
            got_keys = {(r["owner"], r["repo"], int(r["issue_number"])) for r in matched}
            missing = wanted - got_keys
            if missing:
                missing_str = ", ".join(f"{o}/{rp}#{n}" for o, rp, n in sorted(missing))
                logger.warning(
                    "harbor_healthcheck: --tasks requested %d PR(s) not in candidate set: %s",
                    len(missing),
                    missing_str,
                )
            ready = matched
            logger.info("harbor_healthcheck: --tasks filter selected %d PR(s)", len(ready))

        # Skip PRs that already have a successful harbor_runs row, unless --force
        # or --tasks (explicit triage request implies re-run).
        skipped_already_run = 0
        if not self._force and not self._harbor_tasks:
            hr_rows = fetch_all(
                "harbor_runs",
                select="owner, repo, sha, status, max_speedup",
                filters={"status": "success"},
            )
            already_good = {(hr["owner"], hr["repo"], hr["sha"]) for hr in hr_rows if hr.get("max_speedup") is not None}
            before = len(ready)
            ready = [r for r in ready if (r["owner"], r["repo"], r["merge_commit_sha"]) not in already_good]
            skipped_already_run = before - len(ready)

        if self._tasks_per_repo is not None:
            ready = _cap_per_repo(ready, self._tasks_per_repo)

        if self._harbor_limit is not None and self._harbor_limit > 0:
            before = len(ready)
            ready = ready[: self._harbor_limit]
            logger.info("harbor_healthcheck: capped %d -> %d via --harbor-limit", before, len(ready))

        environment = "daytona" if self._harbor_use_daytona else "docker"
        n_concurrent_trials = self._n_concurrent or 4

        logger.info(
            "harbor_healthcheck: %d PRs ready (env=%s, n_concurrent_trials=%d)",
            len(ready),
            environment,
            n_concurrent_trials,
        )

        if self._dry_run:
            extra: dict[str, Any] = {
                "Date range": f"{start_date} to {end_date}",
                "Harbor environment": environment,
                "n_concurrent_trials": n_concurrent_trials,
                "rounds": self._harbor_rounds,
            }
            if skipped_already_run:
                extra["Skipped (already succeeded)"] = skipped_already_run
            self._log_dry_run_summary("harbor_healthcheck", ready, extra=extra)
            return

        if not ready:
            logger.info("harbor_healthcheck: nothing to dispatch")
            return

        import tempfile
        from pathlib import Path

        from datasmith.runners.harbor_healthcheck import run_harbor_healthcheck

        task_dir = Path(tempfile.mkdtemp(prefix="fc-harbor-"))
        try:
            await run_harbor_healthcheck(
                ready,
                task_dir=task_dir,
                use_daytona=self._harbor_use_daytona,
                n_concurrent_trials=n_concurrent_trials,
                rounds=self._harbor_rounds,
            )
        finally:
            # Leave the task dir in place for post-mortem debugging; Harbor
            # writes trial artifacts under jobs/ which are already separate.
            logger.info("harbor_healthcheck: task dir retained at %s", task_dir)

    async def _publish(self, start_date: str, end_date: str) -> None:
        if self._dry_run:
            # Query what would be published without running the pipeline
            rows = fetch_all(
                "candidate_containers",
                select="owner, repo, issue_number",
            )
            self._log_dry_run_summary(
                "publish",
                [{"owner": r["owner"], "repo": r["repo"]} for r in rows],
                extra={"Date range": f"{start_date} to {end_date}"},
            )
            return

        from datasmith.publish.pipeline import publish_pipeline

        await publish_pipeline(start_date, end_date)

    async def _scrape_benchmark_source(self, start_date: str, end_date: str) -> None:
        # Pull every (owner, repo, sha) we've successfully synthesized a container
        # for. We don't filter by date — the website wants the full corpus of
        # benchmark sources, and bench source rarely changes per commit, so
        # re-scraping is mostly a no-op on the upsert path.
        rows = fetch_all("candidate_containers", select="owner, repo, sha")

        # Dedup by (owner, repo) — one SHA per repo is enough to populate the
        # benchmark_codes rows; ASV bench files rarely diverge across commits
        # within a single repo and we always keep the newest scrape via the
        # last_scraped column.
        seen: set[tuple[str, str]] = set()
        items: list[dict[str, Any]] = []
        for r in rows:
            key = (r["owner"], r["repo"])
            if key in seen:
                continue
            seen.add(key)
            items.append({"owner": r["owner"], "repo": r["repo"], "sha": r["sha"]})

        logger.info("Scraping benchmark source for %d repos", len(items))

        if self._dry_run:
            self._log_dry_run_summary(
                "scrape_benchmark_source",
                items,
                extra={"Date range": f"{start_date} to {end_date}"},
            )
            return

        from datasmith.runners.scrape_benchmark_source import ScrapeBenchmarkSourceRunner

        runner = ScrapeBenchmarkSourceRunner(
            **({"n_concurrent": self._n_concurrent} if self._n_concurrent else {}),
        )
        await runner.run(items)

    def _get_completed_stages(self) -> list[str]:
        try:
            rows = fetch_all("runner_progress", select="runner_name, completed, total")
            completed: list[str] = []
            for r in rows:
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
