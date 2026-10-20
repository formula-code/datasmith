from __future__ import annotations

import asyncio
import datetime
import os
import tempfile
import threading
from collections import Counter
from typing import Any

from datasmith.agents.rate_limit import RateLimitError
from datasmith.agents.synthesizer import Synthesizer
from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger
from datasmith.utils.overrides import benchmark_dest_for, fetch_overrides

logger = get_logger("runners.synthesize_images")

# Each setting below can be overridden in tokens.env, which datasmith/__init__.py loads.

# Wait used when an agent hits a rate limit with no parseable reset time.
DATASMITH_RL_DEFAULT_PAUSE_S: float = float(os.environ.get("DATASMITH_RL_DEFAULT_PAUSE_S", "3600"))
# Extra wait after the parsed reset time, to allow for clock skew.
DATASMITH_RL_PAUSE_JITTER_S: float = float(os.environ.get("DATASMITH_RL_PAUSE_JITTER_S", "30"))
# Rate-limit retries per item before it fails, so a false detection cannot loop forever.
DATASMITH_RL_MAX_RETRIES: int = int(os.environ.get("DATASMITH_RL_MAX_RETRIES", "3"))

# After a success, PRs created within this many days likely share its environment,
# so they are queued to reuse the context through TRY_SIMILAR.
DATASMITH_NEIGHBOR_WINDOW_DAYS: int = int(os.environ.get("DATASMITH_NEIGHBOR_WINDOW_DAYS", "60"))
# Maximum neighbors queued per success, because neighbors that reach
# LLM_GENERATE cost agent budget.
DATASMITH_NEIGHBOR_CAP: int = int(os.environ.get("DATASMITH_NEIGHBOR_CAP", "40"))


def _ensure_prerequisite_images(owner: str, repo: str, py_version: str = "", build_root: str = ".") -> None:
    """Build the base and repo Docker images if they don't exist locally.

    ``build_root`` is the row's ``primary_root``, the image's WORKDIR. It is
    part of the repo image tag, so commits with different roots get different images.
    """
    from datasmith.docker.images import ImageManager, get_base_image_name, get_repo_image_name

    mgr = ImageManager()
    base_tag = get_base_image_name()
    repo_tag = get_repo_image_name(owner, repo, py_version, build_root)

    if not mgr.image_exists(base_tag):
        logger.info("Building missing base image: %s", base_tag)
        mgr.build_base_image(py_version=py_version)

    if not mgr.image_exists(repo_tag):
        logger.info("Building missing repo image: %s", repo_tag)
        mgr.build_repo_image(owner, repo, py_version=py_version, build_root=build_root)


def _repo_tag_for(owner: str, repo: str, py_version: str, build_root: str) -> str:
    """Return the repository image tag a row's ``packages`` fields select."""
    from datasmith.docker.images import get_repo_image_name

    return get_repo_image_name(owner, repo, py_version, build_root)


def _build_pr_image(
    owner: str,
    repo: str,
    issue_number: int,
    sha: str,
    env_payload: str,
    docker_context: Any | None = None,
    python_version: str = "",
    base_sha: str = "",
    build_root: str = "",
    benchmark_dest: str = "",
) -> str:
    """Build the final PR image from synthesized context (no push).

    ``benchmark_dest`` is the benchmark file from formulacode_task_overrides,
    passed as the BENCHMARK_DEST build arg; empty skips the benchmark_dest_missing check.

    Returns the PR image tag that will be used for the subsequent push.
    """
    from datasmith.docker.images import ImageManager, get_pr_image_name

    ctx = docker_context
    mgr = ImageManager()
    pr_tag = get_pr_image_name(owner, repo, issue_number)

    # Use base_sha for the Docker checkout so the repo is at the
    # pre-optimization state; fall back to merge_commit_sha for compat.
    checkout_sha = base_sha or sha

    if ctx is not None:
        with tempfile.TemporaryDirectory(prefix="docker-ctx-") as tmpdir:
            ctx.to_directory(tmpdir)
            _fill_missing_scripts(tmpdir, base_commit=checkout_sha)
            mgr.build_pr_image(
                owner,
                repo,
                issue_number,
                context=tmpdir,
                commit_sha=checkout_sha or "HEAD",
                env_payload=env_payload or "[]",
                py_version=python_version,
                build_root=build_root,
                benchmark_dest=benchmark_dest,
            )
    else:
        mgr.build_pr_image(
            owner,
            repo,
            issue_number,
            commit_sha=checkout_sha or "HEAD",
            env_payload=env_payload or "[]",
            py_version=python_version,
            build_root=build_root,
            benchmark_dest=benchmark_dest,
        )

    return pr_tag


# Build without publishing; see _push_pr_image.
DATASMITH_SKIP_IMAGE_PUSH: bool = os.environ.get("DATASMITH_SKIP_IMAGE_PUSH", "").strip().lower() in {
    "1",
    "true",
    "yes",
}


def _push_pr_image(owner: str, repo: str, pr_tag: str, py_version: str = "", build_root: str = "") -> None:
    """Push a built PR image and its repo parent image to DockerHub.

    ``py_version`` and ``build_root`` select the repo parent tag.
    Set DATASMITH_SKIP_IMAGE_PUSH=1 to build without publishing, for example in a trial run.
    """
    from datasmith.docker.images import get_repo_image_name
    from datasmith.docker.publish import DockerHubPublisher

    if DATASMITH_SKIP_IMAGE_PUSH:
        logger.info("DATASMITH_SKIP_IMAGE_PUSH set; not pushing %s", pr_tag)
        return

    publisher = DockerHubPublisher()
    repo_tag = get_repo_image_name(owner, repo, py_version, build_root)

    try:
        publisher.push(repo_tag)
    except Exception:
        logger.warning("Failed to push repo image %s (non-fatal)", repo_tag)

    publisher.push(pr_tag)
    logger.info("Pushed PR image: %s", pr_tag)


def _render_run_tests_sh(docker_templates: Any, base_commit: str) -> str:
    """Render the run-tests.sh Jinja2 template with embedded scripts."""
    from pathlib import Path

    from jinja2 import Environment, FileSystemLoader

    docker_templates = Path(docker_templates)
    env = Environment(
        loader=FileSystemLoader(str(docker_templates)),
        keep_trailing_newline=True,
        autoescape=False,
    )
    template = env.get_template("run-tests.sh")

    pytest_runner = (docker_templates / "pytest_runner.py").read_text()
    parser = (docker_templates / "parser.py").read_text()

    return template.render(
        base_commit=base_commit,
        pytest_runner=pytest_runner,
        parser=parser,
        run_pytest=True,
    )


def _fill_missing_scripts(context_dir: str, base_commit: str = "") -> None:
    """Copy any missing shell scripts and Dockerfile.pr from the templates directory.

    Dockerfile.pr copies every file, so files the synthesizer did not write
    come from the templates. ``run-tests.sh`` is rendered with ``base_commit``.
    """
    import os
    import shutil
    from pathlib import Path

    templates = Path(__file__).parents[1] / "docker" / "templates"
    # Copied from the harbor trial template so stages 6 and 7 select LSV benchmarks identically.
    lsv_templates = Path(__file__).parents[1] / "harbor_adapter" / "template" / "tests"

    # Every file that Dockerfile.pr references via COPY
    required = [
        "Dockerfile.pr",
        "docker_build_env.sh",
        "docker_build_pkg.sh",
        "docker_build_run.sh",
        "docker_build_final.sh",
        "emit_manifest.py",
        "measure.sh",
        "apply_oracle_patch.py",
        "emit_measure.py",
        "profile.sh",
        "run-tests.sh",
        "entrypoint.sh",
    ]
    lsv_required = ["lsv_init.py", "lsv_measure.py", "parser.py"]

    for fname in required:
        target = os.path.join(context_dir, fname)
        if os.path.exists(target):
            continue
        if fname == "run-tests.sh":
            # run-tests.sh is a Jinja2 template — render it instead of copying raw
            rendered = _render_run_tests_sh(templates, base_commit=base_commit)
            with open(target, "w") as f:
                f.write(rendered)
        else:
            src = templates / fname
            if src.exists():
                shutil.copy2(str(src), target)

    for fname in lsv_required:
        target = os.path.join(context_dir, fname)
        if os.path.exists(target):
            continue
        src = lsv_templates / fname
        if src.exists():
            shutil.copy2(str(src), target)


# Serializes base and repo image builds across threads, so none is built twice.
_prereq_lock = threading.Lock()
# Repo image tags whose prerequisites are present; keyed by the tag itself so the
# key always matches what was built.
_prereq_done: set[str] = set()


def _fetch_neighbor_items(
    owner: str,
    repo: str,
    lo_iso: str,
    hi_iso: str,
) -> list[dict[str, Any]]:
    """Query Supabase for PRs in *owner/repo* with ``created_at`` in ``[lo, hi]``.

    Returns items shaped like ``pipeline._synthesize_images`` items, with the
    same eligibility filters as stage 6. The date range is not the run window on
    purpose: neighbors outside the window can share the cached environment.
    """
    from datasmith.utils.db import fetch_all

    rows = fetch_all(
        "pull_requests",
        select=(
            "owner, repo, issue_number, merge_commit_sha, base_sha, patch, title, body, created_at, rendered_problem"
        ),
        filters={
            "owner": owner,
            "repo": repo,
            "is_performance_commit": True,
            "is_performance_commit_symbolic": True,
        },
        neq_filters={"merge_commit_sha": ""},
        gte_filters={"created_at": lo_iso},
        lte_filters={"created_at": hi_iso},
        is_null=["container_name"],
    )
    if not rows:
        return []

    # Every resolved commit of this repository is a candidate neighbour; the
    # probe orders them, it does not exclude them.
    pkg_rows = fetch_all(
        "packages",
        select="owner, repo, sha, env_payload, python_version, probe_status, primary_root",
        filters={"owner": owner, "repo": repo},
    )
    pkg_lookup = {(p["owner"], p["repo"], p["sha"]): p for p in pkg_rows}

    ctx_rows = fetch_all(
        "candidate_prs",
        select="owner, repo, issue_number, issues_json, initial_observations",
        filters={"owner": owner, "repo": repo},
    )
    eligible: set[tuple[str, str, int]] = {
        (c["owner"], c["repo"], c["issue_number"])
        for c in ctx_rows
        if c.get("issues_json") or c.get("initial_observations")
    }

    # Batch repo_description lookup once — cheap and keeps the item dict
    # shape consistent with pipeline._synthesize_images.
    desc_rows = fetch_all(
        "repositories",
        select="owner, repo, description",
        filters={"owner": owner, "repo": repo},
    )
    repo_description = ""
    if desc_rows:
        repo_description = desc_rows[0].get("description") or ""

    items: list[dict[str, Any]] = []
    for r in rows:
        sha = r.get("merge_commit_sha", "")
        pkg = pkg_lookup.get((r["owner"], r["repo"], sha), {})
        if not pkg:
            continue
        if (r["owner"], r["repo"], r["issue_number"]) not in eligible:
            continue
        items.append({
            "owner": r["owner"],
            "repo": r["repo"],
            "issue_number": r["issue_number"],
            "sha": sha,
            "base_sha": r.get("base_sha", ""),
            # The oracle patch, shipped into the synthesis workspace so
            # measure.sh can prove the container measures a speedup.
            "patch": r.get("patch", "") or "",
            "title": r.get("title", ""),
            "body": r.get("body", ""),
            "created_at": r.get("created_at"),
            "pr_context": r.get("rendered_problem") or r.get("body", ""),
            "repo_description": repo_description,
            "env_payload": pkg.get("env_payload", ""),
            "python_version": pkg.get("python_version", ""),
            "primary_root": pkg.get("primary_root") or ".",
            # Same shape as pipeline._synthesize_images items; the pipeline does the ordering.
            "probe_status": pkg.get("probe_status"),
        })
    return items


class SynthesizeImagesRunner(BaseRunner):
    """Run Synthesizer for each PR to produce Docker build contexts."""

    def __init__(
        self,
        synthesizer: Synthesizer,
        gh: Any | None = None,
        n_concurrent: int = 3,
    ) -> None:
        super().__init__(name="synthesize_images", n_concurrent=n_concurrent)
        self._synthesizer = synthesizer
        self._gh = gh  # GitHubClient, optional — needed for rendering problem statements
        # Shared pause: a RateLimitError sets ``_rl_resume_at`` and all workers wait,
        # because they share one agent budget.
        self._rl_lock = asyncio.Lock()
        self._rl_resume_at: datetime.datetime | None = None
        # Set in ``run``, so a success can queue its neighbor PRs for the same workers.
        self._queue: asyncio.Queue[Any] | None = None
        self._enqueued: set[tuple[str, str, int]] = set()

    async def run(self, items: list[Any]) -> None:
        """Override ``BaseRunner.run`` with a queue-based worker pool.

        The queue lets neighbor PRs be added while the stage runs.
        """
        self._total = len(items)
        self._completed = 0
        self._failed = 0
        self._error_types = Counter()
        self._enqueued = set()
        self._init_progress()

        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._queue = queue
        for item in items:
            key = (item["owner"], item["repo"], item["issue_number"])
            if key in self._enqueued:
                continue
            self._enqueued.add(key)
            queue.put_nowait(item)

        workers = [asyncio.create_task(self._worker_loop()) for _ in range(self._n_concurrent)]
        try:
            await queue.join()
        except (KeyboardInterrupt, asyncio.CancelledError):
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self._update_progress(force=True)
            self._log_summary()
            self._queue = None

    async def _worker_loop(self) -> None:
        """Pull items off the shared queue until cancelled."""
        queue = self._queue
        if queue is None:
            return
        while True:
            item = await queue.get()
            try:
                await self._process_item(item)
                self._completed += 1
            except Exception as exc:
                self._failed += 1
                self._error_types[type(exc).__name__] += 1
                self._log_failure(item, exc)
                logger.exception("Failed processing item %s", self._item_id(item))
            finally:
                self._maybe_update_progress()
                queue.task_done()

    async def _render_problem(self, item: dict[str, Any]) -> str | None:
        """Render the problem statement for a PR, scraping linked issues.

        Returns the rendered markdown, or ``None`` if rendering is skipped
        (no GitHubClient) or fails.
        """
        if self._gh is None:
            return None

        owner: str = item["owner"]
        repo: str = item["repo"]
        issue_number: int = item["issue_number"]

        from datasmith.github.links import scrape_links
        from datasmith.github.models import PR
        from datasmith.github.render import render_problem_statement

        # Build a PR object for scrape_links and render_problem_statement
        pr = PR(
            repository=f"{owner}/{repo}",
            issue_number=issue_number,
            title=item.get("title", ""),
            body=item.get("body", ""),
            created_at=item.get("created_at"),
        )

        # BFS-scrape linked issues (async GitHub API calls)
        issues = await scrape_links(
            pr,
            self._gh.get_issue_expanded,
            depth=2,
            only_issues=True,
            limit=6,
        )

        logger.info(
            "Scraped %d linked issues for %s/%s#%d",
            len(issues),
            owner,
            repo,
            issue_number,
        )

        # Render the problem statement (may invoke ProblemExtractor LLM — run in thread)
        repo_description: str = item.get("repo_description", "")
        rendered = await asyncio.to_thread(
            render_problem_statement,
            pr,
            issues=issues,
            repo_description=repo_description,
            anonymize=True,
            extract=True,
        )

        # Persist to DB
        client = get_client()
        client.table("pull_requests").update({"rendered_problem": rendered}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        logger.info("Rendered problem statement for %s/%s#%d", owner, repo, issue_number)
        return rendered

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict, transparently pausing on CLI-agent rate limits.

        Each attempt first waits out any shared pause. An item gets up to
        ``DATASMITH_RL_MAX_RETRIES`` rate-limit pauses before it fails.
        """
        for attempt in range(DATASMITH_RL_MAX_RETRIES + 1):
            await self._wait_for_rate_limit()
            try:
                await self._do_process_item(item)
                return
            except RateLimitError as exc:
                if attempt >= DATASMITH_RL_MAX_RETRIES:
                    logger.warning(
                        "Rate-limit retries exhausted for %s after %d pauses — failing item",
                        self._item_id(item),
                        DATASMITH_RL_MAX_RETRIES,
                    )
                    raise
                await self._trigger_rate_limit_pause(exc)

    async def _do_process_item(self, item: Any) -> None:
        """Inner implementation of ``_process_item``."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        pr_context = item.get("pr_context", "")
        py_version = item.get("python_version", "")
        build_root = item.get("primary_root") or "."

        # Ensure base and repo images exist before synthesis needs them
        await asyncio.to_thread(self._ensure_prereqs, owner, repo, py_version, build_root)

        # Render the problem statement before synthesis (skip if already rendered)
        if not pr_context:
            rendered = await self._render_problem(item)
            if rendered:
                pr_context = rendered

        sha = item.get("sha", "")
        base_sha = item.get("base_sha", "")
        solution_patch = item.get("patch", "") or ""
        # Benchmark file from the task's override row, if any; empty skips the
        # benchmark_dest_missing check.
        benchmark_dest = benchmark_dest_for(fetch_overrides([(owner, repo, issue_number)]), (owner, repo, issue_number))
        if benchmark_dest:
            logger.info(
                "Task override for %s/%s#%d declares benchmark_dest=%s",
                owner,
                repo,
                issue_number,
                benchmark_dest,
            )
        env_payload = item.get("env_payload", "")

        repo_image = _repo_tag_for(owner, repo, py_version, build_root)

        # Run synthesizer in thread (Docker operations are blocking)
        ctx = await asyncio.to_thread(
            self._synthesizer.run,
            owner,
            repo,
            issue_number,
            pr_context,
            sha,
            repo_image=repo_image,
            env_payload=env_payload,
            python_version=py_version,
            base_sha=base_sha,
            solution_patch=solution_patch,
        )

        if ctx is None:
            raise RuntimeError(f"Synthesis failed for {owner}/{repo}#{issue_number}")

        logger.info("Successfully synthesized image for %s/%s#%d", owner, repo, issue_number)

        # Build the final PR image locally (no push yet)
        pr_tag = await asyncio.to_thread(
            _build_pr_image,
            owner,
            repo,
            issue_number,
            sha,
            env_payload,
            ctx,
            py_version,
            base_sha=base_sha,
            build_root=build_root,
            benchmark_dest=benchmark_dest,
        )

        # Record the container name before pushing, so a failed write leaves no
        # published image without a row.
        client = get_client()
        client.table("pull_requests").update({"container_name": pr_tag}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        await asyncio.to_thread(_push_pr_image, owner, repo, pr_tag, py_version, build_root)

        # Queue nearby PRs, which can likely reuse this context through TRY_SIMILAR.
        await self._enqueue_neighbors(owner, repo, issue_number, item.get("created_at"))

    async def _enqueue_neighbors(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        created_at_raw: Any,
    ) -> None:
        """Find and enqueue chronologically adjacent PRs in the same repo."""
        if self._queue is None or not created_at_raw:
            return
        try:
            if isinstance(created_at_raw, str):
                base_dt = datetime.datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
            elif isinstance(created_at_raw, datetime.datetime):
                base_dt = created_at_raw
            else:
                return
        except ValueError:
            return

        window = datetime.timedelta(days=DATASMITH_NEIGHBOR_WINDOW_DAYS)
        lo = (base_dt - window).isoformat()
        hi = (base_dt + window).isoformat()

        neighbors = await asyncio.to_thread(_fetch_neighbor_items, owner, repo, lo, hi)

        added = 0
        for nb in neighbors:
            if added >= DATASMITH_NEIGHBOR_CAP:
                break
            key = (nb["owner"], nb["repo"], nb["issue_number"])
            if key in self._enqueued:
                continue
            self._enqueued.add(key)
            self._total += 1
            self._queue.put_nowait(nb)
            added += 1

        if added:
            logger.info(
                "Enqueued %d neighbor PR(s) for %s/%s#%d (±%d days)",
                added,
                owner,
                repo,
                issue_number,
                DATASMITH_NEIGHBOR_WINDOW_DAYS,
            )

    async def _wait_for_rate_limit(self) -> None:
        """Block until any active rate-limit pause has elapsed."""
        while True:
            resume_at = self._rl_resume_at
            if resume_at is None:
                return
            now = datetime.datetime.now(tz=datetime.UTC)
            remaining = (resume_at - now).total_seconds()
            if remaining <= 0:
                return
            logger.info(
                "Worker sleeping %.0fs for agent rate-limit reset at %s",
                remaining,
                resume_at.isoformat(),
            )
            await asyncio.sleep(min(remaining, 60.0))

    async def _trigger_rate_limit_pause(self, exc: RateLimitError) -> None:
        """Install a shared pause triggered by *exc*.

        Only the first worker sets ``_rl_resume_at``; later workers keep that pause.
        """
        async with self._rl_lock:
            now = datetime.datetime.now(tz=datetime.UTC)
            if self._rl_resume_at and self._rl_resume_at > now:
                # Another worker already set the pause — honour theirs.
                return
            if exc.reset_at is not None:
                resume_at = exc.reset_at + datetime.timedelta(seconds=DATASMITH_RL_PAUSE_JITTER_S)
            else:
                resume_at = now + datetime.timedelta(seconds=DATASMITH_RL_DEFAULT_PAUSE_S)
            self._rl_resume_at = resume_at
            wait_s = max(0.0, (resume_at - now).total_seconds())
            logger.warning(
                "Agent %s hit usage limit — pausing synthesis until %s (%.0fs)",
                exc.agent_name,
                resume_at.isoformat(),
                wait_s,
            )

    @staticmethod
    def _ensure_prereqs(owner: str, repo: str, py_version: str, build_root: str = ".") -> None:
        """Build base/repo images if missing, with dedup across threads.

        The key is the repo image tag, which includes the interpreter and package root.
        """
        key = _repo_tag_for(owner, repo, py_version, build_root)
        if key in _prereq_done:
            return
        with _prereq_lock:
            if key in _prereq_done:
                return
            _ensure_prerequisite_images(owner, repo, py_version, build_root)
            _prereq_done.add(key)
