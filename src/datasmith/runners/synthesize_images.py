from __future__ import annotations

import asyncio
import datetime
import os
import tempfile
import threading
from typing import Any

from datasmith.agents.rate_limit import RateLimitError
from datasmith.agents.synthesizer import Synthesizer
from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.synthesize_images")

# All tunable knobs below are overridable from tokens.env — see CLAUDE.md
# "Tunable constants". tokens.env is auto-loaded by datasmith/__init__.py.

# Default wait applied when an agent signals a rate limit but we couldn't
# parse a reset time. One hour gives room for the five-hour bucket to drain
# a little without pinning the runner to a hard-coded weekly stall.
DATASMITH_RL_DEFAULT_PAUSE_S: float = float(os.environ.get("DATASMITH_RL_DEFAULT_PAUSE_S", "3600"))
# Grace period added on top of the parsed reset time to avoid immediately
# retrying at t=reset and getting throttled by a clock skew of a few seconds.
DATASMITH_RL_PAUSE_JITTER_S: float = float(os.environ.get("DATASMITH_RL_PAUSE_JITTER_S", "30"))
# Maximum consecutive rate-limit retries for a single item before we give up
# and let it fail. Prevents infinite loops if detection is misfiring.
DATASMITH_RL_MAX_RETRIES: int = int(os.environ.get("DATASMITH_RL_MAX_RETRIES", "3"))

# Chronological neighborhood window (days) used when enqueuing neighbor PRs
# after a successful synthesis. PRs created within ±this many days of the
# successful PR are highly likely to share its dependency environment, so
# TRY_SIMILAR will reuse the fresh context for free instead of burning
# another codex session on them.
DATASMITH_NEIGHBOR_WINDOW_DAYS: int = int(os.environ.get("DATASMITH_NEIGHBOR_WINDOW_DAYS", "60"))
# Hard ceiling on neighbors enqueued per successful item. Protects against a
# runaway enqueue burst in a repo with hundreds of PRs inside the window.
# Neighbors whose context hits in TRY_SIMILAR are effectively free, but those
# that fall through to LLM_GENERATE do consume agent budget, so we cap.
DATASMITH_NEIGHBOR_CAP: int = int(os.environ.get("DATASMITH_NEIGHBOR_CAP", "40"))


def _ensure_prerequisite_images(owner: str, repo: str, py_version: str = "", build_root: str = ".") -> None:
    """Build the base and repo Docker images if they don't exist locally.

    The three-tier hierarchy (base → repo → PR) requires each parent image
    to be present in the local daemon before the child can be built.

    ``build_root`` is the row's ``primary_root`` — the package root inside the
    repository. This runner is the only caller that has read it.
    """
    from datasmith.docker.images import ImageManager, get_base_image_name, get_repo_image_name

    mgr = ImageManager()
    base_tag = get_base_image_name()
    repo_tag = get_repo_image_name(owner, repo, py_version)

    if not mgr.image_exists(base_tag):
        logger.info("Building missing base image: %s", base_tag)
        mgr.build_base_image(py_version=py_version)

    if not mgr.image_exists(repo_tag):
        logger.info("Building missing repo image: %s", repo_tag)
        mgr.build_repo_image(owner, repo, py_version=py_version, build_root=build_root)


def _build_pr_image(
    owner: str,
    repo: str,
    issue_number: int,
    sha: str,
    env_payload: str,
    docker_context: Any | None = None,
    python_version: str = "",
    base_sha: str = "",
) -> str:
    """Build the final PR image from synthesized context (no push).

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
            )
    else:
        mgr.build_pr_image(
            owner,
            repo,
            issue_number,
            commit_sha=checkout_sha or "HEAD",
            env_payload=env_payload or "[]",
            py_version=python_version,
        )

    return pr_tag


def _push_pr_image(owner: str, repo: str, pr_tag: str, py_version: str = "") -> None:
    """Push a previously-built PR image (and its repo parent) to DockerHub.

    ``py_version`` names the repo parent. Without it this pushes a tag nothing
    ever built, and the ``except`` below turns that into a warning nobody reads.
    """
    from datasmith.docker.images import get_repo_image_name
    from datasmith.docker.publish import DockerHubPublisher

    publisher = DockerHubPublisher()
    repo_tag = get_repo_image_name(owner, repo, py_version)

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

    Synthesized contexts may only contain a subset of the 9 expected files
    (e.g. only ``build_pkg_sh``).  The Dockerfile.pr ``COPY`` directives
    require every file to be present, so we backfill from the built-in
    templates for anything the synthesizer didn't produce.

    ``run-tests.sh`` is a Jinja2 template that requires rendering with
    ``base_commit`` and embedded Python scripts before it can be used.
    """
    import os
    import shutil
    from pathlib import Path

    templates = Path(__file__).parents[1] / "docker" / "templates"

    # Every file that Dockerfile.pr references via COPY
    required = [
        "Dockerfile.pr",
        "docker_build_env.sh",
        "docker_build_pkg.sh",
        "docker_build_run.sh",
        "docker_build_final.sh",
        "profile.sh",
        "run-tests.sh",
        "entrypoint.sh",
    ]

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


# Lock to serialize prerequisite image builds (base + repo) across threads.
# Building these is expensive and they're shared, so we avoid duplicate work.
_prereq_lock = threading.Lock()
# Track repos whose prerequisite images are confirmed present.
_prereq_done: set[tuple[str, str, str]] = set()


def _fetch_neighbor_items(
    owner: str,
    repo: str,
    lo_iso: str,
    hi_iso: str,
) -> list[dict[str, Any]]:
    """Query Supabase for PRs in *owner/repo* with ``created_at`` in ``[lo, hi]``.

    Returns item dicts shaped like ``pipeline._synthesize_images`` items, so
    :meth:`SynthesizeImagesRunner._do_process_item` can consume them without
    any special-casing. Filters mirror the stage 6 selection: performance
    commits with resolved packages and a non-empty extracted problem context,
    excluding PRs that already have a ``container_name``.
    """
    from datasmith.utils.db import fetch_all

    rows = fetch_all(
        "pull_requests",
        select=("owner, repo, issue_number, merge_commit_sha, base_sha, title, body, created_at, rendered_problem"),
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
            "title": r.get("title", ""),
            "body": r.get("body", ""),
            "created_at": r.get("created_at"),
            "pr_context": r.get("rendered_problem") or r.get("body", ""),
            "repo_description": repo_description,
            "env_payload": pkg.get("env_payload", ""),
            "python_version": pkg.get("python_version", ""),
            "primary_root": pkg.get("primary_root") or ".",
            # Carried for shape parity with pipeline._synthesize_images items.
            # Ordering lives in the pipeline, which owns the queue; a runner
            # importing from update/ would invert the dependency direction.
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
        # Shared pause state — when any worker raises RateLimitError, it sets
        # `_rl_resume_at` and every other worker blocks on `_rl_lock` until
        # the clock passes that timestamp. A single agent's weekly budget is
        # shared across all workers, so pausing one without pausing the rest
        # would just burn the remaining attempt budget on ~2s failures.
        self._rl_lock = asyncio.Lock()
        self._rl_resume_at: datetime.datetime | None = None
        # Queue-based worker pool state. Populated in ``run`` so that
        # ``_do_process_item`` can enqueue chronologically adjacent PRs on
        # success and have other workers pick them up without respawning.
        # The runner formalises the old two-pass workflow (first pass
        # hydrates with codex, second pass with agent=none) into a single
        # pass: once a PR synthesises successfully, its neighbors are added
        # to the same queue and will hit TRY_SIMILAR cheaply before falling
        # through to LLM_GENERATE only if the context genuinely mismatches.
        self._queue: asyncio.Queue[Any] | None = None
        self._enqueued: set[tuple[str, str, int]] = set()

    async def run(self, items: list[Any]) -> None:
        """Override ``BaseRunner.run`` with a queue-based worker pool.

        ``BaseRunner.run`` gathers a fixed list of tasks, but this runner
        needs to enqueue additional items mid-flight (chronologically
        adjacent PRs, added after each successful synthesis). A bounded
        worker pool reading from an ``asyncio.Queue`` gives us that
        ability without perturbing other stages.
        """
        self._total = len(items)
        self._completed = 0
        self._failed = 0
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

        Every worker first blocks on ``_wait_for_rate_limit`` so that a
        pause triggered by a peer worker is respected without that peer
        needing to re-raise into every outstanding task. Each item gets up
        to ``DATASMITH_RL_MAX_RETRIES`` rate-limit pauses before we let it fail.
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
        env_payload = item.get("env_payload", "")

        from datasmith.docker.images import get_repo_image_name

        repo_image = get_repo_image_name(owner, repo, py_version)

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
        )

        # Record the container name in Supabase *before* pushing. If the DB
        # write fails, the image stays unpublished and a re-run picks up the
        # PR cleanly — avoids orphan images on DockerHub with no DB state.
        client = get_client()
        client.table("pull_requests").update({"container_name": pr_tag}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        # DB state is durable — safe to publish the image.
        await asyncio.to_thread(_push_pr_image, owner, repo, pr_tag, py_version)

        # Spread the win: PRs created within ±DATASMITH_NEIGHBOR_WINDOW_DAYS
        # of this one likely share its dependency environment, so the context
        # we just cached will satisfy TRY_SIMILAR for them for free. Only the
        # PRs whose context genuinely differs fall through to LLM_GENERATE.
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

        Only the first worker to enter under the lock sets ``_rl_resume_at``;
        later workers that hit the same exception while the pause is already
        in effect simply return and re-wait on their next loop iteration.
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

        The interpreter is part of the key because it is part of the tag. Keyed
        on the repository alone, a second commit declaring a different Python
        reads as already done and its parent image is never built.

        ``build_root`` is deliberately *not* part of the key: it is not part of
        the tag either, so keying on it would rebuild the same tag with a
        different working directory and let the last writer win. Two commits of
        one repository that share an interpreter and disagree on
        ``primary_root`` share an image, and the first one seen decides.
        """
        key = (owner, repo, py_version)
        if key in _prereq_done:
            return
        with _prereq_lock:
            if key in _prereq_done:
                return
            _ensure_prerequisite_images(owner, repo, py_version, build_root)
            _prereq_done.add(key)
