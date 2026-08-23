"""The ingestion window means the same thing in every stage (spec section 7.3).

Stage 2 windowed ``merged_at``, half-open.  Stages 3, 4 and 5 windowed
``created_at``, inclusive at both ends.  The two defects masked each other
exactly: because stage 2 only ever stored PRs both created *and* merged inside
the window, no stored row could have ``created_at`` before the window, so the
column mismatch was invisible and every stage-2-only test still passed.

That is why this test spans the stages rather than checking one.  It asserts
that one PR — merged inside the window, created a month before it — is
selected by stages 2, 3, 4 and 5 alike, and that the upper bound is exclusive
in all of them.

The fake ``fetch_all`` here evaluates the predicates against in-memory rows
instead of recording the kwargs it was called with.  Asserting on kwargs would
pass just as happily against a filter that selects the wrong rows.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datasmith.update.pipeline import Pipeline

# Half-open [2026-08-01, 2026-08-04).  Three days, the window the spec's
# ground-truth verification uses.
_START = "2026-08-01"
_END = "2026-08-04"

_OWNER = "pandas-dev"
_REPO = "pandas"

# The PR the old code lost everywhere: merged well inside the window, opened a
# month before it.  Repositories with a deliberate review process are made
# entirely of these, which is why the loss was biased rather than random.
_CREATED_BEFORE = 1
# Boundary rows.  The second is the only one that can tell lte from lt.
_LAST_SECOND = 2
_AT_END = 3
_MERGED_BEFORE = 4

_CORE_FILES = [{"filename": "pandas/core/frame.py", "additions": 20, "deletions": 5}]


def _pr(number: int, created_at: str, merged_at: str, *, is_perf: bool | None) -> dict[str, Any]:
    return {
        "owner": _OWNER,
        "repo": _REPO,
        "issue_number": number,
        "title": f"PERF: speed up thing {number}",
        "body": "body",
        "patch": "diff --git a/pandas/core/frame.py b/pandas/core/frame.py\n-old\n+new\n",
        "file_changes": _CORE_FILES,
        "created_at": created_at,
        "merged_at": merged_at,
        "merge_commit_sha": f"sha{number}",
        "is_performance_commit": is_perf,
        "is_performance_commit_symbolic": True,
    }


def _pull_requests(is_perf: bool | None) -> list[dict[str, Any]]:
    """The four-row fixture, with whatever classification state a stage needs."""
    return [
        _pr(_CREATED_BEFORE, "2026-07-01T09:00:00Z", "2026-08-02T10:00:00Z", is_perf=is_perf),
        _pr(_LAST_SECOND, "2026-08-03T09:00:00Z", "2026-08-03T23:59:59Z", is_perf=is_perf),
        _pr(_AT_END, "2026-08-03T09:00:00Z", "2026-08-04T00:00:00Z", is_perf=is_perf),
        _pr(_MERGED_BEFORE, "2026-07-01T09:00:00Z", "2026-07-31T23:00:00Z", is_perf=is_perf),
    ]


# One predicate per ``fetch_all`` kwarg. Each returns True when the row
# survives that kwarg. ISO-8601 strings compare lexicographically in the same
# order they compare as timestamps, which is also how Postgres reads a bare
# ``2026-08-04`` bound: midnight, so a row merged exactly at midnight is
# outside a half-open window.
_PREDICATES: dict[str, Any] = {
    "filters": lambda row, col, val: row.get(col) == val,
    "gte_filters": lambda row, col, val: row.get(col) is not None and row[col] >= val,
    "lte_filters": lambda row, col, val: row.get(col) is not None and row[col] <= val,
    "lt_filters": lambda row, col, val: row.get(col) is not None and row[col] < val,
    "neq_filters": lambda row, col, val: row.get(col) != val,
    "in_filters": lambda row, col, val: row.get(col) in set(val),
}


def _matches(row: dict[str, Any], **kw: Any) -> bool:
    """Evaluate one row against the predicates ``fetch_all`` accepts."""
    if any(row.get(col) is not None for col in kw.get("is_null") or []):
        return False
    return all(check(row, col, val) for name, check in _PREDICATES.items() for col, val in (kw.get(name) or {}).items())


def _fake_fetch_all(tables: dict[str, list[dict[str, Any]]]) -> Any:
    """A ``fetch_all`` that really applies its predicates to *tables*."""

    def _fetch(table: str, select: str = "*", **kw: Any) -> list[dict[str, Any]]:
        return [dict(row) for row in tables.get(table, []) if _matches(row, **kw)]

    return _fetch


class _Selection:
    """Captures the items a dry-run stage decided to process."""

    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def __call__(self, stage_name: str, items: list[Any], extra: Any = None) -> None:
        self.items = list(items)

    @property
    def numbers(self) -> set[int]:
        return {int(i["issue_number"]) for i in self.items}

    @property
    def shas(self) -> set[str]:
        return {str(i["sha"]) for i in self.items}


async def _run_stage(
    stage: str,
    tables: dict[str, list[dict[str, Any]]],
    *,
    force: bool = False,
) -> _Selection:
    """Run one stage in dry-run mode and return what it selected."""
    pipeline = Pipeline(dry_run=True, force=force)
    selection = _Selection()
    pipeline._log_dry_run_summary = selection  # type: ignore[method-assign]
    with patch("datasmith.update.pipeline.fetch_all", _fake_fetch_all(tables)):
        await getattr(pipeline, f"_{stage}")(_START, _END)
    return selection


def _packages(shas: list[str]) -> list[dict[str, Any]]:
    return [{"owner": _OWNER, "repo": _REPO, "sha": sha, "can_install": True} for sha in shas]


class TestWindowContract:
    """One window definition: merged_at, half-open [start, end), every stage."""

    async def test_stage_2_asks_github_for_the_merge_window(self) -> None:
        """Stage 2 stores a PR merged in the window whatever its creation date."""
        from datasmith.runners.scrape_commits import ScrapeCommitsRunner

        node = {
            "number": _CREATED_BEFORE,
            "title": "PERF: speed up thing 1",
            "body": "body",
            "state": "closed",
            "created_at": "2026-07-01T09:00:00Z",
            "merged_at": "2026-08-02T10:00:00Z",
            "closed_at": "2026-08-02T10:00:00Z",
            "merge_commit_sha": f"sha{_CREATED_BEFORE}",
            "base": {"sha": "base"},
            "head": {"sha": "head"},
            "labels": [],
            "file_changes": _CORE_FILES,
            "changed_files": len(_CORE_FILES),
        }
        gh = MagicMock()
        gh.fetch_merged_prs = AsyncMock(return_value=[node])
        upsert = AsyncMock(side_effect=lambda table, rows, **kw: len(rows))

        with (
            patch("datasmith.runners.scrape_commits.afetch_all", AsyncMock(return_value=[])),
            patch("datasmith.runners.scrape_commits.abatch_upsert", upsert),
            patch("datasmith.runners.base.get_client", MagicMock()),
        ):
            runner = ScrapeCommitsRunner(gh, since=_START, until=_END, n_concurrent=1)
            await runner.run([(_OWNER, _REPO)])

        stored = {row["issue_number"] for row in upsert.await_args.args[1]}
        assert stored == {_CREATED_BEFORE}, "stage 2 dropped a PR merged inside the window"

        # The window is expressed as merged_at bounds, half-open.
        _, _, since, until = gh.fetch_merged_prs.await_args.args
        assert since.isoformat().startswith("2026-08-01")
        assert until.isoformat().startswith("2026-08-04")

    @pytest.mark.parametrize("stage", ["classify_prs", "resolve_packages", "render_problems"])
    async def test_pr_created_before_the_window_is_selected(self, stage: str) -> None:
        """Stages 3, 4 and 5 each select the PR that the old filter made invisible."""
        # Stage 3 selects PRs that are not yet classified; stages 4 and 5 select
        # the ones that are, so the fixture carries the state each stage needs.
        rows = _pull_requests(is_perf=None if stage == "classify_prs" else True)
        tables = {
            "pull_requests": rows,
            "packages": [] if stage == "resolve_packages" else _packages([f"sha{n}" for n in (1, 2, 3, 4)]),
            "candidate_prs": [],
            "repositories": [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}],
        }
        selection = await _run_stage(stage, tables)

        selected = selection.shas if stage == "resolve_packages" else selection.numbers
        expected: set[Any]
        if stage == "resolve_packages":
            expected = {f"sha{_CREATED_BEFORE}", f"sha{_LAST_SECOND}"}
        else:
            expected = {_CREATED_BEFORE, _LAST_SECOND}

        assert _selected_key(stage, _CREATED_BEFORE) in selected, (
            f"{stage} dropped the PR merged inside the window but created before it"
        )
        assert selected == expected

    @pytest.mark.parametrize("stage", ["classify_prs", "resolve_packages", "render_problems"])
    async def test_upper_bound_is_exclusive(self, stage: str) -> None:
        """A PR merged exactly at the end bound belongs to the next window."""
        rows = _pull_requests(is_perf=None if stage == "classify_prs" else True)
        tables = {
            "pull_requests": rows,
            "packages": [] if stage == "resolve_packages" else _packages([f"sha{n}" for n in (1, 2, 3, 4)]),
            "candidate_prs": [],
            "repositories": [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}],
        }
        selection = await _run_stage(stage, tables)
        selected = selection.shas if stage == "resolve_packages" else selection.numbers

        assert _selected_key(stage, _AT_END) not in selected, f"{stage} still uses an inclusive upper bound"
        assert _selected_key(stage, _LAST_SECOND) in selected, f"{stage} excluded the window's last second"
        assert _selected_key(stage, _MERGED_BEFORE) not in selected

    async def test_force_still_honours_the_window(self) -> None:
        """--force drops the resume predicate, leaving the window on its own.

        Stage 3 skips classified PRs with ``is_null``; --force removes that
        filter, so the merged_at bounds become the only thing selecting rows.
        Without this case the parametrised tests above never measure them
        unaccompanied.
        """
        tables = {
            "pull_requests": _pull_requests(is_perf=True),
            "packages": [],
            "candidate_prs": [],
            "repositories": [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}],
        }
        selection = await _run_stage("classify_prs", tables, force=True)
        assert selection.numbers == {_CREATED_BEFORE, _LAST_SECOND}


def _selected_key(stage: str, number: int) -> Any:
    """Stage 4 works in merge SHAs; the other two work in issue numbers."""
    return f"sha{number}" if stage == "resolve_packages" else number


class TestScopedSkipPredicates:
    """Skip-set reads are scoped server-side, not read whole and filtered here."""

    async def test_resolve_packages_scopes_the_packages_read(self) -> None:
        """Stage 4 asks for the window's SHAs, not for every packages row."""
        seen: list[dict[str, Any]] = []
        tables = {"pull_requests": _pull_requests(is_perf=True), "packages": []}
        inner = _fake_fetch_all(tables)

        def _recording(table: str, select: str = "*", **kw: Any) -> list[dict[str, Any]]:
            if table == "packages":
                seen.append(kw)
            return inner(table, select, **kw)  # type: ignore[no-any-return]

        pipeline = Pipeline(dry_run=True)
        pipeline._log_dry_run_summary = _Selection()  # type: ignore[method-assign]
        with patch("datasmith.update.pipeline.fetch_all", _recording):
            await pipeline._resolve_packages(_START, _END)

        assert seen, "stage 4 never read the packages table"
        for kw in seen:
            assert "in_filters" in kw and "sha" in kw["in_filters"], "the packages read is not key-scoped"
            assert set(kw["in_filters"]["sha"]) <= {f"sha{_CREATED_BEFORE}", f"sha{_LAST_SECOND}"}

    async def test_render_problems_scopes_every_read(self) -> None:
        """Stage 5's packages, candidate_prs and repositories reads are all scoped."""
        seen: dict[str, list[dict[str, Any]]] = {}
        tables = {
            "pull_requests": _pull_requests(is_perf=True),
            "packages": _packages([f"sha{_CREATED_BEFORE}", f"sha{_LAST_SECOND}"]),
            "candidate_prs": [],
            "repositories": [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}],
        }
        inner = _fake_fetch_all(tables)

        def _recording(table: str, select: str = "*", **kw: Any) -> list[dict[str, Any]]:
            seen.setdefault(table, []).append(kw)
            return inner(table, select, **kw)  # type: ignore[no-any-return]

        pipeline = Pipeline(dry_run=True)
        selection = _Selection()
        pipeline._log_dry_run_summary = selection  # type: ignore[method-assign]
        with patch("datasmith.update.pipeline.fetch_all", _recording):
            await pipeline._render_problems(_START, _END)

        assert selection.numbers == {_CREATED_BEFORE, _LAST_SECOND}
        for table in ("packages", "candidate_prs", "repositories"):
            assert table in seen, f"stage 5 never read {table}"
            for kw in seen[table]:
                assert kw.get("in_filters"), f"the {table} read is unscoped"

    async def test_repo_descriptions_are_not_read_whole(self) -> None:
        """_fetch_repo_descriptions names the owners and repos it wants."""
        from datasmith.update.pipeline import _fetch_repo_descriptions

        seen: list[dict[str, Any]] = []

        def _recording(table: str, select: str = "*", **kw: Any) -> list[dict[str, Any]]:
            seen.append(kw)
            return [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}]

        with patch("datasmith.update.pipeline.fetch_all", _recording):
            got = _fetch_repo_descriptions([{"owner": _OWNER, "repo": _REPO}])

        assert got == {(_OWNER, _REPO): "data frames"}
        assert seen[0]["in_filters"] == {"owner": [_OWNER], "repo": [_REPO]}

    async def test_key_reads_are_chunked(self) -> None:
        """A long key list is split, because PostgREST puts it in the URL."""
        from datasmith.update import pipeline as pipeline_mod

        seen: list[list[str]] = []

        def _recording(table: str, select: str = "*", **kw: Any) -> list[dict[str, Any]]:
            seen.append(list(kw["in_filters"]["sha"]))
            return []

        values = [f"sha{i}" for i in range(250)]
        with (
            patch.object(pipeline_mod, "DATASMITH_KEY_FILTER_CHUNK", 100),
            patch("datasmith.update.pipeline.fetch_all", _recording),
        ):
            pipeline_mod._fetch_scoped("packages", "owner, repo, sha", "sha", values)

        assert [len(chunk) for chunk in seen] == [100, 100, 50]
        assert [sha for chunk in seen for sha in chunk] == values
