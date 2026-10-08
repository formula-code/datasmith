"""The ingestion window means the same thing in every stage (spec section 7.3).

Stage 2 windowed ``merged_at``, half-open.  Stages 3, 4 and 5 windowed
``created_at``, inclusive at both ends.  The two defects masked each other
exactly: because stage 2 only ever stored PRs both created *and* merged inside
the window, no stored row could have ``created_at`` before the window, so the
column mismatch was invisible and every stage-2-only test still passed.

Stages 6 and 7 windowed ``created_at`` inclusively too, and stage 8 windowed
``merged_at`` inclusively — four conventions for one question, hand-written
seven times.  There was no single place the contract lived, so there was
nothing for a test to point at.  Now there is one:
:func:`datasmith.utils.db.window_filters`.

That is why this file spans the stages rather than checking one.  It has two
halves:

* a behavioural half, asserting that one PR — merged inside the window,
  created a month before it — is selected by stages 2 through 8 alike, and
  that the upper bound is exclusive in all of them;
* a structural half, which scans every module under ``src/datasmith`` for a
  hand-written range bound on a timestamp and requires the ones it finds to be
  exactly the audited exemptions.  The behavioural half only covers the stages
  someone remembered to parametrise, so it is the structural half that catches
  the eighth copy — and there already was an eighth, in
  ``runners/synthesize_images.py``, which asks a genuinely different question
  and is exempted by name rather than by having been overlooked.

The fake ``fetch_all`` here evaluates the predicates against in-memory rows
instead of recording the kwargs it was called with.  Asserting on kwargs would
pass just as happily against a filter that selects the wrong rows.
"""

from __future__ import annotations

import ast
import operator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datasmith
from datasmith.update.pipeline import Pipeline
from datasmith.utils.db import WINDOW_COLUMN, window_filters

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


def _pr(
    number: int,
    created_at: str,
    merged_at: str,
    *,
    is_perf: bool | None,
    container: str | None = None,
) -> dict[str, Any]:
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
        "base_sha": f"base{number}",
        "rendered_problem": "make it fast",
        # Stage 6 selects PRs that have no container yet; stage 7 and stage 8
        # select the ones that do.
        "container_name": container,
        "is_performance_commit": is_perf,
        "is_performance_commit_symbolic": True,
    }


def _pull_requests(is_perf: bool | None, container: str | None = None) -> list[dict[str, Any]]:
    """The four-row fixture, with whatever classification state a stage needs."""
    return [
        _pr(_CREATED_BEFORE, "2026-07-01T09:00:00Z", "2026-08-02T10:00:00Z", is_perf=is_perf, container=container),
        _pr(_LAST_SECOND, "2026-08-03T09:00:00Z", "2026-08-03T23:59:59Z", is_perf=is_perf, container=container),
        _pr(_AT_END, "2026-08-03T09:00:00Z", "2026-08-04T00:00:00Z", is_perf=is_perf, container=container),
        _pr(_MERGED_BEFORE, "2026-07-01T09:00:00Z", "2026-07-31T23:00:00Z", is_perf=is_perf, container=container),
    ]


def _ts(value: Any) -> Any:
    """Read a bound the way Postgres does, not the way ``str`` does.

    A range bound is written as a bare date (``2026-08-04``) and compared
    against a full timestamp (``2026-08-04T00:00:00Z``). Postgres casts the
    bare date to midnight, so those two are *equal* and only ``lt`` excludes
    the row. Compared as strings the longer one sorts after the shorter, so an
    inclusive bound would look exclusive and this file would pass against the
    very bug it exists to catch. Anything unparseable is left alone.
    """
    if not isinstance(value, str):
        return value
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    # A bare date parses naive and a timestamp parses aware; the column is
    # ``timestamptz``, so the bound is read as UTC.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _cmp(row: dict[str, Any], col: str, val: Any, op: Any) -> bool:
    """Apply one range predicate, comparing as timestamps where both sides parse."""
    got = row.get(col)
    if got is None:
        return False
    left, right = _ts(got), _ts(val)
    if isinstance(left, datetime) != isinstance(right, datetime):
        left, right = got, val
    return bool(op(left, right))


# One predicate per ``fetch_all`` kwarg. Each returns True when the row
# survives that kwarg.
_PREDICATES: dict[str, Any] = {
    "filters": lambda row, col, val: row.get(col) == val,
    "gte_filters": lambda row, col, val: _cmp(row, col, val, operator.ge),
    "lte_filters": lambda row, col, val: _cmp(row, col, val, operator.le),
    "lt_filters": lambda row, col, val: _cmp(row, col, val, operator.lt),
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


def _contexts(numbers: list[int]) -> list[dict[str, Any]]:
    """Stage 5 output: the rendered context stage 6 refuses to build without."""
    return [
        {
            "owner": _OWNER,
            "repo": _REPO,
            "issue_number": n,
            "issues_json": [{"number": n + 1000}],
            "initial_observations": "it is slow",
        }
        for n in numbers
    ]


# Every stage that windows a database read. Stage 2 windows GitHub instead and
# has its own test; stage 8 windows inside ``records_from_supabase`` and has
# its own class; stage 9 opts out on purpose.
_WINDOWED_STAGES = [
    "classify_prs",
    "resolve_packages",
    "render_problems",
    "synthesize_images",
    "harbor_healthcheck",
]


def _tables_for(stage: str) -> dict[str, list[dict[str, Any]]]:
    """The fixture that satisfies every predicate *stage* applies but the window.

    All four PRs clear the stage's other gates, so a row missing from the
    selection was dropped by the window and by nothing else. Without that,
    a stage could pass by selecting nothing at all.
    """
    # Stage 3 selects PRs that are not yet classified; the rest select the
    # ones that are.
    is_perf = None if stage == "classify_prs" else True
    # Stage 7 wants a container from stage 6; stage 6 wants rows without one.
    container = "docker.io/formulacode/img:1" if stage == "harbor_healthcheck" else None
    rows = _pull_requests(is_perf=is_perf, container=container)
    shas = [str(r["merge_commit_sha"]) for r in rows]
    numbers = [int(r["issue_number"]) for r in rows]
    return {
        "pull_requests": rows,
        # Stage 4 *writes* packages rows, so it must find none; stages 5 and 6
        # gate on them already existing.
        "packages": [] if stage == "resolve_packages" else _packages(shas),
        # Stage 5 treats candidate_prs as a skip set (a rendered PR is done);
        # stage 6 treats it as a precondition.
        "candidate_prs": _contexts(numbers) if stage == "synthesize_images" else [],
        "repositories": [{"owner": _OWNER, "repo": _REPO, "description": "data frames"}],
        # Stage 7 skips PRs that already ran; none have.
        "harbor_runs": [],
    }


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

    @pytest.mark.parametrize("stage", _WINDOWED_STAGES)
    async def test_pr_created_before_the_window_is_selected(self, stage: str) -> None:
        """Stages 3 to 7 each select the PR that the old filter made invisible."""
        selection = await _run_stage(stage, _tables_for(stage))

        selected = selection.shas if stage == "resolve_packages" else selection.numbers
        expected: set[Any] = {
            _selected_key(stage, _CREATED_BEFORE),
            _selected_key(stage, _LAST_SECOND),
        }

        assert _selected_key(stage, _CREATED_BEFORE) in selected, (
            f"{stage} dropped the PR merged inside the window but created before it"
        )
        assert selected == expected

    @pytest.mark.parametrize("stage", _WINDOWED_STAGES)
    async def test_upper_bound_is_exclusive(self, stage: str) -> None:
        """A PR merged exactly at the end bound belongs to the next window."""
        selection = await _run_stage(stage, _tables_for(stage))
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

    def test_the_helper_is_the_definition(self) -> None:
        """The helper itself says merged_at, gte on the low side, strict lt on the high.

        Every other assertion in this file is only as good as this one: they
        all reach the database through ``window_filters``.
        """
        assert WINDOW_COLUMN == "merged_at"
        assert window_filters(_START, _END) == {
            "gte_filters": {"merged_at": _START},
            "lt_filters": {"merged_at": _END},
        }
        # An absent bound is omitted, not guessed at, so one open side does not
        # quietly change what the other side means.
        assert window_filters(None, _END) == {"lt_filters": {"merged_at": _END}}
        assert window_filters(_START, None) == {"gte_filters": {"merged_at": _START}}
        assert window_filters(None, None) == {}


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


class TestPublishWindow:
    """Stage 8's window lives in ``records_from_supabase``, not in ``_publish``.

    ``_publish`` delegates — both its real and its dry-run branch now go
    through ``records_from_supabase`` — so driving it through
    :func:`_run_stage` would measure the delegation rather than the window.
    This class patches the read stage 8 actually windows.
    """

    def _published(self, **kwargs: Any) -> set[int]:
        from datasmith.publish.records import records_from_supabase

        rows = _pull_requests(is_perf=True, container="docker.io/formulacode/img:1")
        tables = {
            "pull_requests": rows,
            # Every PR clears the Daytona speedup gate AND has a verified
            # container, so the only thing that can drop one here is the
            # window. Both gates are satisfied for every row on purpose:
            # a window test that also exercised the publish gates could not
            # tell which one rejected a PR.
            "harbor_runs": [
                {
                    "owner": _OWNER,
                    "repo": _REPO,
                    "sha": str(r["merge_commit_sha"]),
                    "max_speedup": 2.0,
                    "status": "success",
                    "environment": "daytona",
                }
                for r in rows
            ],
            "candidate_containers": [
                {
                    "owner": _OWNER,
                    "repo": _REPO,
                    "sha": str(r["merge_commit_sha"]),
                    "verification_state": "verified",
                }
                for r in rows
            ],
        }
        with patch("datasmith.publish.records.fetch_all", _fake_fetch_all(tables)):
            records = records_from_supabase(start_date=_START, end_date=_END, **kwargs)
        return {r.issue_number for r in records}

    def test_pr_created_before_the_window_is_published(self) -> None:
        """Stage 8 publishes the PR that a created_at window would have hidden."""
        assert _CREATED_BEFORE in self._published()

    def test_upper_bound_is_exclusive(self) -> None:
        """A PR merged at exactly the end bound belongs to next month's release.

        Stage 8's inclusive upper bound never actually double-published, because
        ``published_at IS NULL`` caught the repeat. That is a second guard
        covering for a window that meant the wrong thing — so this asserts with
        the cover removed.
        """
        assert self._published(unpublished_only=False) == {_CREATED_BEFORE, _LAST_SECOND}


# --- the structural half -------------------------------------------------
#
# The behavioural tests above only cover the stages somebody remembered to
# parametrise, and the original defect survived precisely because no test knew
# the full set. So this half does not take a list of files to check: it scans
# every module under ``src/datasmith`` and requires the offenders it finds to
# be exactly the ones audited below. A new copy of the window, anywhere, fails
# by default rather than needing somebody to remember to add it here.

# The functions that must reach the database through the helper.
_MUST_USE_HELPER: tuple[tuple[str, str], ...] = (
    ("update/pipeline.py", "_classify_prs"),
    ("update/pipeline.py", "_resolve_packages"),
    ("update/pipeline.py", "_render_problems"),
    ("update/pipeline.py", "_synthesize_images"),
    ("update/pipeline.py", "_harbor_healthcheck"),
    ("publish/records.py", "records_from_supabase"),
    ("runners/scrape_commits.py", "_existing_issue_numbers"),
)

# ``fetch_all`` kwargs that express a range bound. A window is made of these,
# so a hand-written one is a second definition of the window.
_BOUND_KWARGS = frozenset({"gte_filters", "lte_filters", "lt_filters"})

# The columns a run window can be built out of. A range bound on ``max_speedup``
# or ``duration_s`` is not a window and is none of this test's business; a range
# bound on one of these two is a window whether or not its author meant it as one.
_WINDOW_CAPABLE_COLUMNS = frozenset({"created_at", "merged_at"})

# Sites that range-filter a timestamp and are *not* the run window. Keyed by
# function, not by file, so a genuine second copy of the window in the same
# module still fails. Each entry has to earn its reason.
_AUDITED_EXEMPTIONS: dict[tuple[str, str], str] = {
    ("runners/synthesize_images.py", "_fetch_neighbor_items"): (
        "asks a different question: a symmetric band of +/- "
        "DATASMITH_NEIGHBOR_WINDOW_DAYS around one PR, to find the PRs likely "
        "to share the environment just cached for it. It is meant to reach "
        "outside the run's window, so routing it through window_filters would "
        "break the feature rather than fix it."
    ),
}

_SRC = Path(datasmith.__file__).resolve().parent


def _module(relpath: str) -> ast.Module:
    return ast.parse((_SRC / relpath).read_text(encoding="utf-8"))


def _function(relpath: str, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    for node in ast.walk(_module(relpath)):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{relpath} has no function named {name}")


def _dict_keys(node: ast.AST) -> set[str]:
    """The literal string keys of a dict display, or nothing if it is not one."""
    if not isinstance(node, ast.Dict):
        return set()
    return {k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)}


def _bound_columns_here(node: ast.AST) -> set[str]:
    """Columns named by a range bound written *at* this node, not below it.

    Catches both spellings a caller can use: the keyword form
    ``fetch_all(..., gte_filters={"merged_at": start})`` and the dict-literal
    form ``{"gte_filters": {"merged_at": start}, ...}`` that stages building
    their kwargs up front use. A column reached through ``window_filters``
    appears in neither, which is the whole point of the helper -- and the
    helper's own ``{WINDOW_COLUMN: start_date}`` is a Name, not a string
    literal, so the definition does not report itself as a copy of itself.
    """
    if isinstance(node, ast.keyword) and node.arg in _BOUND_KWARGS:
        return _dict_keys(node.value)
    if isinstance(node, ast.Dict):
        columns: set[str] = set()
        for key, value in zip(node.keys, node.values, strict=False):
            if isinstance(key, ast.Constant) and key.value in _BOUND_KWARGS:
                columns |= _dict_keys(value)
        return columns
    return set()


def _bound_sites() -> dict[tuple[str, str], set[str]]:
    """Every hand-written timestamp bound in ``src/datasmith``, by function.

    Attribution is to the innermost enclosing function, so an exemption cannot
    quietly cover a whole module. A bound written at module scope is reported
    against ``<module>`` rather than being skipped.

    Known limit: only the two literal spellings are visible. A bound built
    indirectly -- ``gte_filters=some_var``, or ``kwargs["gte_filters"] = ...``
    -- is not seen, which is also why ``window_filters`` does not report itself.
    For the seven known stages ``_MUST_USE_HELPER`` covers that gap from the
    other side: switching one of them to an indirect bound stops it calling the
    helper, and that test fails. A brand-new function using an indirect bound is
    the case neither test sees.
    """
    sites: dict[tuple[str, str], set[str]] = {}

    def walk(node: ast.AST, relpath: str, where: str) -> None:
        for child in ast.iter_child_nodes(node):
            name = child.name if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) else where
            columns = _bound_columns_here(child) & _WINDOW_CAPABLE_COLUMNS
            if columns:
                sites.setdefault((relpath, name), set()).update(columns)
            walk(child, relpath, name)

    for path in sorted(_SRC.rglob("*.py")):
        relpath = path.relative_to(_SRC).as_posix()
        walk(_module(relpath), relpath, "<module>")
    return sites


def _calls(node: ast.AST) -> set[str]:
    """The names of every function called under *node*."""
    return {
        sub.func.id if isinstance(sub.func, ast.Name) else sub.func.attr
        for sub in ast.walk(node)
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name | ast.Attribute)
    }


class TestOneWindowDefinition:
    """No module may re-implement the window, and none may window created_at.

    This is the test whose absence let the original defect live for months.
    Four conventions coexisted in seven hand-written copies, and every one of
    them looked locally reasonable; nothing compared them, because there was no
    definition to compare them against.
    """

    def test_the_only_hand_written_bounds_are_audited(self) -> None:
        """Every timestamp bound in src/ is either the helper's, or listed with a reason.

        Written as one equality rather than a per-file loop because both
        directions matter. A *new* site is the eighth copy of the window and
        must fail; a *vanished* site means the exemption has rotted into a
        blanket that no longer covers anything, and it must be deleted rather
        than left standing to excuse whatever lands there next.
        """
        found = _bound_sites()
        assert set(found) == set(_AUDITED_EXEMPTIONS), (
            "hand-written timestamp bounds in src/datasmith changed.\n"
            f"  found:    {sorted(found)}\n"
            f"  audited:  {sorted(_AUDITED_EXEMPTIONS)}\n"
            "Use datasmith.utils.db.window_filters, or add the site to "
            "_AUDITED_EXEMPTIONS with the reason it is not the run window."
        )

    def test_no_stage_windows_created_at(self) -> None:
        """``created_at`` is when a PR was opened, never which run owns it.

        Stages 3-7 windowed it for months. The one site still allowed to
        range-filter it is not selecting a run's work at all.
        """
        offenders = {site: cols for site, cols in _bound_sites().items() if "created_at" in cols}
        assert set(offenders) <= set(_AUDITED_EXEMPTIONS), (
            f"{sorted(set(offenders) - set(_AUDITED_EXEMPTIONS))} range-filter created_at; "
            "the run window is merged_at -- see datasmith.utils.db.window_filters"
        )

    def test_the_neighbour_exemption_is_real(self) -> None:
        """The exemption must name a function that exists and still has a bound.

        Without this the entry could outlive the code it excuses and sit there
        looking like an audit somebody performed.
        """
        for relpath, name in _AUDITED_EXEMPTIONS:
            node = _function(relpath, name)  # raises if it no longer exists
            assert (relpath, name) in _bound_sites(), (
                f"{relpath}::{name} no longer range-filters a timestamp; drop its _AUDITED_EXEMPTIONS entry"
            )
            assert "window_filters" not in _calls(node), (
                f"{relpath}::{name} now uses the helper, so it is no longer an exemption"
            )

    @pytest.mark.parametrize(("relpath", "name"), _MUST_USE_HELPER)
    def test_every_windowing_stage_calls_the_helper(self, relpath: str, name: str) -> None:
        """A stage that stopped windowing entirely would pass the two tests above."""
        assert "window_filters" in _calls(_function(relpath, name)), (
            f"{relpath}::{name} no longer calls window_filters, so it selects rows from every run"
        )

    def test_publish_delegates_rather_than_inventing_a_fourth_convention(self) -> None:
        """``_publish`` has no window of its own, and says where its window is."""
        node = _function("update/pipeline.py", "_publish")
        assert "window_filters" not in _calls(node)
        assert "records_from_supabase" in ast.get_source_segment(
            (_SRC / "update/pipeline.py").read_text(encoding="utf-8"), node
        ), "_publish must name the function that applies its window"

    def test_stage_9_opts_out_explicitly(self) -> None:
        """Stage 9 wants the whole corpus, and must say so in the helper's own name.

        Its opt-out used to be invisible: a stage with no bounds looks exactly
        like a stage that forgot them. Naming ``window_filters`` in the comment
        is what makes ``grep -rn window_filters src/`` return the opt-out site
        along with the seven uses.
        """
        source = (_SRC / "update/pipeline.py").read_text(encoding="utf-8")
        node = _function("update/pipeline.py", "_scrape_benchmark_source")
        segment = ast.get_source_segment(source, node) or ""
        assert "window_filters" in segment, "stage 9's opt-out must name the helper it is declining to use"
        assert "window_filters" not in _calls(node), "stage 9 is documented as unwindowed but windows anyway"

    def test_stage_2_carries_the_same_bounds_to_github(self) -> None:
        """Stage 2's window is a search query, so it is the one path the helper cannot serve.

        It must still mean the same thing, so the check is that the untouched
        ``start_date``/``end_date`` reach the runner as ``since``/``until``.
        """
        source = (_SRC / "update/pipeline.py").read_text(encoding="utf-8")
        node = _function("update/pipeline.py", "_scrape_commits")
        segment = ast.get_source_segment(source, node) or ""
        assert '{"since": start_date, "until": end_date}' in segment
        assert "window_filters" in segment, "stage 2 must say which contract its GitHub query implements"
