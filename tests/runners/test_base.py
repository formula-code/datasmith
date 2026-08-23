"""Tests for datasmith.runners.base — BaseRunner with progress tracking."""

from __future__ import annotations

import ast
import asyncio
import logging
import re
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datasmith.runners.base import BaseRunner


class _DummyRunner(BaseRunner):
    """Concrete implementation for testing."""

    def __init__(self, n_concurrent: int = 10, side_effect: dict[int, Exception] | None = None) -> None:
        super().__init__(name="test_runner", n_concurrent=n_concurrent)
        self._side_effect = side_effect or {}
        self.processed: list[Any] = []

    async def _process_item(self, item: Any) -> None:
        if item in self._side_effect:
            raise self._side_effect[item]
        self.processed.append(item)


def _mock_supabase() -> MagicMock:
    """Create a mock Supabase client with fluent API."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


class TestProgressTracking:
    async def test_progress_tracking(self) -> None:
        """Run 5 items, verify Supabase upsert calls for progress."""
        mock_client = _mock_supabase()

        with patch("datasmith.runners.base.get_client", return_value=mock_client):
            runner = _DummyRunner()
            await runner.run([1, 2, 3, 4, 5])

        assert runner._completed == 5
        assert runner._failed == 0

        # Verify runner_progress table was written to (init + updates)
        progress_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "runner_progress"]
        assert len(progress_calls) >= 2  # At least init + final

    async def test_failure_logging(self) -> None:
        """2 items raise, verify runner_failures insert calls."""
        mock_client = _mock_supabase()

        side_effects = {1: ValueError("boom1"), 3: RuntimeError("boom3")}
        runner = _DummyRunner(side_effect=side_effects)

        with patch("datasmith.runners.base.get_client", return_value=mock_client):
            await runner.run([0, 1, 2, 3, 4])

        assert runner._completed == 3
        assert runner._failed == 2

        # Verify runner_failures insert calls
        failure_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "runner_failures"]
        assert len(failure_calls) == 2


class TestConcurrency:
    async def test_semaphore_bounds_concurrency(self) -> None:
        """n_concurrent=2 — verify at most 2 items run concurrently."""
        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        class _ConcurrencyTracker(BaseRunner):
            async def _process_item(self, item: Any) -> None:
                nonlocal max_concurrent, current_concurrent
                async with lock:
                    current_concurrent += 1
                    if current_concurrent > max_concurrent:
                        max_concurrent = current_concurrent
                await asyncio.sleep(0.05)
                async with lock:
                    current_concurrent -= 1

        mock_client = _mock_supabase()

        with patch("datasmith.runners.base.get_client", return_value=mock_client):
            runner = _ConcurrencyTracker(name="concurrency_test", n_concurrent=2)
            await runner.run([1, 2, 3, 4, 5])

        assert max_concurrent <= 2


class TestItemFailureResilience:
    async def test_item_failure_doesnt_abort(self) -> None:
        """Item 3 raises, but items 4 and 5 are still processed."""
        mock_client = _mock_supabase()

        side_effects = {3: ValueError("item 3 failed")}
        runner = _DummyRunner(n_concurrent=1, side_effect=side_effects)

        with patch("datasmith.runners.base.get_client", return_value=mock_client):
            await runner.run([1, 2, 3, 4, 5])

        assert runner._completed == 4
        assert runner._failed == 1
        # Items 1, 2, 4, 5 should all have been processed
        assert set(runner.processed) == {1, 2, 4, 5}


class TestEndOfStageSummary:
    """Silence must stop meaning success.

    The motivating run reported "154/154 repositories, zero failures" while
    storing 35 PRs.  The stage had no way to say that was wrong, so every run
    now ends with the counts and the error types behind them — including the
    runs where nothing raised, because that run *had* no failures.
    """

    async def test_a_clean_run_still_says_what_it_did(self, caplog: pytest.LogCaptureFixture) -> None:
        mock_client = _mock_supabase()
        runner = _DummyRunner(n_concurrent=2)

        with (
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            caplog.at_level(logging.INFO, logger="datasmith.runners.base"),
        ):
            await runner.run([1, 2, 3])

        summaries = [r.getMessage() for r in caplog.records if "finished" in r.getMessage()]
        assert len(summaries) == 1, summaries
        assert "test_runner finished: 3 item(s), 3 succeeded, 0 failed" in summaries[0]
        assert "error types: none" in summaries[0]

    async def test_the_summary_names_the_distinct_error_types(self, caplog: pytest.LogCaptureFixture) -> None:
        mock_client = _mock_supabase()
        side_effects: dict[int, Exception] = {
            1: ValueError("bad"),
            2: ValueError("also bad"),
            3: KeyError("missing"),
        }
        runner = _DummyRunner(n_concurrent=1, side_effect=side_effects)

        with (
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            caplog.at_level(logging.INFO, logger="datasmith.runners.base"),
        ):
            await runner.run([1, 2, 3, 4, 5])

        summary = next(r.getMessage() for r in caplog.records if "finished" in r.getMessage())
        assert "5 item(s), 2 succeeded, 3 failed" in summary
        assert "ValueError=2" in summary
        assert "KeyError=1" in summary

    async def test_error_types_do_not_leak_between_runs(self, caplog: pytest.LogCaptureFixture) -> None:
        """A second run reports its own outcome, not the first run's plus it."""
        mock_client = _mock_supabase()
        runner = _DummyRunner(n_concurrent=1, side_effect={1: ValueError("bad")})

        with patch("datasmith.runners.base.get_client", return_value=mock_client):
            await runner.run([1, 2])
            runner._side_effect = {}
            with caplog.at_level(logging.INFO, logger="datasmith.runners.base"):
                await runner.run([3, 4])

        # The last summary is the second run's; the first run's is still in the
        # record list, which is exactly what makes leaking counters detectable.
        summary = [r.getMessage() for r in caplog.records if "finished" in r.getMessage()][-1]
        assert "2 item(s), 2 succeeded, 0 failed" in summary
        assert "error types: none" in summary


class TestRunnerIdFormat:
    def test_runner_id_format(self) -> None:
        """Verify runner_id format is '{name}-{8 hex chars}'."""
        runner = _DummyRunner()
        assert re.match(r"^test_runner-[0-9a-f]{8}$", runner.runner_id)


class TestEverySubclassReportsItsOutcome:
    """The summary is only worth having if no stage can quietly skip it.

    ``_log_summary`` lives in ``BaseRunner.run``'s ``finally``, so a subclass
    that *replaces* ``run`` rather than extending it loses the summary in
    silence.  That is exactly what happened to stage 6, the longest-running
    and most failure-prone stage: it fully overrode ``run`` for its queue-based
    worker pool and ended with no total/succeeded/failed line, while
    ``base.py``'s docstring claimed every run ends with one.

    Testing ``BaseRunner`` directly cannot notice that, so this walks the real
    source instead and fails closed on a new override.
    """

    @staticmethod
    def _run_overrides() -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
        """Map ``module.Class`` -> the ``run`` it defines, for BaseRunner subclasses."""
        import datasmith.runners as runners_pkg

        found: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
        root = Path(runners_pkg.__file__).parent
        for path in sorted(root.glob("*.py")):
            if path.name in {"__init__.py", "base.py"}:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ClassDef):
                    continue
                if not any(isinstance(b, ast.Name) and b.id == "BaseRunner" for b in node.bases):
                    continue
                for child in node.body:
                    if isinstance(child, ast.AsyncFunctionDef | ast.FunctionDef) and child.name == "run":
                        found[f"{path.stem}.{node.name}"] = child
        return found

    def test_at_least_one_override_exists(self) -> None:
        """Guard the guard: if nothing is found, the scan silently proves nothing."""
        assert self._run_overrides(), "found no BaseRunner.run overrides — the AST scan is broken"

    def test_every_run_override_still_reports(self) -> None:
        """An override must delegate to ``super().run`` or log the summary itself."""
        offenders = []
        for qualname, fn in self._run_overrides().items():
            calls = {
                ast.unparse(n.func)
                for n in ast.walk(fn)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            }
            delegates = any(c.startswith("super().run") for c in calls)
            reports = "self._log_summary" in calls
            if not (delegates or reports):
                offenders.append(qualname)
        assert not offenders, (
            f"{offenders} override BaseRunner.run without calling super().run() or "
            "self._log_summary(), so those stages end without a total/succeeded/failed line"
        )

    def test_self_reporting_overrides_also_reset_error_types(self) -> None:
        """An override that owns its loop must clear the counter it fills.

        Without the reset the breakdown accumulates across runs, which is the
        same defect ``test_error_types_do_not_leak_between_runs`` pins for the
        base class.
        """
        offenders = []
        for qualname, fn in self._run_overrides().items():
            calls = {
                ast.unparse(n.func)
                for n in ast.walk(fn)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            }
            if any(c.startswith("super().run") for c in calls):
                continue  # super() does the reset
            body = ast.unparse(fn)
            if "self._error_types" not in body:
                offenders.append(qualname)
        assert not offenders, f"{offenders} run their own loop but never reset self._error_types"
