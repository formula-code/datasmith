"""Tests for datasmith.runners.resolve_packages — ResolvePackagesRunner.

Stage 4 clones repositories and runs ``uv pip compile``, so its parallelism is
a property of the machine.  It used to run on ``run_in_executor(None, ...)``,
the interpreter's default pool sized ``min(32, cpu_count + 4)`` — which means
something different on every host and becomes 32 concurrent clones on a
128-core one.  These tests pin the pool down: explicit, bounded, owned, and
shut down even when the stage raises.
"""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datasmith.runners.base import BaseRunner
from datasmith.runners.resolve_packages import ResolvePackagesRunner


def _mock_supabase() -> MagicMock:
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _item(sha: str = "abc123def456") -> dict[str, Any]:
    return {"owner": "pandas-dev", "repo": "pandas", "sha": sha}


def _result() -> dict[str, Any]:
    return {
        "package_name": "pandas",
        "package_version": "2.2.0",
        "python_version": "3.11",
        "final_dependencies": ["numpy==1.26.4"],
        "build_command": ["pip install -e ."],
        "install_command": ["pip install -e ."],
        "primary_root": ".",
        "resolution_strategy": "pyproject",
        "can_install": True,
    }


class TestBoundedThreadPool:
    async def test_work_runs_on_the_runners_own_pool(self) -> None:
        """Not the shared default executor: ``None`` is what made the size implicit."""
        seen: list[Any] = []
        client = _mock_supabase()
        runner = ResolvePackagesRunner(n_concurrent=1, max_workers=1)

        loop = asyncio.get_running_loop()
        original = loop.run_in_executor

        def _spy(executor: Any, func: Any, *args: Any) -> Any:
            seen.append(executor)
            return original(executor, func, *args)

        with (
            patch("datasmith.runners.resolve_packages.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.resolution.analyze_commit", return_value=_result()),
            patch.object(loop, "run_in_executor", _spy),
        ):
            await runner.run([_item()])

        assert seen, "stage 4 never dispatched to an executor"
        assert None not in seen, "stage 4 still uses the shared default executor"
        assert all(isinstance(ex, ThreadPoolExecutor) for ex in seen), seen

    def test_pool_size_comes_from_the_env_knob(self) -> None:
        """The cap is a DATASMITH_ constant, not a function of cpu_count()."""
        with patch("datasmith.runners.resolve_packages.DATASMITH_RESOLVE_PACKAGES_WORKERS", 3):
            assert ResolvePackagesRunner()._max_workers == 3

        # An explicit argument still wins, for a caller that measured its disk.
        assert ResolvePackagesRunner(max_workers=2)._max_workers == 2
        # A nonsense value cannot produce a zero-worker pool that hangs.
        assert ResolvePackagesRunner(max_workers=0)._max_workers == 1

    async def test_concurrent_clones_are_capped_by_the_pool(self) -> None:
        """n_concurrent is a queue depth; the pool is what the disk has to survive."""
        peak = 0
        live = 0
        guard = threading.Lock()

        def _analyze(sha: str, repo: str) -> dict[str, Any]:
            nonlocal peak, live
            with guard:
                live += 1
                peak = max(peak, live)
            time.sleep(0.02)
            with guard:
                live -= 1
            return _result()

        client = _mock_supabase()
        runner = ResolvePackagesRunner(n_concurrent=10, max_workers=2)
        with (
            patch("datasmith.runners.resolve_packages.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.resolution.analyze_commit", _analyze),
        ):
            await runner.run([_item(f"sha{i}") for i in range(10)])

        assert peak <= 2, f"{peak} concurrent resolutions against a pool of 2"

    async def test_pool_is_shut_down_even_when_the_stage_raises(self) -> None:
        """A failed run must not leave clone threads alive behind it."""
        runner = ResolvePackagesRunner(n_concurrent=1, max_workers=1)
        captured: list[ThreadPoolExecutor] = []

        async def _boom(self: BaseRunner, items: list[Any]) -> None:
            assert runner._executor is not None
            captured.append(runner._executor)
            raise RuntimeError("stage aborted")

        with patch.object(BaseRunner, "run", _boom), pytest.raises(RuntimeError, match="stage aborted"):
            await runner.run([_item()])

        assert runner._executor is None, "the pool outlived the run"
        with pytest.raises(RuntimeError):
            captured[0].submit(int)

    async def test_pool_is_shut_down_after_a_normal_run(self) -> None:
        client = _mock_supabase()
        runner = ResolvePackagesRunner(n_concurrent=1, max_workers=1)
        with (
            patch("datasmith.runners.resolve_packages.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.resolution.analyze_commit", return_value=_result()),
        ):
            await runner.run([_item()])
        assert runner._executor is None


class TestPersistence:
    async def test_resolution_row_is_upserted(self) -> None:
        client = _mock_supabase()
        runner = ResolvePackagesRunner(n_concurrent=1, max_workers=1)
        with (
            patch("datasmith.runners.resolve_packages.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.resolution.analyze_commit", return_value=_result()),
        ):
            await runner.run([_item()])

        # runner_progress shares the mocked table object, so pick the payload
        # that carries a resolution rather than the most recent upsert.
        rows = [c.args[0] for c in client.table.return_value.upsert.call_args_list if "sha" in c.args[0]]
        assert len(rows) == 1
        row = rows[0]
        assert row["owner"] == "pandas-dev"
        assert row["sha"] == "abc123def456"
        assert row["env_payload"] == '["numpy==1.26.4"]'

    async def test_none_result_writes_nothing(self) -> None:
        client = _mock_supabase()
        runner = ResolvePackagesRunner(n_concurrent=1, max_workers=1)
        with (
            patch("datasmith.runners.resolve_packages.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.resolution.analyze_commit", return_value=None),
        ):
            await runner.run([_item()])

        assert not any(c.args[0] == "packages" for c in client.table.call_args_list)
