"""Tests for datasmith.runners.resolve_packages.

Two things are pinned here, because two separate changes converged on this file.

The runner must persist every field the resolver produces — the predecessor
hardcoded ``requires_python`` to ``None`` while the parsed value was computed
upstream and discarded, and wrote three columns nothing ever read.

And its parallelism must stay a property the operator sets. Stage 4 clones
repositories and runs ``uv pip compile``, so it used to run on
``run_in_executor(None, ...)`` — the interpreter's default pool, sized
``min(32, cpu_count + 4)``, which means something different on every host and
becomes 32 concurrent clones on a 128-core one. The pool is explicit, bounded,
owned, and shut down even when the stage raises.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datasmith.resolution.orchestrator import RESOLVER_VERSION, ResolutionResult
from datasmith.runners.base import BaseRunner
from datasmith.runners.resolve_packages import ResolvePackagesRunner, build_row


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


def _result(**kw: Any) -> ResolutionResult:
    # A literal rather than dict(...): ruff's C408 rejects the call form, and the
    # two are the same object.
    base: dict[str, Any] = {
        "owner_repo": "h5py/h5py",
        "sha": "abc123",
        "package_name": "h5py",
        "package_version": "3.15.1",
        "primary_root": ".",
        "requires_python": ">=3.9",
        "python_version": "3.11",
        "interpreter_source": "requires-python",
        "env_payload": ["numpy==2.4.1"],
        "probe_status": "installable",
        "probe_log": "ok",
        "cutoff_used": "2026-01-22T00:00:00Z",
        "cutoff_relaxed": False,
        "dropped_requirements": [],
        "resolver_version": RESOLVER_VERSION,
    }
    base.update(kw)
    return ResolutionResult(**base)


def test_row_carries_provenance():
    row = build_row("h5py", "h5py", "abc123", _result())
    assert row["resolver_version"] == RESOLVER_VERSION
    assert row["interpreter_source"] == "requires-python"
    assert row["cutoff_used"] == "2026-01-22T00:00:00Z"
    assert row["resolved_at"]
    assert row["uv_version"]


def test_requires_python_is_stored_not_nulled():
    # The predecessor hardcoded this to None while the parsed value was computed
    # and discarded.
    row = build_row("h5py", "h5py", "abc123", _result())
    assert row["requires_python"] == ">=3.9"


def test_env_payload_is_json_encoded():
    row = build_row("h5py", "h5py", "abc123", _result())
    assert json.loads(row["env_payload"]) == ["numpy==2.4.1"]


def test_dropped_requirements_round_trip():
    dropped = [{"raw": "pyuwsgi;sys.platform!='win32'", "reason": "unparseable requirement"}]
    row = build_row("h5py", "h5py", "abc123", _result(dropped_requirements=dropped))
    assert json.loads(row["dropped_requirements"]) == dropped


def test_retired_columns_are_not_written():
    row = build_row("h5py", "h5py", "abc123", _result())
    for retired in ("build_commands", "install_commands", "resolution_strategy"):
        assert retired not in row


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

        def _analyze(sha: str, repo: str) -> ResolutionResult:
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
        # owner/repo/sha come from the item, not from the result -- build_row is
        # told which commit it is writing for.
        assert row["owner"] == "pandas-dev"
        assert row["sha"] == "abc123def456"
        assert row["env_payload"] == '["numpy==2.4.1"]'

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
