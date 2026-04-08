from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from datasmith.runners.base import BaseRunner


class DummyRunner(BaseRunner):
    def __init__(self, n_concurrent: int = 10):
        super().__init__("dummy", n_concurrent=n_concurrent)
        self.processed: list[str] = []

    async def _process_item(self, item):
        await asyncio.sleep(0.01)
        self.processed.append(str(item))


class FailingRunner(BaseRunner):
    def __init__(self):
        super().__init__("failing", n_concurrent=5)
        self.processed: list[str] = []

    async def _process_item(self, item):
        self.processed.append(str(item))
        if int(item) % 3 == 0:
            raise ValueError(f"Item {item} failed")


@pytest.mark.asyncio
class TestConcurrentRunners:
    async def test_concurrent_no_data_loss(self, mock_supabase_client):
        """5 concurrent items should all be processed."""
        runner = DummyRunner(n_concurrent=5)
        items = list(range(20))

        with patch("datasmith.runners.base.get_client", return_value=mock_supabase_client):
            await runner.run(items)

        assert len(runner.processed) == 20
        assert set(runner.processed) == {str(i) for i in range(20)}

    async def test_concurrent_with_failures(self, mock_supabase_client):
        """Failures in some items don't prevent others from processing."""
        runner = FailingRunner()
        items = list(range(10))

        with patch("datasmith.runners.base.get_client", return_value=mock_supabase_client):
            await runner.run(items)

        # Items 0, 3, 6, 9 fail (divisible by 3), but all 10 are attempted
        assert len(runner.processed) == 10
        assert runner._failed == 4  # 0, 3, 6, 9
        assert runner._completed == 6

    async def test_concurrent_cache_no_corruption(self):
        """Multiple threads hashing same args should produce same result."""
        import threading

        from datasmith.utils.db import stable_hash

        results: list[str] = []
        errors: list[Exception] = []

        def worker():
            try:
                for i in range(50):
                    h = stable_hash("test", i, {"key": "value"})
                    results.append(h)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors
        assert len(results) == 500
        # All results for same input should be identical
        for i in range(50):
            expected = stable_hash("test", i, {"key": "value"})
            assert results[i] == expected
