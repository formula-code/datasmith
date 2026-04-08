"""Tests for datasmith.utils.db — Supabase client, caching, batch upsert."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from datasmith.utils.db import batch_upsert, get_client, stable_hash, supabase_cached


class SampleModel(BaseModel):
    name: str
    value: int

    @property
    def cache_key(self) -> str:
        return f"sample:{self.name}"


class TestStableHash:
    def test_deterministic(self) -> None:
        h1 = stable_hash("a", "b", {"x": 1, "y": 2})
        h2 = stable_hash("a", "b", {"x": 1, "y": 2})
        assert h1 == h2

    def test_different_args_differ(self) -> None:
        h1 = stable_hash("a", "b")
        h2 = stable_hash("b", "a")
        assert h1 != h2

    def test_returns_hex_string(self) -> None:
        h = stable_hash("test")
        assert len(h) == 64  # SHA-256 hex


class TestBatchUpsert:
    def test_empty_list(self) -> None:
        with patch("datasmith.utils.db.get_client"):
            result = batch_upsert("test_table", [])
        assert result == 0

    def test_chunks_correctly(self) -> None:
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.upsert.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        rows = [{"id": i} for i in range(250)]
        with patch("datasmith.utils.db.get_client", return_value=mock_client):
            result = batch_upsert("test_table", rows, chunk_size=100)

        assert result == 250
        assert mock_table.upsert.call_count == 3
        # Verify chunk sizes
        chunks = [call.args[0] for call in mock_table.upsert.call_args_list]
        assert [len(c) for c in chunks] == [100, 100, 50]


class TestSupabaseCached:
    def test_miss_then_hit(self) -> None:
        call_count = 0
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table

        # First call: cache miss (empty data)
        select_mock = MagicMock()
        mock_table.select.return_value = select_mock
        select_mock.eq.return_value = select_mock
        miss_response = MagicMock()
        miss_response.data = []
        hit_response = MagicMock()
        hit_response.data = [{"result_json": {"answer": 42}}]

        select_mock.execute.side_effect = [miss_response, hit_response]
        mock_table.upsert.return_value = mock_table

        @supabase_cached
        def compute(entity: Any) -> dict[str, int]:
            nonlocal call_count
            call_count += 1
            return {"answer": 42}

        model = SampleModel(name="test", value=1)

        with patch("datasmith.utils.db.get_client", return_value=mock_client):
            result1 = compute(model)
            result2 = compute(model)

        assert call_count == 1  # Only called once
        assert result1 == {"answer": 42}
        assert result2 == {"result_json": {"answer": 42}} or result2 == {"answer": 42}

    def test_force_bypass(self) -> None:
        call_count = 0
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table
        mock_table.upsert.return_value = mock_table
        mock_table.execute.return_value = MagicMock()

        @supabase_cached
        def compute(entity: Any) -> dict[str, int]:
            nonlocal call_count
            call_count += 1
            return {"answer": call_count}

        model = SampleModel(name="test", value=1)

        with patch("datasmith.utils.db.get_client", return_value=mock_client):
            compute(model, force=True)
            compute(model, force=True)

        assert call_count == 2  # Called twice since force=True

    def test_pydantic_roundtrip(self) -> None:
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_client.table.return_value = mock_table

        select_mock = MagicMock()
        mock_table.select.return_value = select_mock
        select_mock.eq.return_value = select_mock
        miss_response = MagicMock()
        miss_response.data = []
        select_mock.execute.return_value = miss_response
        mock_table.upsert.return_value = mock_table

        @supabase_cached
        def compute(entity: Any) -> SampleModel:
            return SampleModel(name="cached", value=99)

        model = SampleModel(name="test", value=1)

        with patch("datasmith.utils.db.get_client", return_value=mock_client):
            result = compute(model)

        assert isinstance(result, SampleModel)
        assert result.name == "cached"
        assert result.value == 99

        # Verify upsert was called with model_dump output
        upsert_call = mock_table.upsert.call_args[0][0]
        assert upsert_call["result_json"] == {"name": "cached", "value": 99}


class TestGetClient:
    def test_singleton(self) -> None:
        import datasmith.utils.db as db_mod

        db_mod._client = None  # Reset singleton
        with patch("datasmith.utils.db.create_client") as mock_create:
            mock_create.return_value = MagicMock()
            c1 = get_client()
            c2 = get_client()
        assert c1 is c2
        assert mock_create.call_count == 1
        db_mod._client = None  # Clean up

    def test_connection_failure_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import datasmith.utils.db as db_mod

        db_mod._client = None
        monkeypatch.setenv("SUPABASE_URL", "")
        monkeypatch.setenv("SUPABASE_KEY", "")
        with pytest.raises(ValueError, match="SUPABASE_URL"):
            get_client()
        db_mod._client = None
