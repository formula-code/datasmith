"""Tests for datasmith.utils.db — Supabase client, caching, batch upsert."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from datasmith.utils.db import (
    UnfilteredLargeSelectError,
    abatch_upsert,
    afetch_all,
    batch_upsert,
    fetch_all,
    get_async_client,
    get_client,
    stable_hash,
    supabase_cached,
)


@pytest.fixture(autouse=True)
def _clear_async_client_cache():
    """Drop any memoised async client so tests never share one."""
    import datasmith.utils.db as db_mod

    db_mod._async_clients.clear()
    yield
    db_mod._async_clients.clear()


def _sync_query_mock(pages: list[list[dict[str, Any]]]) -> tuple[MagicMock, MagicMock]:
    """Build a client whose select chain returns *pages* in order.

    Every builder method returns the same object, so the test can inspect
    exactly which predicates fetch_all attached.
    """
    query = MagicMock()
    for method in ("select", "eq", "is_", "gte", "lte", "lt", "neq", "in_", "order", "range"):
        getattr(query, method).return_value = query
    query.execute.side_effect = [MagicMock(data=page) for page in pages]
    client = MagicMock()
    client.table.return_value = query
    return client, query


def _async_query_mock(pages: list[list[dict[str, Any]]]) -> tuple[MagicMock, MagicMock]:
    """Async twin of :func:`_sync_query_mock`; ``execute`` is awaitable."""
    query = MagicMock()
    for method in ("select", "eq", "is_", "gte", "lte", "lt", "neq", "in_", "order", "range"):
        getattr(query, method).return_value = query
    query.execute = AsyncMock(side_effect=[MagicMock(data=page) for page in pages])
    client = MagicMock()
    client.table.return_value = query
    return client, query


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


class TestLtFilters:
    """`lt_filters` is a real strict less-than, not lte minus a second."""

    def test_lt_applied_to_query(self) -> None:
        client, query = _sync_query_mock([[{"issue_number": 1}]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            rows = fetch_all(
                "pull_requests",
                select="issue_number",
                gte_filters={"merged_at": "2026-08-01"},
                lt_filters={"merged_at": "2026-09-01"},
            )
        assert rows == [{"issue_number": 1}]
        query.gte.assert_called_once_with("merged_at", "2026-08-01")
        query.lt.assert_called_once_with("merged_at", "2026-09-01")
        # The half-open window must NOT degrade into an inclusive upper bound.
        query.lte.assert_not_called()

    def test_lt_and_lte_are_independent(self) -> None:
        client, query = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            fetch_all(
                "pull_requests",
                select="issue_number",
                lte_filters={"created_at": "2026-08-31"},
                lt_filters={"merged_at": "2026-09-01"},
            )
        query.lte.assert_called_once_with("created_at", "2026-08-31")
        query.lt.assert_called_once_with("merged_at", "2026-09-01")

    def test_lt_omitted_when_absent(self) -> None:
        client, query = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            fetch_all("pull_requests", select="issue_number", filters={"owner": "pandas-dev"})
        query.lt.assert_not_called()

    def test_lt_counts_as_a_filter_for_the_guardrail(self) -> None:
        """A window-scoped read of `patch` is legitimate and must not raise."""
        client, _ = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            rows = fetch_all(
                "pull_requests",
                select="owner, repo, patch",
                lt_filters={"merged_at": "2026-09-01"},
            )
        assert rows == []


class TestLargeSelectGuardrail:
    """Selecting `patch` table-wide killed PostgREST; refuse it up front."""

    def test_unfiltered_star_on_large_table_raises(self) -> None:
        client, _ = _sync_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError, match="pull_requests"),
        ):
            fetch_all("pull_requests")

    def test_unfiltered_large_column_raises(self) -> None:
        client, _ = _sync_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError, match="patch"),
        ):
            fetch_all("pull_requests", select="owner, repo, patch")

    def test_guardrail_raises_before_any_query(self) -> None:
        client, query = _sync_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError),
        ):
            fetch_all("pull_requests", select="patch")
        query.execute.assert_not_called()

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"filters": {"owner": "pandas-dev"}},
            {"is_null": ["container_name"]},
            {"gte_filters": {"merged_at": "2026-08-01"}},
            {"lte_filters": {"merged_at": "2026-08-31"}},
            {"lt_filters": {"merged_at": "2026-09-01"}},
            {"neq_filters": {"merge_commit_sha": ""}},
        ],
    )
    def test_any_filter_permits_the_read(self, kwargs: dict[str, Any]) -> None:
        client, _ = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            assert fetch_all("pull_requests", select="patch", **kwargs) == []

    def test_empty_filter_containers_do_not_count(self) -> None:
        """`filters={}` is the caller having no filter, not having one."""
        client, _ = _sync_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError),
        ):
            fetch_all("pull_requests", select="patch", filters={}, is_null=[])

    def test_narrow_select_on_large_table_is_fine(self) -> None:
        client, _ = _sync_query_mock([[{"owner": "a", "repo": "b"}]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            rows = fetch_all("pull_requests", select="owner, repo")
        assert rows == [{"owner": "a", "repo": "b"}]

    def test_unfiltered_star_on_small_table_is_fine(self) -> None:
        """`repositories` is small and stage 5 still reads it whole."""
        client, _ = _sync_query_mock([[{"owner": "a", "repo": "b"}]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            rows = fetch_all("repositories")
        assert rows == [{"owner": "a", "repo": "b"}]

    def test_substring_lookalike_column_is_not_flagged(self) -> None:
        """`patch_url` merely contains "patch"; it is not a large column."""
        client, _ = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            assert fetch_all("pull_requests", select="owner, patch_url") == []

    def test_aliased_large_column_is_flagged(self) -> None:
        """`diff:patch` renames the column but still ships its bytes."""
        client, _ = _sync_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError),
        ):
            fetch_all("pull_requests", select="owner, diff:patch")

    async def test_guardrail_also_applies_to_afetch_all(self) -> None:
        client, query = _async_query_mock([[]])
        with (
            patch("datasmith.utils.db.get_async_client", return_value=client),
            pytest.raises(UnfilteredLargeSelectError),
        ):
            await afetch_all("pull_requests", select="patch")
        query.execute.assert_not_awaited()


class TestGetAsyncClient:
    """Per-event-loop singleton, mirroring the memoised get_client()."""

    async def test_same_loop_reuses_one_client(self) -> None:
        created = [MagicMock(name="c1"), MagicMock(name="c2")]
        with patch("supabase.acreate_client", AsyncMock(side_effect=created)) as create:
            c1 = await get_async_client()
            c2 = await get_async_client()
        assert c1 is c2
        assert create.await_count == 1

    async def test_concurrent_callers_share_one_client(self) -> None:
        """All eight can miss the cache, but they must still agree on one client.

        The create suspends, so every coroutine gets past the cache check
        before any of them stores a result — the exact race `setdefault`
        exists to settle.
        """
        created: list[MagicMock] = []

        async def _slow_create(*args: Any, **kwargs: Any) -> MagicMock:
            await asyncio.sleep(0)
            client = MagicMock(name=f"c{len(created)}")
            created.append(client)
            return client

        with patch("supabase.acreate_client", _slow_create):
            clients = await asyncio.gather(*(get_async_client() for _ in range(8)))

        assert len(created) == 8, "the race did not actually happen"
        assert all(c is clients[0] for c in clients)

    def test_distinct_loops_get_distinct_clients(self) -> None:
        created = [MagicMock(name="c1"), MagicMock(name="c2")]
        with patch("supabase.acreate_client", AsyncMock(side_effect=created)) as create:
            first = asyncio.run(get_async_client())
            second = asyncio.run(get_async_client())
        assert first is not second
        assert create.await_count == 2

    def test_missing_credentials_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SUPABASE_URL", "")
        monkeypatch.setenv("SUPABASE_KEY", "")
        with pytest.raises(ValueError, match="SUPABASE_URL"):
            asyncio.run(get_async_client())


class TestAFetchAll:
    async def test_single_page(self) -> None:
        client, query = _async_query_mock([[{"owner": "a", "repo": "b"}]])
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            rows = await afetch_all("repositories", select="owner, repo")
        assert rows == [{"owner": "a", "repo": "b"}]
        query.execute.assert_awaited_once()

    async def test_paginates_until_short_page(self) -> None:
        full = [{"owner": "a", "repo": "b", "issue_number": i} for i in range(3)]
        client, query = _async_query_mock([full, full[:1]])
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            rows = await afetch_all("pull_requests", select="issue_number", filters={"owner": "a"}, page_size=3)
        assert len(rows) == 4
        assert query.execute.await_count == 2
        assert [c.args for c in query.range.call_args_list] == [(0, 2), (3, 5)]

    async def test_applies_every_filter_kind(self) -> None:
        client, query = _async_query_mock([[]])
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            await afetch_all(
                "pull_requests",
                select="issue_number",
                filters={"owner": "pandas-dev"},
                is_null=["container_name"],
                gte_filters={"merged_at": "2026-08-01"},
                lte_filters={"created_at": "2026-08-31"},
                lt_filters={"merged_at": "2026-09-01"},
                neq_filters={"merge_commit_sha": ""},
                in_filters={"issue_number": [1, 2, 3]},
            )
        query.in_.assert_called_once_with("issue_number", [1, 2, 3])
        query.eq.assert_called_once_with("owner", "pandas-dev")
        query.is_.assert_called_once_with("container_name", "null")
        query.gte.assert_called_once_with("merged_at", "2026-08-01")
        query.lte.assert_called_once_with("created_at", "2026-08-31")
        query.lt.assert_called_once_with("merged_at", "2026-09-01")
        query.neq.assert_called_once_with("merge_commit_sha", "")

    async def test_default_order_by_matches_sync(self) -> None:
        client, query = _async_query_mock([[]])
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            await afetch_all("pull_requests", select="issue_number", filters={"owner": "a"})
        assert [c.args[0] for c in query.order.call_args_list] == ["owner", "repo", "issue_number"]

    async def test_null_data_is_an_empty_page(self) -> None:
        client = MagicMock()
        query = MagicMock()
        for method in ("select", "eq", "order", "range"):
            getattr(query, method).return_value = query
        query.execute = AsyncMock(return_value=MagicMock(data=None))
        client.table.return_value = query
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            assert await afetch_all("repositories", select="owner") == []


class TestABatchUpsert:
    async def test_empty_list_writes_nothing(self) -> None:
        client = MagicMock()
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            assert await abatch_upsert("pull_requests", []) == 0
        client.table.assert_not_called()

    async def test_chunks_sequentially(self) -> None:
        table = MagicMock()
        table.upsert.return_value = table
        table.execute = AsyncMock(return_value=MagicMock())
        client = MagicMock()
        client.table.return_value = table

        rows = [{"id": i} for i in range(250)]
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            total = await abatch_upsert("pull_requests", rows, chunk_size=100)

        assert total == 250
        assert table.upsert.call_count == 3
        assert [len(c.args[0]) for c in table.upsert.call_args_list] == [100, 100, 50]
        assert table.execute.await_count == 3


class TestInFilters:
    """``in_filters`` exists so a skip-set read can name the keys it wants.

    Equality on ``(owner, repo)`` is not a substitute: it still returns every
    row that repository ever produced, which for a 150-repository window is
    the whole table again in 150 requests.
    """

    def test_values_are_passed_as_a_list(self) -> None:
        client, query = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            fetch_all("packages", select="owner, repo, sha", in_filters={"sha": ("a", "b")})
        query.in_.assert_called_once_with("sha", ["a", "b"])

    def test_several_columns_are_all_applied(self) -> None:
        client, query = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            fetch_all(
                "candidate_prs",
                select="owner, repo, issue_number",
                in_filters={"owner": ["pandas-dev"], "repo": ["pandas", "pandas-stubs"]},
            )
        assert [c.args for c in query.in_.call_args_list] == [
            ("owner", ["pandas-dev"]),
            ("repo", ["pandas", "pandas-stubs"]),
        ]

    def test_an_in_filter_satisfies_the_large_select_guardrail(self) -> None:
        """A key-scoped read of ``patch`` is bounded, so it must not be refused."""
        client, query = _sync_query_mock([[]])
        with patch("datasmith.utils.db.get_client", return_value=client):
            rows = fetch_all("pull_requests", select="patch", in_filters={"issue_number": [1, 2]})
        assert rows == []
        query.in_.assert_called_once_with("issue_number", [1, 2])

    async def test_async_sibling_applies_it_too(self) -> None:
        client, query = _async_query_mock([[]])
        with patch("datasmith.utils.db.get_async_client", return_value=client):
            await afetch_all("packages", select="sha", in_filters={"sha": ["a"]})
        query.in_.assert_called_once_with("sha", ["a"])
