from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_supabase_client():
    """Mock Supabase client for integration tests."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.select.return_value = table
    table.insert.return_value = table
    table.upsert.return_value = table
    table.update.return_value = table
    table.eq.return_value = table
    table.is_.return_value = table
    table.gte.return_value = table
    table.lte.return_value = table
    table.limit.return_value = table
    table.execute.return_value = MagicMock(data=[])
    return client
