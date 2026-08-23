"""The migration must add every column the runner writes, and grant nothing to anon."""

import re
from pathlib import Path

import pytest

# Anchored at the repository, not the current directory: a relative path makes
# the glob come back empty from anywhere else, and the fixture's own assert would
# then report "migration not found" for a migration that is present.
MIGRATIONS = Path(__file__).resolve().parents[2] / "supabase" / "migrations"
REQUIRED = [
    "dropped_requirements",
    "probe_status",
    "probe_log",
    "interpreter_source",
    "cutoff_used",
    "resolver_version",
    "uv_version",
    "resolved_at",
]


@pytest.fixture
def migration_sql() -> str:
    matches = sorted(MIGRATIONS.glob("*_packages_resolution_v2.sql"))
    assert matches, "migration not found"
    return matches[-1].read_text()


@pytest.mark.parametrize("column", REQUIRED)
def test_column_is_added(migration_sql, column):
    assert re.search(rf"\b{column}\b", migration_sql), column


def test_grants_nothing_to_anon(migration_sql):
    assert "TO anon" not in migration_sql
    assert "GRANT" not in migration_sql.upper() or "anon" not in migration_sql


def test_legacy_rows_are_stamped(migration_sql):
    assert "legacy" in migration_sql
