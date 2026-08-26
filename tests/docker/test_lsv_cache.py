"""Unit tests for the stage-7 LSV cache (baseline + survey) wiring.

These exercise the pure host-side/in-container glue that does not need Harbor,
Docker, or a live Supabase: the resource-key mapping, the bytea transport, the
render_env shell escaping, the task-id parsing shared by reader and writer, and
-- most importantly -- that the on_conflict column lists in the in-container
writeback match the primary keys the migrations actually declare. A drift there
silently disables every upsert (PostgREST 400s, writeback swallows it), so it is
asserted directly against the SQL.
"""

from __future__ import annotations

import importlib.util
import re
import shlex
from pathlib import Path
from types import ModuleType

import pytest

from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, HarborTaskPaths
from datasmith.harbor_adapter.utils import render_run_setup_sh, render_test_sh
from datasmith.runners.harbor_healthcheck import (
    _compute_resource_attrs,
    _decode_bytea,
    _lsv_render_env,
)

_ROOT = Path(__file__).parents[2]
_TEMPLATE = _ROOT / "src" / "datasmith" / "harbor_adapter" / "template"
_MIGRATIONS = _ROOT / "supabase" / "migrations"


def _load_template_module(name: str) -> ModuleType:
    """Import a stdlib-only template script by path (the template dir is not a
    package)."""
    spec = importlib.util.spec_from_file_location(f"_tmpl_{name}", _TEMPLATE / f"{name}.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pk_columns(migration_filename: str) -> list[str]:
    """Extract the PRIMARY KEY column list from a migration's CREATE TABLE."""
    sql = (_MIGRATIONS / migration_filename).read_text()
    m = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", sql, re.IGNORECASE | re.DOTALL)
    assert m, f"no PRIMARY KEY found in {migration_filename}"
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


# ── resource key ────────────────────────────────────────────────────────────


def test_compute_resource_attrs_daytona_keys_on_machine_class() -> None:
    attrs = _compute_resource_attrs(
        use_daytona=True, container_name="img:1", image_digest="manifest:ab", cpu_count=2, mem_mb=32768
    )
    assert attrs["env"] == "daytona"
    assert attrs["machine_class"] == "default"
    assert attrs["docker_host_id"] == ""  # daytona keys on class, not host
    assert attrs["cpu_count"] == "2"
    assert attrs["mem_bytes"] == str(32768 * 1024 * 1024)


def test_compute_resource_attrs_docker_keys_on_host(monkeypatch: pytest.MonkeyPatch) -> None:
    attrs = _compute_resource_attrs(
        use_daytona=False, container_name="img:1", image_digest="", cpu_count=4, mem_mb=8192
    )
    assert attrs["env"] == "docker"
    assert attrs["machine_class"] == ""  # docker keys on host, not class
    assert attrs["docker_host_id"]  # gethostname() default, non-empty
    assert attrs["cpu_count"] == "4"


def test_lsv_render_env_carries_key_and_creds() -> None:
    base = {"DATASMITH_SUPABASE_URL": "https://db", "DATASMITH_SUPABASE_SERVICE_KEY": "k"}
    attrs = _compute_resource_attrs(use_daytona=True, container_name="c", image_digest="d", cpu_count=2, mem_mb=1024)
    env = _lsv_render_env(base, task_id="o__r__7", attrs=attrs)
    assert env["DATASMITH_SUPABASE_URL"] == "https://db"
    assert env["LSV_TASK_ID"] == "o__r__7"
    assert env["LSV_ENV"] == "daytona"
    assert env["LSV_MEM_BYTES"] == str(1024 * 1024 * 1024)


# ── bytea transport ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("payload", [b"", b"hello", bytes(range(256)), b"\x00\x01\x02sqlite"])
def test_decode_bytea_roundtrip(payload: bytes) -> None:
    assert _decode_bytea("\\x" + payload.hex()) == payload


@pytest.mark.parametrize("bad", [None, "", "deadbeef", 42, "\\xzz", b"\\x00"])
def test_decode_bytea_rejects_non_hex(bad: object) -> None:
    assert _decode_bytea(bad) is None


# ── render_env shell escaping ───────────────────────────────────────────────


def _exports_in(script: str) -> dict[str, str]:
    """Parse `export K=V` lines the render_env block emits, unquoting V."""
    out: dict[str, str] = {}
    for line in script.splitlines():
        m = re.match(r"^export (LSV_[A-Z_]+|DATASMITH_[A-Z_]+)=(.*)$", line)
        if m:
            out[m.group(1)] = "".join(shlex.split(m.group(2)))
    return out


def test_render_env_roundtrips_through_shell_quote() -> None:
    tricky = {
        "LSV_CONTAINER_NAME": "repo name with spaces",
        "LSV_IMAGE_DIGEST": "manifest:a'b$c;rm -rf /",
        "DATASMITH_SUPABASE_URL": "https://db.formulacode.org",
    }
    for script in (
        render_run_setup_sh(owner="o", repo="r", issue_number=1, render_env=tricky),
        render_test_sh(base_commit="abc", owner="o", repo="r", issue_number=1, render_env=tricky),
    ):
        parsed = _exports_in(script)
        for k, v in tricky.items():
            assert parsed[k] == v, f"{k} did not survive shell_quote round-trip"


def test_render_env_none_and_empty_are_identical_and_bare() -> None:
    none_setup = render_run_setup_sh(owner="o", repo="r", issue_number=1, render_env=None)
    empty_setup = render_run_setup_sh(owner="o", repo="r", issue_number=1, render_env={})
    assert none_setup == empty_setup
    assert "LSV_TASK_ID" not in none_setup  # no cache exports when disabled


# ── on_conflict / PK agreement (the drift that silently kills upserts) ───────


def test_baseline_writeback_on_conflict_matches_migration_pk() -> None:
    writeback = _load_template_module("lsv_cache_writeback")
    assert list(writeback._BASELINE_PK_COLS) == _pk_columns("00031_lsv_baseline_cache.sql")


def test_deps_cache_on_conflict_matches_migration_pk() -> None:
    writeback = _load_template_module("lsv_cache_writeback")
    src = (_TEMPLATE / "lsv_cache_writeback.py").read_text()
    m = re.search(r"lsv_deps_cache\?on_conflict=([a-z_,]+)", src)
    assert m, "deps_cache upsert on_conflict not found"
    assert m.group(1).split(",") == _pk_columns("00032_lsv_deps_cache.sql")
    # touch the module so an import error here fails loudly too
    assert hasattr(writeback, "_upsert_deps_cache_row")


# ── task-id parsing is shared by reader (lsv_init) and writer (writeback) ─────


@pytest.mark.parametrize("mod_name", ["lsv_init", "lsv_cache_writeback"])
def test_parse_task_id_double_underscore(mod_name: str) -> None:
    mod = _load_template_module(mod_name)
    assert mod._parse_task_id("pandas-dev__pandas__123") == ("pandas-dev", "pandas", 123)
    assert mod._parse_task_id("o__r__notanint") is None
    assert mod._parse_task_id("nounderscores") is None


# ── adapter bakes cache/ so the Dockerfile COPY never fails ──────────────────


def test_write_cache_files_miss_leaves_only_gitkeep(tmp_path: Path) -> None:
    adapter = FormulaCodeAdapter(harbor_tasks_root=tmp_path)
    paths = HarborTaskPaths(tmp_path / "task")
    adapter._write_cache_files(paths, None)
    cache = paths.environment_dir / "cache"
    assert (cache / ".gitkeep").exists()
    assert not (cache / "lightspeed_deps.db").exists()


def test_write_cache_files_hit_writes_deps_db(tmp_path: Path) -> None:
    adapter = FormulaCodeAdapter(harbor_tasks_root=tmp_path)
    paths = HarborTaskPaths(tmp_path / "task")
    adapter._write_cache_files(paths, b"SQLite format 3\x00survey")
    assert (paths.environment_dir / "cache" / "lightspeed_deps.db").read_bytes() == b"SQLite format 3\x00survey"
