"""Tests for the container-backed agent tooling."""

from __future__ import annotations

import json
from typing import Callable
from unittest.mock import MagicMock, patch

import pytest

from datasmith.agents.tools.container import ContainerToolExecutor, ExecResult


@pytest.fixture()
def patched_container() -> Callable[[], MagicMock]:
    """Patch ``PersistentContainer`` with a controllable mock."""

    patcher = patch("datasmith.agents.tools.container.PersistentContainer")
    persistent_cls = patcher.start()
    pc = MagicMock()
    pc.start.return_value = None
    pc.find_repo_root.return_value = "/repo"
    pc.infer_repo_facts.return_value = {
        "pkg_candidates": ["example"],
        "python_versions_from_asv": ["3.10"],
    }
    pc.list_tree.return_value = ["src/module.py"]
    pc.read_file.return_value = "print('hi')\n"
    pc.try_import.return_value = {
        "ok": True,
        "tried": ["example"],
        "succeeded": ["example"],
        "stdout": "IMPORTED::example::unknown",
        "stderr": "",
        "rc": 0,
    }
    pc.exec.return_value = ExecResult(rc=0, stdout="out", stderr="err")
    persistent_cls.return_value = pc

    def factory() -> MagicMock:
        pc.reset_mock()
        pc.start.return_value = None  # ensure callable after reset
        pc.exec.return_value = ExecResult(rc=0, stdout="out", stderr="err")
        return pc

    yield factory
    patcher.stop()


def _make_executor(patched_container: Callable[[], MagicMock]) -> tuple[ContainerToolExecutor, MagicMock]:
    container = patched_container()
    # ``patched_container`` ensures the patched class returns ``container``
    docker_client = MagicMock()
    executor = ContainerToolExecutor(docker_client=docker_client, image_name="example:tag")
    return executor, container


def test_registry_registers_expected_tools(patched_container: Callable[[], MagicMock]) -> None:
    executor, _ = _make_executor(patched_container)
    assert set(executor.registry.list_tools()) == {
        "probe_repo",
        "list_tree",
        "read_file",
        "try_import",
        "exec_arbitrary",
    }


def test_choose_action_dispatches_and_updates_facts(
    patched_container: Callable[[], MagicMock],
) -> None:
    executor, container = _make_executor(patched_container)
    observation = executor.choose_action("probe_repo", "")
    data = json.loads(observation)
    assert data["pkg_candidates"] == ["example"]
    container.infer_repo_facts.assert_called()


def test_choose_action_handles_read_file_invalid_json(
    patched_container: Callable[[], MagicMock],
) -> None:
    executor, _ = _make_executor(patched_container)
    result = executor.choose_action("read_file", "not json")
    assert result.startswith("[tool_error] ToolExecutionError")


def test_try_import_tool_returns_serialised_json(
    patched_container: Callable[[], MagicMock],
) -> None:
    executor, _ = _make_executor(patched_container)
    payload = json.dumps({"candidates": ["example"], "python": "python"})
    result = executor.choose_action("try_import", payload)
    parsed = json.loads(result)
    assert parsed["ok"] is True


def test_exec_arbitrary_formats_output(patched_container: Callable[[], MagicMock]) -> None:
    executor, _ = _make_executor(patched_container)
    result = executor.choose_action("exec_arbitrary", "echo hi")
    assert result.startswith("[exec_arbitrary] rc=0")
    assert "--- STDOUT ---" in result


def test_unknown_action_returns_noop(patched_container: Callable[[], MagicMock]) -> None:
    executor, _ = _make_executor(patched_container)
    assert executor.choose_action("unknown", "") == "[noop] Unknown action 'unknown'"


def test_import_check_prefers_micromamba_env(patched_container: Callable[[], MagicMock]) -> None:
    executor, container = _make_executor(patched_container)
    executor.import_check("python")
    container.try_import.assert_called_with(
        "micromamba run -n asv_3.10 python",
        ["example"],
    )
