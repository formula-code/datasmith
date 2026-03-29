"""Abstract interface for CLI-based coding agents."""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from datasmith.utils import get_logger

logger = get_logger("agents.installed")


@dataclass
class AgentResult:
    """Unified result from any installed CLI agent."""

    success: bool
    output: str = ""
    raw_output: str = ""
    files_changed: list[str] = field(default_factory=list)
    duration_s: float = 0.0
    error: str = ""


# Backward-compat alias
CodexResult = AgentResult


class InstalledAgent(ABC):
    """Abstract interface for CLI-based coding agents.

    Each subclass wraps a specific CLI tool (Codex, Claude Code, Gemini CLI)
    and normalises its output into an ``AgentResult``.
    """

    @abstractmethod
    def name(self) -> str:
        """Human-readable agent name."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the CLI binary is on PATH."""

    @abstractmethod
    def exec(
        self,
        prompt: str,
        timeout: int = 3600,
        workdir: str | None = None,
    ) -> AgentResult:
        """Run a prompt non-interactively. Returns AgentResult."""

    def exec_or_dry_run(
        self,
        prompt: str,
        timeout: int = 3600,
        workdir: str | None = None,
        dry_run: bool = False,
    ) -> AgentResult:
        """Shared dry-run wrapper around :meth:`exec`."""
        if dry_run:
            logger.info("[DRY RUN] %s command for prompt (%d chars): %.500s", self.name(), len(prompt), prompt)
            return AgentResult(
                success=True,
                output="[dry run — no execution]",
                duration_s=0.0,
            )
        return self.exec(prompt, timeout=timeout, workdir=workdir)

    @staticmethod
    def _which(binary: str) -> bool:
        return shutil.which(binary) is not None


def _kill_process_group(proc: subprocess.Popen[str], sig: int = signal.SIGTERM) -> None:
    """Send *sig* to the process group of *proc*, swallowing errors."""
    with contextlib.suppress(ProcessLookupError, OSError):
        os.killpg(os.getpgid(proc.pid), sig)


# ---------------------------------------------------------------------------
# Global subprocess registry — allows the SIGINT handler to reach agent
# processes that live in their own sessions (start_new_session=True) and
# therefore don't receive CTRL+C from the terminal.
# ---------------------------------------------------------------------------
_active_procs: set[subprocess.Popen[str]] = set()
_active_procs_lock = threading.Lock()


def _register_proc(proc: subprocess.Popen[str]) -> None:
    with _active_procs_lock:
        _active_procs.add(proc)


def _unregister_proc(proc: subprocess.Popen[str]) -> None:
    with _active_procs_lock:
        _active_procs.discard(proc)


def terminate_all_agents(force: bool = False) -> None:
    """Kill every tracked agent subprocess.

    Called from the CLI signal handler so that threads blocked on
    ``proc.communicate()`` can unblock and the process can exit.

    With *force=True* sends SIGKILL instead of SIGTERM.
    """
    sig = signal.SIGKILL if force else signal.SIGTERM
    # list() snapshot avoids issues with concurrent set mutation.
    for proc in list(_active_procs):
        _kill_process_group(proc, sig)


def _terminate_and_wait(proc: subprocess.Popen[str]) -> None:
    """Send SIGTERM, wait, escalate to SIGKILL if needed."""
    _kill_process_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _kill_process_group(proc, signal.SIGKILL)
        proc.wait()


def run_agent_subprocess(
    cmd: list[str],
    *,
    timeout: int = 3600,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    agent_name: str = "agent",
) -> tuple[int, str, str, float]:
    """Run an agent CLI command with process-group cleanup on interrupt or timeout.

    Returns ``(returncode, stdout, stderr, duration_s)``.

    On timeout the process is killed and any partial output captured so far
    is returned with ``returncode=-1``.

    Raises ``FileNotFoundError`` if the binary is missing and
    re-raises ``KeyboardInterrupt`` (after cleanup).
    """
    start = time.time()
    proc: subprocess.Popen[str] | None = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd,
            env=env,
            start_new_session=True,
        )
        _register_proc(proc)
        stdout, stderr = proc.communicate(timeout=timeout)
        duration = time.time() - start
        return proc.returncode, stdout, stderr, duration
    except subprocess.TimeoutExpired as exc:
        logger.warning("%s timed out after %ds — capturing partial output", agent_name, timeout)
        partial_stdout, partial_stderr = _collect_partial_output(exc, proc)
        duration = time.time() - start
        return -1, partial_stdout, partial_stderr, duration
    except KeyboardInterrupt:
        if proc is not None:
            _terminate_and_wait(proc)
        raise
    finally:
        if proc is not None:
            _unregister_proc(proc)
            if proc.poll() is None:
                _kill_process_group(proc, signal.SIGKILL)
                proc.wait()


def _collect_partial_output(
    exc: subprocess.TimeoutExpired,
    proc: subprocess.Popen[str] | None,
) -> tuple[str, str]:
    """Extract whatever output was buffered before a timeout."""
    partial_stdout = ""
    partial_stderr = ""
    if exc.stdout:
        partial_stdout = exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode(errors="replace")
    if exc.stderr:
        partial_stderr = exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode(errors="replace")
    if proc is not None:
        _terminate_and_wait(proc)
        try:
            remaining_out, remaining_err = proc.communicate(timeout=5)
            partial_stdout += remaining_out or ""
            partial_stderr += remaining_err or ""
        except Exception:
            logger.debug("Failed to read remaining output after timeout", exc_info=True)
    return partial_stdout, partial_stderr


# Registry of concrete agents in preference order.
# Populated by __init__.py after all subclasses are importable.
_AGENT_CLASSES: list[type[InstalledAgent]] = []


def get_agent(preference: list[str] | None = None) -> InstalledAgent:
    """Auto-detect and return the first available agent.

    *preference* is a list of agent names (lowercase) to try in order.
    Default: ``["claude", "codex", "gemini"]``.

    Raises ``RuntimeError`` if none are available.
    """
    from datasmith.agents.installed.claude import ClaudeAgent
    from datasmith.agents.installed.codex import CodexAgent
    from datasmith.agents.installed.gemini import GeminiAgent

    registry: dict[str, type[InstalledAgent]] = {
        "claude": ClaudeAgent,
        "codex": CodexAgent,
        "gemini": GeminiAgent,
    }

    order = preference or ["claude", "codex", "gemini"]
    for name in order:
        cls = registry.get(name)
        if cls is None:
            continue
        agent = cls()
        if agent.is_available():
            logger.info("Auto-detected agent: %s", agent.name())
            return agent

    available = list(registry.keys())
    raise RuntimeError(f"No installed CLI agent found. Tried: {order}. Install one of: {available}")
