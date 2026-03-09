from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field

from datasmith.utils import get_logger

logger = get_logger("agents.codex")


@dataclass
class CodexResult:
    """Result from a Codex CLI execution."""

    success: bool
    output: str = ""
    files_changed: list[str] = field(default_factory=list)
    duration_s: float = 0.0
    error: str = ""


def _parse_codex_stdout(stdout: str) -> tuple[list[str], list[str]]:
    """Parse JSON stream from Codex stdout into (output_lines, files_changed)."""
    files_changed: list[str] = []
    output_lines: list[str] = []

    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                if "file" in obj:
                    files_changed.append(obj["file"])
                if "output" in obj:
                    output_lines.append(obj["output"])
                elif "message" in obj:
                    output_lines.append(obj["message"])
        except json.JSONDecodeError:
            output_lines.append(line)

    return output_lines, files_changed


def codex_exec(
    prompt: str,
    model: str = "o4-mini",
    timeout: int = 900,
    workdir: str | None = None,
) -> CodexResult:
    """Execute a prompt via the Codex CLI.

    Wraps ``codex exec --full-auto --json -m {model} "{prompt}"`` via subprocess.
    """
    cmd = ["codex", "exec", "--full-auto", "--json", "-m", model, prompt]

    start = time.time()
    try:
        result = subprocess.run(  # noqa: S603
            cmd, capture_output=True, text=True, timeout=timeout, cwd=workdir
        )
        duration = time.time() - start
        output_lines, files_changed = _parse_codex_stdout(result.stdout)

        return CodexResult(
            success=result.returncode == 0,
            output="\n".join(output_lines) if output_lines else result.stdout,
            files_changed=files_changed,
            duration_s=duration,
            error=result.stderr if result.returncode != 0 else "",
        )
    except subprocess.TimeoutExpired:
        return CodexResult(
            success=False,
            duration_s=time.time() - start,
            error=f"Codex execution timed out after {timeout}s",
        )
    except FileNotFoundError:
        return CodexResult(
            success=False,
            duration_s=time.time() - start,
            error="codex CLI not found. Install with: npm install -g @openai/codex",
        )
    except Exception as e:
        return CodexResult(
            success=False,
            duration_s=time.time() - start,
            error=str(e),
        )
