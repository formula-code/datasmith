"""Dependency resolution using uv."""

from __future__ import annotations

import datetime as dt
import os
import zipfile
from collections.abc import Iterable
from pathlib import Path

from .constants import ANSI_RE
from .python_manager import run_uv
from .requirements import parse_many, render


def strip_ansi(s: str) -> str:
    """Remove ANSI escape codes from a string."""
    return ANSI_RE.sub("", s)


def rfc3339(ts: dt.datetime) -> str:
    """Convert a datetime to RFC3339 format string."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    return ts.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def uv_compile_from_pyproject(
    pyproject_path: Path, python_version: str | None, cutoff_rfc3339: str | None
) -> list[str]:
    """Use `uv pip compile` to resolve to pinned requirements from pyproject.toml."""
    if not pyproject_path.exists():
        return []
    args = ["pip", "compile", str(pyproject_path.resolve())]
    if python_version:
        args.extend(["--python", python_version])
    args.append("--all-extras")
    extra_env: dict[str, str] = {}
    if cutoff_rfc3339:
        extra_env["UV_EXCLUDE_NEWER"] = cutoff_rfc3339
    cp = run_uv(args, input_text=None, extra_env=extra_env, cwd=pyproject_path.parent)
    if cp.returncode != 0:
        raise RuntimeError(f"uv pip compile failed:\n{cp.stderr.decode() or cp.stdout.decode()}")
    out: list[str] = []
    for raw in cp.stdout.decode().splitlines():
        s = strip_ansi(raw).strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def uv_compile(requirements: Iterable[str], *, python_version: str | None, cutoff_rfc3339: str | None) -> list[str]:
    """Use `uv pip compile` to resolve to pinned requirements from stdin."""
    parsed, _dropped = parse_many(requirements)
    reqs = render(parsed)
    if not reqs:
        return []
    req_text = "\n".join(reqs) + "\n"
    args = ["pip", "compile", "-"]
    if python_version:
        args.extend(["--python", python_version])
    args.append("--upgrade")
    extra_env: dict[str, str] = {}
    if cutoff_rfc3339:
        extra_env["UV_EXCLUDE_NEWER"] = cutoff_rfc3339
    cp = run_uv(args, input_text=req_text, extra_env=extra_env)
    if cp.returncode != 0:
        raise RuntimeError(f"uv pip compile failed:\n{cp.stderr.decode() or cp.stdout.decode()}")
    out: list[str] = []
    for raw in cp.stdout.decode().splitlines():
        s = strip_ansi(raw).strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def uv_dry_run_install(
    pinned: Iterable[str], *, python_version: str | None, venv_path: Path | None = None
) -> tuple[bool, str]:
    """Run a dry-run install to validate that dependencies can be installed."""
    parsed, _dropped = parse_many(pinned)
    text_lines = render(parsed)
    if not text_lines:
        return True, "No runtime dependencies."
    text = "\n".join(text_lines) + "\n"
    args = ["pip", "install", "--dry-run", "-r", "-"]

    if venv_path and venv_path.exists():
        python_exe = venv_path / "bin" / "python"
        if not python_exe.exists():
            python_exe = venv_path / "Scripts" / "python.exe"
        if python_exe.exists():
            args.extend(["--python", str(python_exe)])
        elif python_version:
            args.extend(["--python", python_version, "--system"])
    elif python_version:
        args.extend(["--python", python_version, "--system"])

    cp = run_uv(args, input_text=text)
    ok = cp.returncode == 0
    log = strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
    return ok, log


def uv_install_real(pinned: Iterable[str], *, python_executable: str | None = None) -> tuple[bool, str]:
    """Perform a real install of pinned requirements to surface sdist build failures."""
    parsed, _dropped = parse_many(pinned)
    lines = render(parsed)
    if not lines:
        return True, "No dependencies to install."
    text = "\n".join(lines) + "\n"
    args = ["pip", "install", "-r", "-"]
    if python_executable:
        args.extend(["--python", python_executable])
    cp = run_uv(args, input_text=text)
    ok = cp.returncode == 0
    log = strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
    return ok, log


def uv_build_and_read_metadata(project_dir: Path) -> tuple[str | None, str | None, list[str], str | None]:
    """Run `uv build` in the project directory, then read metadata from the wheel.

    This is a best-effort fallback — many repos have dynamic setup.py files that
    fail in partial clones. Failures are expected and logged at debug level.
    """
    import subprocess as _sp

    from datasmith.utils import get_logger as _get_logger

    _logger = _get_logger("resolution.dependency_resolver")

    # Use subprocess directly with DEVNULL for stderr to suppress noisy
    # setup.py tracebacks from child processes that bypass capture_output.
    env = os.environ.copy()
    env.setdefault("UV_COLOR", "never")
    env.setdefault("NO_COLOR", "1")
    cp = _sp.run(
        ["uv", "build"],
        capture_output=True,
        stdin=_sp.DEVNULL,
        cwd=str(project_dir),
        env=env,
    )
    if cp.returncode != 0:
        _logger.debug(
            "uv build failed in %s (expected for repos with dynamic setup.py): %s",
            project_dir.name,
            strip_ansi(cp.stderr.decode())[-200:],
        )
        return None, None, [], None
    dist_dir = project_dir / "dist"
    if not dist_dir.exists():
        return None, None, [], None
    wheels = sorted(dist_dir.glob("*.whl"))
    if not wheels:
        return None, None, [], None
    name, version = None, None
    requires_dist: list[str] = []
    requires_python: str | None = None
    with zipfile.ZipFile(wheels[-1]) as zf:
        meta_name = next((n for n in zf.namelist() if n.endswith(".dist-info/METADATA")), None)
        if not meta_name:
            return None, None, [], None
        content = zf.read(meta_name).decode("utf-8", errors="replace")
        for line in content.splitlines():
            if line.startswith("Name: "):
                name = line.split("Name:", 1)[1].strip()
            elif line.startswith("Version: "):
                version = line.split("Version:", 1)[1].strip()
            elif line.startswith("Requires-Dist: "):
                req = line.split("Requires-Dist:", 1)[1].strip()
                requires_dist.append(req)
            elif line.startswith("Requires-Python: "):
                requires_python = line.split("Requires-Python:", 1)[1].strip()
    return name, version, requires_dist, requires_python
