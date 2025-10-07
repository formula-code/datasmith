"""Dependency resolution using uv."""

from __future__ import annotations

import datetime as dt
import zipfile
from collections.abc import Iterable
from pathlib import Path

from .constants import ANSI_RE
from .package_filters import fix_marker_spacing
from .python_manager import run_uv


def strip_ansi(s: str) -> str:
    """Remove ANSI escape codes from a string."""
    return ANSI_RE.sub("", s)


def rfc3339(ts: dt.datetime) -> str:
    """Convert a datetime to RFC3339 format string."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def uv_compile(requirements: Iterable[str], *, python_version: str | None, cutoff_rfc3339: str | None) -> list[str]:
    """
    Use `uv pip compile` to resolve to pinned requirements.
    Reads from stdin (using '-') and prints the compiled file to stdout.

    Args:
        requirements: Iterable of requirement strings
        python_version: Python version to compile for (e.g., "3.8")
        cutoff_rfc3339: RFC3339 timestamp to exclude packages newer than this date

    Returns:
        List of pinned requirement strings

    Raises:
        RuntimeError: If uv pip compile fails
    """
    # Fix marker spacing for all requirements
    reqs = sorted({fix_marker_spacing(r.strip()) for r in requirements if r and r.strip()})
    if not reqs:
        return []
    req_text = "\n".join(reqs) + "\n"
    args = ["pip", "compile", "-"]
    if python_version:
        args.extend(["--python", python_version])
    extra_env: dict[str, str] = {}
    if cutoff_rfc3339:
        extra_env["UV_EXCLUDE_NEWER"] = cutoff_rfc3339
    cp = run_uv(args, input_text=req_text, extra_env=extra_env)
    if cp.returncode != 0:
        # Bubble up the actual error text
        raise RuntimeError(f"uv pip compile failed:\n{cp.stderr.decode() or cp.stdout.decode()}")
    out: list[str] = []
    for raw in cp.stdout.decode().splitlines():
        s = strip_ansi(raw).strip()
        # ignore comments (including those that had ANSI colours)
        if s and not s.startswith("#"):
            out.append(s)
    return out


def uv_dry_run_install(
    pinned: Iterable[str], *, python_version: str | None, venv_path: Path | None = None
) -> tuple[bool, str]:
    """
    Run a dry-run install to validate that dependencies can be installed.

    Args:
        pinned: Pinned requirement strings
        python_version: Python version string (e.g., "3.8")
        venv_path: Optional path to a virtual environment to use

    Returns:
        Tuple of (success: bool, log: str)
    """
    # Fix marker spacing for all requirements
    text_lines = [fix_marker_spacing(x) for x in pinned if x.strip()]
    if not text_lines:
        # Nothing to install; treat as OK but say why.
        return True, "No runtime dependencies."
    text = "\n".join(text_lines) + "\n"
    args = ["pip", "install", "--dry-run", "-r", "-"]

    if venv_path and venv_path.exists():
        # Use the virtual environment's Python interpreter directly
        python_exe = venv_path / "bin" / "python"
        if not python_exe.exists():
            # Windows
            python_exe = venv_path / "Scripts" / "python.exe"
        if python_exe.exists():
            args.extend(["--python", str(python_exe)])
        else:
            # Fallback to version string with --system if venv structure is unexpected
            if python_version:
                args.extend(["--python", python_version, "--system"])
    elif python_version:
        # Fallback: use --python with version string and --system
        args.extend(["--python", python_version, "--system"])

    cp = run_uv(args, input_text=text)
    ok = cp.returncode == 0
    log = strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
    return ok, log


def uv_build_and_read_metadata(project_dir: Path) -> tuple[str | None, str | None, list[str], str | None]:
    """
    Run `uv build` in the project directory, then read Name/Version/Requires-Dist/Requires-Python
    from the wheel METADATA.

    Args:
        project_dir: Path to the project directory

    Returns:
        Tuple of (name, version, requires_dist, requires_python)
        Returns (None, None, [], None) if build fails or no wheel is produced
    """
    cp = run_uv(["build"], cwd=project_dir)
    if cp.returncode != 0:
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
                # Fix marker spacing issues (e.g., "andextra" -> " and extra")
                req = fix_marker_spacing(req)
                requires_dist.append(req)
            elif line.startswith("Requires-Python: "):
                requires_python = line.split("Requires-Python:", 1)[1].strip()
    return name, version, requires_dist, requires_python
