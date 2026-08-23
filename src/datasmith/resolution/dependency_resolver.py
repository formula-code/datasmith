"""Dependency resolution using uv."""

from __future__ import annotations

import datetime as dt
import os
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path

from datasmith.utils import get_logger

from .constants import ANSI_RE
from .python_manager import run_uv
from .requirements import Dropped, to_requirement_lines

logger = get_logger("resolution.dependency_resolver")


def seed_lines(raws: Iterable[str], *, context: str) -> list[str]:
    """Turn requirement strings into the text uv is given, and report the losses.

    Every line uv does not get is logged with the reason it was refused, so a
    missing package is diagnosable from the run log instead of vanishing.
    """
    lines, dropped = to_requirement_lines(raws)
    _log_dropped(dropped, context=context)
    return lines


def _log_dropped(dropped: list[Dropped], *, context: str) -> None:
    if not dropped:
        return
    logger.info("%s: dropped %d requirement(s)", context, len(dropped))
    for item in dropped:
        logger.debug("%s: dropped %r (%s)", context, item.raw, item.reason)


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
    reqs = seed_lines(requirements, context="uv pip compile")
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


#: Prefix of the log a dry-run writes when it never ran, because the throwaway
#: environment could not be built.  It names a fact about the host, not about the
#: requirements, and must stay distinguishable from a resolution failure.
VENV_SETUP_FAILED = "probe environment unavailable"


def _venv_interpreter(venv_path: Path) -> Path:
    """The interpreter inside a venv, POSIX layout unless the Windows one exists."""
    windows = venv_path / "Scripts" / "python.exe"
    if windows.exists():
        return windows
    return venv_path / "bin" / "python"


def _run_dry_run(text: str, interpreter_args: list[str]) -> tuple[bool, str]:
    cp = run_uv(["pip", "install", "--dry-run", "-r", "-", *interpreter_args], input_text=text)
    ok = cp.returncode == 0
    log = strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
    return ok, log


def uv_dry_run_install(
    pinned: Iterable[str], *, python_version: str | None, venv_path: Path | None = None
) -> tuple[bool, str]:
    """Run a dry-run install to validate that dependencies can be installed.

    The target is always an interpreter uv is allowed to write to.  The previous
    ``--python <version> --system`` fallback asked uv to install into whatever
    interpreter that version resolved to, and on any host whose Pythons are
    uv-managed -- which is every host this pipeline runs on -- uv refuses:
    "externally managed ... should not be modified".  Every commit then came back
    ``failed``, and the recorded status was the host's refusal rather than a fact
    about the seed.  So when no usable venv is handed in, one is built for the
    requested version and thrown away afterwards.

    There is deliberately no ``--system`` path left to fall back to: if the
    throwaway environment cannot be built, the answer says so with
    :data:`VENV_SETUP_FAILED` instead of re-describing a host refusal as a
    resolution failure.
    """
    text_lines = seed_lines(pinned, context="uv pip install --dry-run")
    if not text_lines:
        return True, "No runtime dependencies."
    text = "\n".join(text_lines) + "\n"

    if venv_path and venv_path.exists():
        return _run_dry_run(text, ["--python", str(_venv_interpreter(venv_path))])

    if not python_version:
        return _run_dry_run(text, [])

    with tempfile.TemporaryDirectory(prefix="datasmith-probe-venv-") as tmpdir:
        scratch = Path(tmpdir) / "venv"
        cp = run_uv(["venv", str(scratch), "--python", python_version])
        if cp.returncode != 0:
            log = strip_ansi(cp.stdout.decode() + "\n" + cp.stderr.decode())
            logger.debug("Could not build a Python %s probe environment: %s", python_version, log)
            return False, f"{VENV_SETUP_FAILED}: no Python {python_version} environment could be built\n{log}"
        return _run_dry_run(text, ["--python", str(_venv_interpreter(scratch))])


def uv_build_and_read_metadata(project_dir: Path) -> tuple[str | None, str | None, list[str], str | None]:
    """Run `uv build` in the project directory, then read metadata from the wheel.

    This is a best-effort fallback — many repos have dynamic setup.py files that
    fail in partial clones. Failures are expected and logged at debug level.
    """
    import subprocess as _sp

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
        logger.debug(
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
