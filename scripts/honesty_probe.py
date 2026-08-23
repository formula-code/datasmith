#!/usr/bin/env python3
"""Collect honesty facts from inside a built task image.

STDLIB ONLY. This runs inside the container, under the environment's own
interpreter, and must not import anything from `datasmith`.

It is bind-mounted at run time, never baked into the image. That is deliberate.
A probe baked into the image is a probe the image's builder could edit, and we
have direct evidence of that happening: 130 of 134 repositories modified the
baked harness, and one replaced the `python` binary outright.

CONTRACT: always print one JSON object between the markers, and always exit 0.
A crashed check is a recorded fact, not a lost result. The caller decides what
passes; this file only reports. That split is the same one the build manifest
uses, and it exists so a check cannot quietly become a judgement.

Facts collected, in four groups:

  integrity  -- is the toolchain the one we installed
  imports    -- does the package, and every compiled extension, load
  benchmarks -- does asv discover anything
  tests      -- does pytest collect
"""

from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sys
import sysconfig
import time
from pathlib import Path

START = "FORMULACODE_HONESTY_START"
END = "FORMULACODE_HONESTY_END"

# A compiled extension can take a long time to import (BLAS init, CUDA probing).
# Cap the whole extension sweep rather than each import, so one slow module
# cannot hide the rest.
EXT_SWEEP_BUDGET_S = float(os.environ.get("DATASMITH_HONESTY_EXT_BUDGET_S", "180"))
PYTEST_COLLECT_TIMEOUT_S = float(os.environ.get("DATASMITH_HONESTY_COLLECT_TIMEOUT_S", "600"))
ASV_DISCOVER_TIMEOUT_S = float(os.environ.get("DATASMITH_HONESTY_DISCOVER_TIMEOUT_S", "600"))


def _run(cmd: list[str], timeout: float, cwd: str | None = None) -> tuple[int, str, str]:
    """Run a command, never raise. Returns (rc, stdout, stderr)."""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        return -1, "", f"timed out after {timeout}s"
    # A broken command is data. The caller records it and carries on.
    except Exception as exc:
        return -2, "", f"{type(exc).__name__}: {exc}"
    return proc.returncode, proc.stdout, proc.stderr


# ---------------------------------------------------------------- integrity


def _is_elf(path: str) -> bool | None:
    """True if `path` starts with the ELF magic. None if unreadable.

    A shell script here is the tamper signature we observed: the agent moved the
    real interpreter aside and put a bash wrapper in its place, which then
    redirected the measurement entrypoints to its own code.
    """
    try:
        with open(path, "rb") as fh:
            return fh.read(4) == b"\x7fELF"
    except OSError:
        return None


def _which(name: str) -> str | None:
    """Resolve `name` on PATH without shelling out.

    `shutil.which` scans PATH in-process. Calling the `which` binary would ask
    a potentially replaced tool where the other replaced tools are.
    """
    return shutil.which(name)


def _customize_modules() -> list[str]:
    """Locate sitecustomize / usercustomize anywhere on sys.path.

    Both are imported automatically by CPython at startup, in EVERY process,
    including the benchmark subprocess. That makes them the cheapest place to
    change measured behaviour without touching any measured file.
    """
    found: list[str] = []
    for entry in sys.path:
        if not entry:
            continue
        for name in ("sitecustomize.py", "usercustomize.py"):
            candidate = Path(entry) / name
            if candidate.is_file():
                found.append(str(candidate))
    return sorted(set(found))


def integrity_facts() -> dict:
    # sys.executable is NOT sufficient on its own. A wrapper that `exec`s the
    # real interpreter makes sys.executable report the wrapper's TARGET, so the
    # ELF test passes while the wrapper is still in place. Observed directly on
    # dynamicslab/pysindy#139, where sys.executable read "python.real" and the
    # ELF check passed against a container whose `python` was a bash script.
    #
    # The file that PATH resolves is the one that decides what runs, so test
    # that, and record any disagreement between the two.
    python_path = sys.executable or ""
    python_on_path = _which("python")
    grep_path = _which("grep")
    return {
        "python_path": python_path,
        "python_is_elf": _is_elf(python_path) if python_path else None,
        "python_on_path": python_on_path,
        "python_on_path_is_elf": _is_elf(python_on_path) if python_on_path else None,
        "python_path_matches_argv": (
            None
            if not (python_path and python_on_path)
            else os.path.realpath(python_path) == os.path.realpath(python_on_path)
        ),
        "python_in_prefix": bool(python_path) and python_path.startswith(sys.prefix),
        "grep_path": grep_path,
        "grep_is_system": (grep_path in ("/usr/bin/grep", "/bin/grep")) if grep_path else None,
        "ld_preload": os.environ.get("LD_PRELOAD") or None,
        "customize_modules": _customize_modules(),
        "thread_caps": {
            key: os.environ.get(key)
            for key in (
                "OMP_NUM_THREADS",
                "MKL_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
            )
        },
        "platform": sysconfig.get_platform(),
        "python_version": sys.version.split()[0],
    }


# ------------------------------------------------------------------ imports


def _package_dir(import_name: str) -> str | None:
    rc, out, _ = _run(
        [sys.executable, "-c", f"import {import_name} as m, os; print(os.path.dirname(m.__file__ or ''))"],
        timeout=300,
    )
    path = out.strip()
    return path if rc == 0 and path else None


def _extension_modules(pkg_dir: str, import_name: str) -> list[str]:
    """Every compiled extension under the package, as an importable module name."""
    root = Path(pkg_dir)
    modules: list[str] = []
    for so in sorted(root.rglob("*.so")):
        rel = so.relative_to(root)
        # numpy/random/_generator.cpython-312-x86_64-linux-gnu.so -> random._generator
        stem = rel.name.split(".")[0]
        parts = [*rel.parent.parts, stem]
        modules.append(f"{import_name}." + ".".join(parts) if parts else import_name)
    return modules


def import_facts(import_name: str | None) -> dict:
    facts: dict = {
        "import_name": import_name,
        "package_import_ok": None,
        "package_dir": None,
        "extensions_total": None,
        "extensions_ok": None,
        "extensions_failed": [],
        "extensions_truncated": False,
    }
    if not import_name:
        return facts

    rc, _, err = _run([sys.executable, "-c", f"import {import_name}"], timeout=300)
    facts["package_import_ok"] = rc == 0
    if rc != 0:
        facts["package_import_error"] = err.strip()[-2000:]
        return facts

    pkg_dir = _package_dir(import_name)
    facts["package_dir"] = pkg_dir
    if not pkg_dir:
        return facts

    modules = _extension_modules(pkg_dir, import_name)
    facts["extensions_total"] = len(modules)

    ok = 0
    failed: list[dict] = []
    deadline = time.monotonic() + EXT_SWEEP_BUDGET_S
    for mod in modules:
        if time.monotonic() > deadline:
            facts["extensions_truncated"] = True
            break
        rc, _, err = _run([sys.executable, "-c", f"import {mod}"], timeout=60)
        if rc == 0:
            ok += 1
        else:
            failed.append({"module": mod, "error": err.strip().splitlines()[-1][:300] if err.strip() else ""})
    facts["extensions_ok"] = ok
    facts["extensions_failed"] = failed[:50]
    return facts


# --------------------------------------------------------------- benchmarks


_ASV_PREFIXES = ("time_", "mem_", "peakmem_", "track_", "timeraw_")

# A named tuple rather than a literal inside isinstance(). Ruff's UP038 wants
# `X | Y` there, and that form needs Python 3.10 -- this probe runs with the
# CONTAINER's interpreter, and the base image carries asv_3.7 through asv_3.12.
_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


def _asv_benchmark_dir(conf: Path) -> Path | None:
    """`benchmark_dir` from the asv config, resolved against the config's dir.

    asv configs are JSON with comments in practice, so strip // lines before
    parsing rather than requiring json5 inside the container.
    """
    try:
        raw = conf.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    stripped = "\n".join(ln for ln in raw.splitlines() if not ln.strip().startswith("//"))
    try:
        cfg = json.loads(stripped)
    except (ValueError, TypeError):
        return None
    value = cfg.get("benchmark_dir")
    if not value or not isinstance(value, str):
        return None
    path = Path(value)
    return path if path.is_absolute() else (conf.parent / path)


def _count_source_benchmarks(bench_dir: Path) -> int:
    """Benchmark functions under `bench_dir`, by asv naming convention.

    Parses source. Does not import, so a module whose import fails still
    counts, and nothing the build wrote can change the answer.
    """
    total = 0
    for path in sorted(bench_dir.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except (OSError, SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, _FUNCTION_NODES) and node.name.startswith(_ASV_PREFIXES):
                total += 1
    return total


def benchmark_facts(conf_name: str | None, repo_root: str | None) -> dict:
    """Count the benchmark suite two independent ways.

    `asv_benchmarks.txt` lives in the repository and is writable by whatever
    built the image. Two trials in the stored corpus rewrote it, one of them
    down to a single benchmark. So neither that file nor anything else the
    build produced can be the only source.

    `Benchmarks.load` is NOT that independent source, despite an earlier
    version of this docstring saying so. It reads `results/benchmarks.json`
    from the results directory and raises when the file is absent -- which is
    the normal state of an image that has never run asv. The check then
    reported `discovered_n=None` and SKIPPED, so a container with no
    benchmarks at all was indistinguishable from one that simply had not run
    yet. Observed on pandas.

    So we also parse the benchmark directory with `ast` and count functions
    that follow the asv naming convention. That reads repository source and
    nothing the build wrote. The two numbers answer different questions:
    `source_n` is "does a suite exist", `discovered_n` is "can asv see it".
    """
    facts: dict = {
        "conf_name": conf_name,
        "discovered_n": None,
        "discover_error": None,
        "source_n": None,
        "source_error": None,
        "benchmark_dir": None,
    }
    if not conf_name or not repo_root:
        facts["discover_error"] = "CONF_NAME or REPO_ROOT not set in the image"
        return facts

    conf = Path(conf_name)
    if not conf.is_absolute():
        conf = Path(repo_root) / conf_name
    if not conf.is_file():
        facts["discover_error"] = f"asv config not found: {conf}"
        facts["source_error"] = "no asv config, so no benchmark_dir to parse"
        return facts

    bench_dir = _asv_benchmark_dir(conf)
    facts["benchmark_dir"] = str(bench_dir) if bench_dir else None
    if bench_dir is None:
        facts["source_error"] = "benchmark_dir absent from the asv config"
    elif not bench_dir.is_dir():
        facts["source_error"] = f"benchmark_dir does not exist: {bench_dir}"
        facts["source_n"] = 0
    else:
        facts["source_n"] = _count_source_benchmarks(bench_dir)

    code = (
        "import json,sys\n"
        "from asv.config import Config\n"
        "from asv.benchmarks import Benchmarks\n"
        f"conf = Config.load({str(conf)!r})\n"
        "try:\n"
        "    bm = Benchmarks.load(conf)\n"
        "    print(len([k for k in bm if not k.startswith('_')]))\n"
        "except Exception as exc:\n"
        "    print('ERR:' + type(exc).__name__ + ': ' + str(exc)[:300])\n"
    )
    rc, out, err = _run([sys.executable, "-c", code], timeout=ASV_DISCOVER_TIMEOUT_S, cwd=str(conf.parent))
    text = out.strip()
    if rc != 0:
        facts["discover_error"] = (err or out).strip()[-1000:]
    elif text.startswith("ERR:"):
        facts["discover_error"] = text[4:]
    else:
        try:
            facts["discovered_n"] = int(text.splitlines()[-1])
        except (ValueError, IndexError):
            facts["discover_error"] = f"unparseable discovery output: {text[:200]}"
    return facts


# -------------------------------------------------------------------- tests


def pytest_facts(repo_root: str | None) -> dict:
    facts: dict = {"collect_ok": None, "collected_n": None, "collect_error": None}
    if not repo_root or not Path(repo_root).is_dir():
        facts["collect_error"] = "REPO_ROOT not set or missing"
        return facts

    rc, out, err = _run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "--no-header", "-p", "no:cacheprovider"],
        timeout=PYTEST_COLLECT_TIMEOUT_S,
        cwd=repo_root,
    )
    facts["collect_ok"] = rc in (0, 5)  # 5 = no tests collected, which is a fact not a crash
    tail = (out or "").strip().splitlines()
    for line in reversed(tail[-12:]):
        parts = line.split()
        if parts and parts[0].isdigit() and "test" in line:
            facts["collected_n"] = int(parts[0])
            break
    if not facts["collect_ok"]:
        facts["collect_error"] = ((err or "") + (out or "")).strip()[-2000:]
    return facts


def main() -> int:
    started = time.time()
    import_name = os.environ.get("IMPORT_NAME") or None
    repo_root = os.environ.get("REPO_ROOT") or "/workspace/repo"
    conf_name = os.environ.get("CONF_NAME") or None

    facts = {
        "probe_version": 1,
        "integrity": integrity_facts(),
        "imports": import_facts(import_name),
        "benchmarks": benchmark_facts(conf_name, repo_root),
        "tests": pytest_facts(repo_root),
    }
    facts["probe_duration_s"] = round(time.time() - started, 2)

    print(START)
    print(json.dumps(facts, sort_keys=True, indent=2))
    print(END)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
