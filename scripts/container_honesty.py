#!/usr/bin/env python3
"""Judge whether a built task image is honest.

An image is honest when four things hold:

  1. the toolchain is the one we installed, and nothing shimmed it
  2. the package imports, and so does every compiled extension
  3. asv discovers benchmarks
  4. pytest collects

`scripts/honesty_probe.py` collects the facts inside the container. This file
decides what they mean. The split is deliberate and mirrors the build manifest:
a script that both measures and judges can quietly stop doing one of them.

THIS IS NOT A SECURITY BOUNDARY. Read this before trusting a verdict.

The probe runs inside the container, under an interpreter the image's builder
configured. `sitecustomize.py` is imported at startup, before the probe's first
line, so anything the probe observes can be lied about. This was demonstrated,
not theorised: 19 lines patching `Path.is_file` and `shutil.which` made this
gate report HONEST on a container whose `grep` had been replaced.

What it is good for is the careless case -- a half-built extension, a repo that
discovers no benchmarks, a package that will not import, and a tamper nobody
tried to hide. What it cannot do is resist a builder that wants to pass it.

The defences that survive a determined builder are elsewhere, and none of them
ask the container about itself:

  * do not pay the builder for the property being checked
  * rebuild from a recipe, so the builder's container is not the measured one
  * read the image from the host (`docker export`, `docker history`) rather
    than executing it
  * ask questions whose answers are already known

Checks are THREE-VALUED. A check whose input is absent is SKIPPED, never
passed. That rule exists because the reverse cost us a corpus: a counter that
returned 0 for "cannot detect" was read as "nothing detected".

Usage:

    python scripts/container_honesty.py --image formulacode/networkx-networkx:8148
    python scripts/container_honesty.py --image <tag> --json out.json
    python scripts/container_honesty.py --image <tag> --cpuset 100-107

Exit code is 0 when no FATAL check failed, and 1 otherwise. SKIPPED never
fails, and is always reported.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_PROBE = _ROOT / "scripts" / "honesty_probe.py"

_BLOCK = re.compile(r"FORMULACODE_HONESTY_START\s*\n(.*?)\nFORMULACODE_HONESTY_END", re.DOTALL)

# Sourcing the profile scripts is what puts IMPORT_NAME, CONF_NAME and
# REPO_ROOT in the probe's environment. `micromamba run` then gives the probe
# the environment's own interpreter, which is the one under test.
_IN_CONTAINER = """
set +u
source /etc/profile.d/asv_utils.sh 2>/dev/null || true
source /etc/profile.d/asv_build_vars.sh 2>/dev/null || true
eval "$(micromamba shell hook --shell=bash)" 2>/dev/null || true
micromamba activate "$ENV_NAME" || true
set -u
micromamba run -n "$ENV_NAME" python /honesty_probe.py
"""


def run_probe(image: str, cpuset: str | None, timeout: int) -> tuple[dict | None, str]:
    """Run the probe inside `image`. Returns (facts, raw_output)."""
    cmd = ["docker", "run", "--rm", "--pull", "never"]
    if cpuset:
        cmd += ["--cpuset-cpus", cpuset]
    cmd += [
        "-v",
        f"{_PROBE}:/honesty_probe.py:ro",
        "--entrypoint",
        "bash",
        image,
        "-lc",
        _IN_CONTAINER,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, errors="replace")
    except subprocess.TimeoutExpired:
        return None, f"probe exceeded {timeout}s"
    # A docker failure is a fact about the image, not a reason to abort the sweep.
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"

    raw = (proc.stdout or "") + (proc.stderr or "")
    match = _BLOCK.findall(proc.stdout or "")
    if not match:
        return None, raw[-4000:]
    try:
        return json.loads(match[-1]), raw[-4000:]
    except json.JSONDecodeError as exc:
        return None, f"probe emitted unparseable JSON: {exc}\n{raw[-2000:]}"


# ------------------------------------------------------------------ policy
#
# Each check returns True (pass), False (fail) or None (skip -- input absent).


def _c_python_is_elf(f: dict) -> bool | None:
    """The interpreter PATH resolves must be a real binary, not a shell script.

    Testing `sys.executable` alone is not enough. A wrapper that `exec`s the
    real interpreter makes `sys.executable` report the target, so the check
    passes with the wrapper still installed. pysindy#139 did exactly that:
    sys.executable read "python.real" and this check passed until it was
    pointed at the PATH-resolved file instead.
    """
    on_path = f["integrity"].get("python_on_path_is_elf")
    if on_path is not None:
        return bool(on_path)
    return f["integrity"].get("python_is_elf")


def _c_python_not_wrapped(f: dict) -> bool | None:
    """`python` on PATH and sys.executable must be the same file.

    A mismatch means something sits between the two. That is the wrapper
    signature, and it survives the ELF test on its own.
    """
    return f["integrity"].get("python_path_matches_argv")


def _c_python_in_prefix(f: dict) -> bool | None:
    value = f["integrity"].get("python_in_prefix")
    return None if value is None else bool(value)


def _c_grep_is_system(f: dict) -> bool | None:
    """One image replaced grep so the secret scan reported clean without scanning."""
    return f["integrity"].get("grep_is_system")


def _c_no_ld_preload(f: dict) -> bool | None:
    return f["integrity"].get("ld_preload") is None


def _c_no_customize(f: dict) -> bool | None:
    """sitecustomize runs in EVERY process, including the measured benchmark.

    130 of 134 repositories installed one to work around a missing `import sys`
    in our own pytest_runner. That import is fixed, so a new build has no
    reason to carry one.
    """
    mods = f["integrity"].get("customize_modules")
    return None if mods is None else len(mods) == 0


def _c_package_imports(f: dict) -> bool | None:
    return f["imports"].get("package_import_ok")


def _c_extensions_import(f: dict) -> bool | None:
    """A build that silently skipped an extension still imports the package."""
    total = f["imports"].get("extensions_total")
    if total is None:
        return None
    if f["imports"].get("extensions_truncated"):
        return None  # swept partially; cannot conclude
    return len(f["imports"].get("extensions_failed") or []) == 0


def _c_benchmarks_discovered(f: dict) -> bool | None:
    n = f["benchmarks"].get("discovered_n")
    return None if n is None else n > 0


def _c_pytest_collects(f: dict) -> bool | None:
    return f["tests"].get("collect_ok")


CHECKS: list[tuple[str, str, object]] = [
    ("python_is_elf", "fatal", _c_python_is_elf),
    ("python_not_wrapped", "fatal", _c_python_not_wrapped),
    ("python_in_prefix", "warn", _c_python_in_prefix),
    ("grep_is_system", "fatal", _c_grep_is_system),
    ("no_ld_preload", "fatal", _c_no_ld_preload),
    ("no_sitecustomize", "fatal", _c_no_customize),
    ("package_imports", "fatal", _c_package_imports),
    ("extensions_import", "fatal", _c_extensions_import),
    ("benchmarks_discovered", "fatal", _c_benchmarks_discovered),
    ("pytest_collects", "fatal", _c_pytest_collects),
]


def evaluate(facts: dict) -> dict:
    passed: list[str] = []
    failed: list[str] = []
    warned: list[str] = []
    skipped: list[str] = []
    for name, severity, fn in CHECKS:
        try:
            verdict = fn(facts)  # type: ignore[operator]
        # A check must never crash the gate. A raising check is a FAILED check.
        except Exception as exc:
            failed.append(f"{name} (check raised {type(exc).__name__}: {exc})")
            continue
        if verdict is None:
            skipped.append(name)
        elif verdict:
            passed.append(name)
        elif severity == "fatal":
            failed.append(name)
        else:
            warned.append(name)
    return {
        "honest": not failed,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "skipped": skipped,
    }


def summarise(image: str, facts: dict, verdict: dict) -> str:
    integ, imp, bench, tests = facts["integrity"], facts["imports"], facts["benchmarks"], facts["tests"]
    lines = [
        f"image            {image}",
        f"verdict          {'HONEST' if verdict['honest'] else 'NOT HONEST'}",
        "",
        f"  python (argv)  {integ.get('python_path')}  elf={integ.get('python_is_elf')}",
        f"  python (PATH)  {integ.get('python_on_path')}  elf={integ.get('python_on_path_is_elf')}"
        + ("" if integ.get("python_path_matches_argv") is not False else "   *** WRAPPED ***"),
        f"  grep           {integ.get('grep_path')}",
        f"  sitecustomize  {integ.get('customize_modules') or 'none'}",
        f"  LD_PRELOAD     {integ.get('ld_preload') or 'unset'}",
        f"  thread caps    {integ.get('thread_caps')}",
        f"  package        {imp.get('import_name')}  import_ok={imp.get('package_import_ok')}",
        f"  extensions     {imp.get('extensions_ok')}/{imp.get('extensions_total')} import"
        + ("  TRUNCATED" if imp.get("extensions_truncated") else ""),
        f"  benchmarks     {bench.get('discovered_n')}  err={bench.get('discover_error')}",
        f"  pytest         collect_ok={tests.get('collect_ok')} n={tests.get('collected_n')}",
        "",
        f"  passed   {', '.join(verdict['passed']) or '-'}",
        f"  FAILED   {', '.join(verdict['failed']) or '-'}",
        f"  warned   {', '.join(verdict['warned']) or '-'}",
        f"  skipped  {', '.join(verdict['skipped']) or '-'}",
    ]
    for entry in imp.get("extensions_failed") or []:
        lines.append(f"    extension failed: {entry.get('module')}: {entry.get('error')}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--image", required=True)
    ap.add_argument("--cpuset", default=None, help="e.g. 100-107")
    ap.add_argument("--timeout", type=int, default=2400)
    ap.add_argument("--json", default=None, help="write facts and verdict here")
    args = ap.parse_args()

    facts, raw = run_probe(args.image, args.cpuset, args.timeout)
    if facts is None:
        print(f"image            {args.image}")
        print("verdict          NOT HONEST (probe produced no facts)")
        print(raw)
        return 1

    verdict = evaluate(facts)
    print(summarise(args.image, facts, verdict))

    if args.json:
        Path(args.json).write_text(json.dumps({"image": args.image, "facts": facts, "verdict": verdict}, indent=2))
        print(f"\nwrote {args.json}")

    return 0 if verdict["honest"] else 1


if __name__ == "__main__":
    sys.exit(main())
