"""Review summaries for measured tasks, and the build error behind a failed image."""

from __future__ import annotations

import collections
import math
import re
import statistics
from pathlib import Path
from typing import Any

from datasmith.measurement.noise import moved, repo_spreads, rule_for, sigmas
from datasmith.verify.survey import Survey, display_id, repo_of

_SOLUTION = re.compile(r"<< '__SOLUTION__'\n(.*?)\n__SOLUTION__", re.DOTALL)


def oracle_diff(task_dir: Path) -> str:
    """The oracle diff inside solution/solve.sh."""
    path = task_dir / "solution" / "solve.sh"
    m = _SOLUTION.search(path.read_text()) if path.exists() else None
    return m.group(1) if m else ""


def files_in_diff(diff: str) -> list[str]:
    files = [line[6:].strip() for line in diff.splitlines() if line.startswith("+++ b/")]
    return list(dict.fromkeys(files))


def prompt_task(instruction: str) -> str:
    """The task description section of instruction.md."""
    text = instruction.split("**Task Description**", 1)[-1].split("## Task Requirements", 1)[0]
    lines = [ln for ln in text.strip().splitlines() if ln.strip() and not ln.startswith("Your main goal")]
    return " ".join(lines).replace("Use the following information if needed to understand the problem:", "").strip()


def touched_packages(files: list[str]) -> set[str]:
    """Top-level package names under the source root that the diff changes, tests excluded."""
    out = set()
    for p in files:
        if "/tests/" in p or p.startswith("tests/"):
            continue
        parts = p.split("/")
        out.add(parts[1] if len(parts) > 2 else parts[-1].rsplit(".", 1)[0])
    return out


def summarize(survey: Survey, task: str, record: dict, rule: dict, spreads: dict[str, float]) -> dict[str, Any]:
    """Facts for review: prompt, patch, oracle and no-change moves on the k·sigma_b bar, by suite. No verdict."""
    k = float(rule_for(rule, repo_of(task)).get("k", 2))
    orc = sorted((r for r in record["runs"] if r["role"] != "nop"), key=lambda r: r["run"])
    nop = sorted((r for r in record["runs"] if r["role"] == "nop"), key=lambda r: r["run"])
    up, down, nop_up, nop_down, bars = [], [], 0, 0, []
    for b, (_, sigma) in sigmas(rule, record, spreads).items():
        bar = k * sigma
        bars.append(bar)
        o = [r["log_speedups"].get(b) for r in orc]
        if o and None not in o:
            u, d = moved(o, bar)
            m = math.exp(statistics.median(o))
            up += [(m, b)] if u else []
            down += [(m, b)] if d else []
        n = [r["log_speedups"].get(b) for r in nop]
        if any(v is not None for v in n):
            u, d = moved(n, bar)
            nop_up, nop_down = nop_up + u, nop_down + d
    task_dir = survey.task_dir(task)
    diff = oracle_diff(task_dir)
    files = files_in_diff(diff)
    pkgs = touched_packages(files)
    instruction = (task_dir / "instruction.md").read_text() if (task_dir / "instruction.md").exists() else ""
    return {
        "task": task,
        "prompt": prompt_task(instruction),
        "diff_lines": diff.count("\n"),
        "files": files,
        "n_benchmarks": len(bars),
        "bar_median": math.exp(statistics.median(bars)) if bars else None,
        "oracle_up": len(up),
        "oracle_down": len(down),
        "nop_up": nop_up,
        "nop_down": nop_down,
        "up_in_changed_packages": sum(any(p in b.split(".")[0] for p in pkgs) for _, b in up),
        "up_by_suite": collections.Counter(b.split(".")[0] for _, b in up).most_common(8),
        "top_up": sorted(up, reverse=True)[:5],
        "top_down": sorted(down)[:3],
    }


def render(s: dict[str, Any]) -> str:
    lines = [
        f"### {s['task']}",
        f"- Prompt: {s['prompt'][:400]}",
        f"- Patch: {s['diff_lines']} diff lines in {len(s['files'])} files: {', '.join(s['files'][:10])}",
        f"- Benchmarks: {s['n_benchmarks']}; noise bar median {s['bar_median']:.3f}x"
        if s["bar_median"]
        else "- Benchmarks: 0",
        f"- Oracle up {s['oracle_up']}, down {s['oracle_down']}; no-change runs, same rule: up {s['nop_up']}, down {s['nop_down']}",
        f"- Up benchmarks in suites named after a changed package: {s['up_in_changed_packages']} of {s['oracle_up']}",
        f"- Up by suite: {', '.join(f'{n} {c}' for n, c in s['up_by_suite']) or 'none'}",
    ]
    lines += [f"  - up {m:.3f}x {b}" for m, b in s["top_up"]]
    lines += [f"  - down {m:.3f}x {b}" for m, b in s["top_down"]]
    return "\n".join(lines)


def summaries(survey: Survey, rule: dict, tasks: list[str] | None = None) -> list[dict[str, Any]]:
    """Summaries for the given tasks, or for every measured task still pending review."""
    records = survey.records()
    spreads = repo_spreads(list(records.values()))
    if tasks is None:
        pending = {r["task"].lower() for r in survey.reviews() if r["decision"] in ("", "pending")}
        states = survey.states()
        tasks = [
            t for t in records if states.get(t, {}).get("phase") == "measured" and display_id(t).lower() in pending
        ]
    return [summarize(survey, t, records[t], rule, spreads) for t in tasks if t in records]


_ERROR = re.compile(
    r"(ModuleNotFoundError|ImportError|AttributeError|NameError|TypeError|ValueError|NoBenchmarksError|ASVError"
    r"|UserError: No __init__|CancelledError|no asv config resolved)[^\n]*"
)
_SKIP = re.compile(r"benchmarks\.json missing|During handling|Failed to build the project")
_MISSING = re.compile(
    r"No module named '([\w.]+)'|Missing optional dependency '([\w.-]+)'|[Pp]lease install '([\w.-]+)'"
    r"|require either `([\w.-]+)`"
)
FAILURE_CLASSES = (
    ("missing_module", re.compile(r"ModuleNotFoundError|Missing optional dependency|[Pp]lease install|require either")),
    ("syntax_error", re.compile(r"SyntaxError|IndentationError")),
    (
        "version_api_mismatch",
        re.compile(
            r"cannot import name|unexpected keyword|has no attribute|unsupported operand"
            r"|dtype size changed|separate project"
        ),
    ),
    ("missing_system_library", re.compile(r"\.so[.\d]*: cannot open shared object")),
    ("no_benchmark_package", re.compile(r"No __init__\.py")),
    ("no_benchmarks", re.compile(r"NoBenchmarksError")),
    ("dictionary_changed", re.compile(r"dictionary changed size during iteration")),
)


def failure_reason(log: Path) -> dict[str, str]:
    """The last error line of an image build log, its class and the first missing module after benchmark discovery."""
    lines = log.read_text(errors="replace").splitlines()
    hits = [m.group(0).strip() for line in lines if not _SKIP.search(line) for m in [_ERROR.search(line)] if m]
    hits += [line.strip() for line in lines if "dictionary changed size during iteration" in line]
    why = hits[-1][:200] if hits else ""
    start = max(
        (i for i, line in enumerate(lines) if "Error running" in line and "benchmark.py discover" in line),
        default=len(lines),
    )
    found = next((m for line in lines[start:] for m in [_MISSING.search(line)] if m), None)
    missing = next((g for g in found.groups() if g), "") if found else ""
    cls = next(
        (name for name, rx in FAILURE_CLASSES if rx.search(why) or (name == "missing_module" and missing)),
        "other" if why else "no_error_line",
    )
    return {"reason": why, "class": cls, "missing": missing}
