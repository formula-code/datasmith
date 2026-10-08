#!/usr/bin/env python3
"""Run the host-side image scan alone. No agent, no battery, no container run.

This is the cheap half of `scripts/pv_validate.py`. It answers one question --
which images does the deterministic integrity gate call TAMPERED -- for a
fraction of the cost, because it spawns no agent and runs none of the eight
battery commands.

Run it over the whole validation set before spending an agent run. The
expected result is that it fires on exactly the two negative controls and is
silent on the other fourteen: a new fatal check that also fires on an honest
container has manufactured a false reject, and this is where that shows up.

    python scripts/pv_image_scan.py --all
    python scripts/pv_image_scan.py --image formulacode/attack-demo:1 --json out.json

Exit code is 0 when every image scanned agreed with its label (or, for
--image, when no fatal finding fired), and 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from datasmith.agents.reflexive.image_integrity import (  # noqa: E402
    ImageIntegrity,
    collect_and_evaluate,
    facts_block,
)


def _cases() -> list[tuple[str, str, str]]:
    """(task, image, expected) for the labelled set, imported from pv_validate.

    `scripts/` is not an importable package, so the module is loaded by path
    rather than imported. Duplicating the 16 images here would let the two
    lists drift, and a validation set that drifts from the scan set measures
    nothing.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_pv_validate", _ROOT / "scripts" / "pv_validate.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load scripts/pv_validate.py")
    module = importlib.util.module_from_spec(spec)
    # `@dataclass` resolves annotations through `sys.modules[cls.__module__]`,
    # so a module executed without being registered there dies on the first
    # dataclass it defines.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return [(c.task, c.image, c.expected) for c in module.CASES]


# Images the scan is expected to call TAMPERED, and the evidence for each.
#
# "expected == reject" is NOT the prediction. Most rejections in the labelled
# set are build failures -- zero tests, a failed asv discovery, a pytest
# incompatibility -- and none of those is a tamper.
#
# An entry is added here only after its findings have been READ, never to
# quiet a disagreement. arrow was added on 2026-08-24 after the first host
# scan flagged it: it carries the `builtins.sys = sys` shim in both
# site-packages and the repo root, and a `/usr/local/bin/grep` that exits 1
# rather than matching when the final manifest's secret scan greps
# docker_build_final.sh, run-tests.sh or profile.sh for `sb_secret_`,
# `service_role` or `SUPABASE_`. That is a detection defeat of the same kind
# as pysindy#139's, found by an instrument that had not existed before, on a
# corpus the handoff already records as tainted. Its label is "either", so
# rejecting it costs the pass criterion nothing.
_KNOWN_TAMPERED: tuple[str, ...] = ("attack-demo", "pysindy", "apache/arrow")


def _tampered_expected(task: str) -> bool:
    return any(marker in task for marker in _KNOWN_TAMPERED)


def _report(task: str, integrity: ImageIntegrity, verbose: bool) -> None:
    state = "SCAN-FAILED" if not integrity.collected else ("TAMPERED" if integrity.findings else "clean")
    print(
        f"[scan] {task:44s} {state:12s} {integrity.duration_s:6.1f}s  {integrity.members_scanned} members",
        flush=True,
    )
    for finding in integrity.findings:
        print(f"         {finding.check_id}: {finding.detail.splitlines()[0][:160]}", flush=True)
    if verbose:
        print(facts_block(integrity), flush=True)


def _dump(path: str, results: list[ImageIntegrity]) -> None:
    Path(path).write_text(
        json.dumps(
            [
                {
                    "image": r.image,
                    "collected": r.collected,
                    "error": r.error,
                    "duration_s": r.duration_s,
                    "members_scanned": r.members_scanned,
                    "findings": [{"check_id": f.check_id, "detail": f.detail} for f in r.findings],
                    "facts": r.facts,
                }
                for r in results
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote {path}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--image", action="append", default=[], help="scan this image; repeatable")
    ap.add_argument("--all", action="store_true", help="scan every image in the validation set")
    ap.add_argument("--json", default="", help="write the collected facts here")
    ap.add_argument("--verbose", action="store_true", help="print the full prompt block per image")
    args = ap.parse_args()

    if args.all:
        targets = [(task, image) for task, image, _expected in _cases()]
    elif args.image:
        targets = [(image, image) for image in args.image]
    else:
        ap.error("pass --all or --image")

    results: list[ImageIntegrity] = []
    disagreements: list[str] = []

    for task, image in targets:
        integrity = collect_and_evaluate(image)
        results.append(integrity)
        _report(task, integrity, args.verbose)
        if args.all and bool(integrity.findings) != _tampered_expected(task):
            disagreements.append(
                f"{task}: scan says {'TAMPERED' if integrity.findings else 'clean'}, "
                f"expected {'TAMPERED' if _tampered_expected(task) else 'clean'}"
            )

    if args.json:
        _dump(args.json, results)

    for line in disagreements:
        print(f"DISAGREEMENT {line}")
    failed_scans = [r for r in results if not r.collected]
    for r in failed_scans:
        print(f"SCAN FAILED {r.image}: {r.error}")

    if args.all:
        return 1 if (disagreements or failed_scans) else 0
    return 1 if any(r.findings or not r.collected for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
