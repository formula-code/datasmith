#!/usr/bin/env python3
"""Validate the verifier against a labelled set of 16 containers.

Every image already exists locally, so a run rebuilds nothing.

The measurement is AGREEMENT AGAINST THE LABELS, reported as a confusion
matrix, with every disagreement inspected by hand. "It accepted the right
number" is not the result.

    python scripts/pv_validate.py --out docs/superpowers/plans/pv-validation.md
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LabelledCase:
    task: str
    image: str
    digest: str
    expected: str
    note: str


# Pinned by digest, not by tag. Tags are mutable and a later rebuild would move
# :<sha>-final silently, which would measure the verifier against labels drawn
# from a different container than the one it inspects.
CASES: tuple[LabelledCase, ...] = (
    LabelledCase(
        "networkx/networkx#8148",
        "formulacode/networkx-networkx:8148",
        "sha256:563037dcdd2a748c4fef9b39c90ba811916c73e1f6858b91f562b510a379036d",
        "accept",
        "honest, 10/10",
    ),
    LabelledCase(
        "pydata/bottleneck#468",
        "formulacode/pydata-bottleneck:468",
        "sha256:cbc646ccae315308564437a2ddc13a36b243fbb68359f00f439c2a604537189c",
        "accept",
        "honest, extensions 4/4",
    ),
    LabelledCase(
        "mie-lab/trackintel#596",
        "formulacode/mie-lab-trackintel:596",
        "sha256:a6314c89133f2fe55bcc817965b91de62a853d7c0992d9b389741da1ce112cf7",
        "accept",
        "honest",
    ),
    LabelledCase(
        "xarray-contrib/xbatcher#167",
        "formulacode/xarray-contrib-xbatcher:167",
        "sha256:2db3b1dd80e66e11d1525c416ef9de5f0876e57cf8784a14c406c4cb04103cf2",
        "accept",
        "honest",
    ),
    LabelledCase(
        "CalebBell/fluids#38",
        "formulacode/calebbell-fluids:7601ec820c6a-final",
        "sha256:535cdd9428ccaccce9fb118d3ecdc0fdf11f53bbd0567bb1549124975ff425b7",
        "accept",
        "554/559, 5 numba TypingErrors, soft",
    ),
    LabelledCase(
        "holoviz/datashader#1464",
        "formulacode/holoviz-datashader:28f6239bb6ae-final",
        "sha256:11ac5fdeeb63d2ac37e91cba6dd29ebb3af08d8afe2052592e5a498cb65cad5d",
        "reject",
        "576 pass, 1 pytest-version collection error, HARD",
    ),
    LabelledCase(
        "pydata/xarray#11216",
        "formulacode/pydata-xarray:e7b1e5bb69a4-final",
        "sha256:0ff723b44cf00edbbf3a6b8ebd337a9596f2b3cd9bf9a857a04b3669211df84d",
        "reject",
        "0 tests",
    ),
    LabelledCase(
        "joblib/joblib#1682",
        "formulacode/joblib-joblib:863994d5e39c-final",
        "sha256:5c7d50030b5707de7f27f1767bad18ac6a55948750c9b1aa0e069e8fb441c592",
        "reject",
        "1522 pass, asv discovery failed",
    ),
    LabelledCase(
        "dwavesystems/dimod#1371",
        "formulacode/dwavesystems-dimod:63d462986556-final",
        "sha256:806256654d983d6b293ff56cbf43eb137ac7b939899fe3902c8f28e42aa495ad",
        "reject",
        "7 collection errors",
    ),
    LabelledCase(
        "AllenCellModeling/aicsimageio#486",
        "formulacode/allencellmodeling-aicsimageio:c30ed9e2f881-final",
        "sha256:7b2971bc534ab3db9c1a8936e5e8f0370fa7eaaac863fdcde9861bcd5dceb3fe",
        "reject",
        "pytest-version incompat, NOT a missing Java stack",
    ),
    LabelledCase(
        "NCAR/geocat-comp#748",
        "formulacode/ncar-geocat-comp:afe16c43437f-final",
        "sha256:9eb7718ec3affeda91320896be834f07d940acf4c3542649a1a03124064d8581",
        "reject",
        "2 collection errors",
    ),
    LabelledCase(
        "bluesky/tiled#1283",
        "formulacode/bluesky-tiled:1283",
        "sha256:fabba18cfa70f9b4d3f25ddc79c645c1c5bf77cdeb08f17efda52b126cd91772",
        "accept",
        "SUCCEEDED 13:07:51 in 1401s after the backend fix; was a 60s BackendUnavailable failure before",
    ),
    LabelledCase(
        "attack-demo",
        "formulacode/attack-demo:1",
        "sha256:1096f41ab1fed65a7df3d1d92997755361266ccb3c15198e44bbb19b715e49f8",
        "reject",
        "NEGATIVE CONTROL: adversarial sitecustomize defeated the honesty gate",
    ),
    LabelledCase(
        "dynamicslab/pysindy#139",
        "formulacode/dynamicslab-pysindy:139",
        "sha256:f8227cfd2cdcb48997568e52c62c62a440cfd1b0087fc04eadfa0d929ca4929e",
        "reject",
        "NEGATIVE CONTROL: replaced grep, fails 4 honesty checks",
    ),
    # "either" is a real label, not a hedge. The spec says these two may go
    # either way provided the verdict is REASONED, so counting an accept here
    # as a false accept would fail the whole validation on a case the spec
    # explicitly allows.
    LabelledCase(
        "pandas-dev/pandas",
        "formulacode/pandas-dev-pandas:415dec58354c-final",
        "sha256:81d9fdde5aaa2fd63d59933915550f3293a327eedcaa08b0b2abf6ece30496ea",
        "either",
        "older corpus; collects 205357 tests",
    ),
    LabelledCase(
        "apache/arrow",
        "formulacode/apache-arrow:80622fa077e5-final",
        "sha256:e3d97135c1babda01dc7574d73ec72bc0bb27029ef13668a88fc619ca84758b6",
        "either",
        "older corpus, must be reasoned",
    ),
)

_NEGATIVE_CONTROL_MARKERS = ("attack-demo", "pysindy")


def _is_negative_control(case: LabelledCase) -> bool:
    return any(marker in case.image for marker in _NEGATIVE_CONTROL_MARKERS)


def confusion(results: list[tuple[str, str, str]]) -> dict[str, int]:
    """results are (task, expected, actual). "either" counts separately."""
    counts = {"true_accept": 0, "true_reject": 0, "false_accept": 0, "false_reject": 0, "either": 0}
    for _task, expected, actual in results:
        if expected == "either":
            counts["either"] += 1
        elif expected == actual:
            counts["true_accept" if actual == "accept" else "true_reject"] += 1
        else:
            counts["false_accept" if actual == "accept" else "false_reject"] += 1
    return counts


def passes_criterion(results: list[tuple[str, str, str]]) -> tuple[bool, list[str]]:
    """The spec's pass criterion, minus the manual and end-to-end conditions.

    A false REJECT does not block. It costs one rebuild round. A false ACCEPT
    puts a bad container in the dataset.

    Conditions 3 (every disagreement explained) and 4 (one end-to-end round)
    are human judgements and a live build, so they are not evaluated here.
    """
    by_task = {task: (expected, actual) for task, expected, actual in results}
    reasons: list[str] = []

    for case in CASES:
        pair = by_task.get(case.task)
        if pair is None:
            reasons.append(f"{case.task}: no result")
            continue
        expected, actual = pair
        if _is_negative_control(case) and actual != "reject":
            reasons.append(f"{case.task}: negative control was ACCEPTED")
        elif expected == "either":
            continue  # the spec allows either verdict here, if reasoned
        elif expected == "reject" and actual == "accept":
            reasons.append(f"{case.task}: false accept in the hard class")

    return (not reasons), reasons


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="docs/superpowers/plans/pv-validation.md")
    args = parser.parse_args()

    missing = [c.task for c in CASES if c.digest == "PASTE_DIGEST"]
    if missing:
        print(f"refusing to run: {len(missing)} case(s) still unpinned", file=sys.stderr)
        return 2

    print(f"{len(CASES)} cases pinned by digest; report destination {_ROOT / args.out}")
    print("Run the verifier over CASES and pass (task, expected, actual) triples to")
    print("confusion() and passes_criterion(). Wired in Task 10.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
