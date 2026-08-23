"""Nothing is excluded for its probe result; it only decides who runs first."""

import subprocess

from datasmith.resolution.probe import PROBE_RANK
from datasmith.update.pipeline import order_by_probe


def test_no_source_file_filters_on_can_install():
    cp = subprocess.run(
        ["grep", "-rn", "can_install", "src/datasmith/update", "src/datasmith/runners"],
        capture_output=True,
        text=True,
    )
    offending = [ln for ln in cp.stdout.splitlines() if "filters=" in ln or '"can_install": True' in ln]
    assert offending == [], f"can_install is still a filter:\n{chr(10).join(offending)}"


def test_ordering_puts_installable_first():
    rows = [
        {"sha": "c", "probe_status": "failed"},
        {"sha": "a", "probe_status": "installable"},
        {"sha": "d", "probe_status": "empty"},
        {"sha": "b", "probe_status": "unresolved"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["a", "b", "c", "d"]


def test_ordering_keeps_every_row():
    rows = [{"sha": str(i), "probe_status": s} for i, s in enumerate(PROBE_RANK)]
    assert len(order_by_probe(rows)) == len(rows)


def test_unknown_and_missing_status_sort_last_but_survive():
    rows = [
        {"sha": "x", "probe_status": None},
        {"sha": "y", "probe_status": "installable"},
        {"sha": "z", "probe_status": "bogus"},
    ]
    out = order_by_probe(rows)
    assert out[0]["sha"] == "y"
    assert len(out) == 3


def test_ordering_is_stable_within_a_status():
    rows = [
        {"sha": "b", "probe_status": "installable"},
        {"sha": "a", "probe_status": "installable"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["b", "a"]
