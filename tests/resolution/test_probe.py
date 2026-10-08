"""Advisory only. It must never raise, and never decide eligibility."""

import pytest

from datasmith.resolution.pin import Pinned
from datasmith.resolution.probe import PROBE_RANK, probe
from datasmith.resolution.requirements import Dropped


def test_empty_seed_is_empty_not_failed():
    assert probe(Pinned(), python_version="3.11").status == "empty"


def test_clean_dry_run_is_installable(monkeypatch):
    monkeypatch.setattr("datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (True, "ok"))
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "installable"
    assert r.log == "ok"


def test_relaxed_cutoff_is_unresolved_even_when_it_installs(monkeypatch):
    monkeypatch.setattr("datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (True, "ok"))
    r = probe(Pinned(requirements=["numpy==2.0"], cutoff_relaxed=True), python_version="3.11")
    assert r.status == "unresolved"


def test_failed_dry_run_is_failed(monkeypatch):
    monkeypatch.setattr("datasmith.resolution.probe.uv_dry_run_install", lambda *a, **k: (False, "conflict"))
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "failed"
    assert "conflict" in r.log


def test_a_raising_uv_is_caught_not_propagated(monkeypatch):
    def _boom(*a, **k):
        raise OSError("uv is missing")

    monkeypatch.setattr("datasmith.resolution.probe.uv_dry_run_install", _boom)
    r = probe(Pinned(requirements=["numpy==2.0"]), python_version="3.11")
    assert r.status == "failed"
    assert "uv is missing" in r.log


def test_pin_failure_is_failed_not_empty():
    p = Pinned(dropped=[Dropped(raw="numpy", reason="compile failed: unsatisfiable")])
    assert probe(p, python_version="3.11").status == "failed"


@pytest.mark.parametrize("status", ["installable", "unresolved", "failed", "empty"])
def test_every_status_has_a_rank(status):
    assert status in PROBE_RANK


def test_rank_orders_best_first():
    assert PROBE_RANK["installable"] < PROBE_RANK["unresolved"] < PROBE_RANK["failed"] < PROBE_RANK["empty"]
