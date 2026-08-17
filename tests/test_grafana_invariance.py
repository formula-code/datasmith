"""Guard the public-facing Grafana figures against pipeline-side changes.

Operator constraint: reanalysis and new information must not move any figure
on the Grafana dashboard.

Measured blast radius (2026-08-13): five panels read ``candidate_containers``
and **every one is a COUNT(*) over rows**. So the rule is sharp --

    adding or changing a COLUMN on an existing row is invisible;
    DELETING a row moves five public figures at once.

These tests encode that rule so a future audit or backfill cannot quietly
break it. They parse the dashboard JSON rather than hardcoding a panel list,
so a newly added panel is covered automatically.
"""

import json
import re
from pathlib import Path

import pytest

_DASHBOARD = Path(__file__).parents[1] / "grafana" / "provisioning" / "dashboards-json" / "datasmith-overview.json"

# Scripts that write to candidate_containers and must never delete rows while
# the operator constraint stands.
_AUDIT_SCRIPTS = ("scripts/audit_timeout_verified.py",)


def _panels_with_sql() -> list[tuple[str, str]]:
    """Return (panel_title, rawSql) for every panel that carries SQL."""
    data = json.loads(_DASHBOARD.read_text())
    out: list[tuple[str, str]] = []

    def walk(node, panel=None):
        if isinstance(node, dict):
            if node.get("type") and node.get("title") is not None and "targets" in node:
                panel = node.get("title")
            for key, value in node.items():
                if key == "rawSql" and isinstance(value, str):
                    out.append((panel or "<untitled>", value))
                walk(value, panel)
        elif isinstance(node, list):
            for item in node:
                walk(item, panel)

    walk(data)
    return out


class TestDashboardShape:
    """The facts the constraint rests on. If these change, the risk analysis
    in docs/superpowers/specs/2026-08-13-followon-plans-design.md is stale and
    must be redone before any audit runs."""

    def test_dashboard_exists_and_has_sql_panels(self):
        panels = _panels_with_sql()
        assert panels, "no SQL panels parsed — this guard would pass vacuously"

    def test_every_candidate_containers_panel_is_a_row_count(self):
        """The load-bearing fact: all five panels COUNT rows. That is why a
        column write is safe and a row delete is not.

        If a future panel reads a column instead, this fails and the safety
        argument must be re-derived rather than assumed.
        """
        offenders = []
        for title, sql in _panels_with_sql():
            if "candidate_containers" not in sql:
                continue
            if not re.search(r"COUNT\s*\(", sql, re.IGNORECASE):
                offenders.append(title)
        assert not offenders, (
            f"panels reading candidate_containers without COUNT(): {offenders}. "
            "The 'column writes are invisible' argument no longer holds; "
            "re-derive the blast radius before running any audit."
        )

    def test_no_panel_reads_the_columns_the_audit_writes(self):
        """manifest_warnings and build_manifest are what Plan 4 and stage 6
        write. No panel may read them, or those writes become public figures."""
        offenders = []
        for title, sql in _panels_with_sql():
            for col in ("manifest_warnings", "build_manifest"):
                if col in sql:
                    offenders.append((title, col))
        assert not offenders, (
            f"a Grafana panel now reads an audit-written column: {offenders}. Writing it would move a public figure."
        )

    def test_resource_metrics_panels_do_not_source_from_candidate_containers(self):
        """The three resource_metrics panels read error_logs. If one ever
        sourced from candidate_containers, --calibrate's report-only rule
        would become load-bearing for a public figure rather than merely
        prudent."""
        offenders = []
        for title, sql in _panels_with_sql():
            if "resource_metrics" in sql and "candidate_containers" in sql:
                offenders.append(title)
        assert not offenders, f"panel(s) reading resource_metrics from candidate_containers: {offenders}"


class TestAuditScriptsNeverDelete:
    """Static guard on the audit tooling itself.

    Skips cleanly until the script exists, so it lands green now and starts
    guarding the moment Plan 4 is implemented.
    """

    @pytest.mark.parametrize("rel", _AUDIT_SCRIPTS)
    def test_no_delete_against_candidate_containers(self, rel):
        path = Path(__file__).parents[1] / rel
        if not path.exists():
            pytest.skip(f"{rel} not implemented yet")
        src = path.read_text()
        assert ".delete(" not in src, (
            f"{rel} calls .delete() — deleting candidate_containers rows moves "
            "five public Grafana figures (Total Problems, PR to Problem Rate, "
            "Problems by Repository, Monthly distribution, Pipeline Funnel)."
        )
        assert "DELETE FROM" not in src.upper(), f"{rel} contains a raw DELETE"

    @pytest.mark.parametrize("rel", _AUDIT_SCRIPTS)
    def test_calibrate_does_not_write_resource_metrics(self, rel):
        """--calibrate must report, not rewrite. Overwriting resource_metrics
        on the audited rows would destroy the evidence being audited."""
        path = Path(__file__).parents[1] / rel
        if not path.exists():
            pytest.skip(f"{rel} not implemented yet")
        src = path.read_text()
        assert "_save_context" not in src, (
            f"{rel} routes through _save_context, which upserts "
            "candidate_containers and would overwrite resource_metrics on the "
            "rows whose original measurement is the evidence under audit."
        )
