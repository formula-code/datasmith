"""Tests for scripts/audit_timeout_verified.py.

The audit's defining constraint is what it must NOT do: no row may be created
or deleted, because five public Grafana panels are COUNT(*) over
candidate_containers rows. tests/test_grafana_invariance.py guards that
statically; these guard the selection and stamping logic.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "audit_timeout_verified.py"


def _load():
    spec = importlib.util.spec_from_file_location("audit_timeout_verified", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _row(duration, warnings=None, owner="o", repo="r", sha="s"):
    return {
        "owner": owner,
        "repo": repo,
        "sha": sha,
        "issue_number": 1,
        "resource_metrics": {"test_duration_s": duration} if duration is not None else {},
        "manifest_warnings": warnings,
    }


class TestCohortSelection:
    def test_selects_rows_at_the_boundary(self):
        m = _load()
        assert len(m.select_cohort([_row(720.0)])) == 1

    def test_selects_rows_past_the_boundary(self):
        m = _load()
        assert len(m.select_cohort([_row(805.81)])) == 1

    def test_excludes_fast_rows(self):
        m = _load()
        assert m.select_cohort([_row(719.59)]) == []

    def test_tolerates_a_missing_duration(self):
        """Rows built before resource_metrics existed have no duration. They
        are not suspect -- they are unmeasured, a different thing."""
        m = _load()
        assert m.select_cohort([_row(None)]) == []

    def test_tolerates_a_non_numeric_duration(self):
        m = _load()
        row = _row(None)
        row["resource_metrics"] = {"test_duration_s": "not-a-number"}
        assert m.select_cohort([row]) == []

    def test_tolerates_resource_metrics_being_null(self):
        m = _load()
        row = _row(None)
        row["resource_metrics"] = None
        assert m.select_cohort([row]) == []

    def test_boundary_is_env_overridable(self, monkeypatch):
        monkeypatch.setenv("DATASMITH_VERIFY_SUSPECT_TIMEOUT_S", "600")
        m = _load()
        assert len(m.select_cohort([_row(650.0)])) == 1


class TestStampIsAdditive:
    def test_appends_to_an_empty_list(self):
        m = _load()
        assert m.merge_warning(None) == ["suspect_timeout_720s"]

    def test_preserves_existing_warnings(self):
        """A row carrying warnings from its own build must keep them --
        replacing the array would destroy build-time findings."""
        m = _load()
        assert m.merge_warning(["cpu_cap_unset"]) == ["cpu_cap_unset", "suspect_timeout_720s"]

    def test_is_idempotent(self):
        m = _load()
        once = m.merge_warning(None)
        assert m.merge_warning(once) == once


class TestRanking:
    def test_ranks_by_repo_descending(self):
        m = _load()
        cohort = [
            _row(800, owner="a", repo="x", sha="1"),
            _row(800, owner="a", repo="x", sha="2"),
            _row(800, owner="b", repo="y", sha="3"),
        ]
        assert m.rank_by_repo(cohort)[0] == ("a/x", 2)

    def test_ranking_is_by_repo_not_harbor_runs(self):
        """The original plan ranked by whether a harbor_runs row exists. It
        does not discriminate: zero of the 636 suspect rows have one
        (verified against the live database). Ranking by repo instead."""
        import ast

        tree = ast.parse(_SCRIPT.read_text())
        assert any(isinstance(n, ast.FunctionDef) and n.name == "rank_by_repo" for n in ast.walk(tree)), (
            "rank_by_repo is gone"
        )

        # Strip every docstring, then look for harbor_runs in what remains.
        # The script legitimately DISCUSSES harbor_runs at length -- explaining
        # why ranking on it is degenerate is the finding. Matching prose would
        # punish recording that.
        for node in ast.walk(tree):
            if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef):
                doc = ast.get_docstring(node, clean=False)
                if doc and node.body:
                    node.body = node.body[1:] if len(node.body) > 1 else [ast.Pass()]
        code = ast.unparse(tree)
        assert "harbor_runs" not in code, (
            "the audit ranks or filters on harbor_runs in CODE, but that field "
            "does not discriminate within this cohort: zero of the 636 suspect "
            "rows have one (verified against the live database)."
        )


class TestNeverDeletes:
    def test_no_delete_call_anywhere(self):
        m = _load()
        src = _SCRIPT.read_text()
        assert ".delete(" not in src
        assert not hasattr(m, "delete_rows")

    @pytest.mark.parametrize("fn", ["run_audit", "run_calibrate"])
    def test_entry_points_exist(self, fn):
        m = _load()
        assert callable(getattr(m, fn))
