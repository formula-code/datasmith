import json

from datasmith.measurement.noise import load_rule
from datasmith.verify import report, review, status
from datasmith.verify.cli import main
from datasmith.verify.survey import Survey, display_id, task_id


def test_ids_round_trip():
    assert task_id("scikit-learn/scikit-learn#123") == "scikit-learn__scikit-learn__123"
    assert display_id("pydata__xarray__3588") == "pydata/xarray#3588"


def test_auto_rules_pick_no_gain_and_unmeasured(survey):
    s = Survey(survey)
    picked = {d["task"]: d["rule"] for d in review.auto_decisions(s)}
    assert picked == {"o__r__2": "oracle_up_0", "o__r__3": "uncovered"}
    review.apply_auto(s)
    decided = {r["task"]: r["decision"] for r in s.reviews()}
    assert decided == {"o/r#1": "pending", "o/r#2": "not_approved", "o/r#3": "not_approved"}
    assert review.auto_decisions(s) == []


def test_auto_rules_never_change_a_decided_row(survey):
    s = Survey(survey)
    review.set_decision(s, "o/r#2", "approved", note="kept by hand")
    assert "o__r__2" not in {d["task"] for d in review.auto_decisions(s)}


def test_set_decision_keeps_a_backup(survey):
    s = Survey(survey)
    review.set_decision(s, "o__r__1", "approved", note="gold")
    assert list(survey.glob("manual_review.csv.bak-*"))
    assert next(r for r in s.reviews() if r["task"] == "o/r#1")["note"] == "gold"


def test_add_pending_adds_only_new_rows(survey):
    s = Survey(survey)
    assert review.add_pending(s, ["o__r__1", "o__r__9"]) == ["o/r#9"]
    assert len(s.reviews()) == 4


def test_report_summarizes_pending_measured_tasks(survey):
    s = Survey(survey)
    out = {x["task"]: x for x in report.summaries(s, load_rule(s.rule_path))}
    assert set(out) == {"o__r__1", "o__r__2"}
    assert out["o__r__1"]["files"] == ["pkg/a/core.py"]
    assert out["o__r__1"]["prompt"] == "The code is slow."
    assert out["o__r__1"]["up_in_changed_packages"] == 1
    assert "### o__r__1" in report.render(out["o__r__1"])


def test_failure_reason_classes(tmp_path):
    log = tmp_path / "t.log"
    log.write_text("Error running benchmark.py discover\nModuleNotFoundError: No module named 'numba'\n")
    assert report.failure_reason(log) == {
        "reason": "ModuleNotFoundError: No module named 'numba'",
        "class": "missing_module",
        "missing": "numba",
    }
    log.write_text("RuntimeError: dictionary changed size during iteration\n")
    assert report.failure_reason(log)["class"] == "dictionary_changed"


def test_status_counts(survey):
    c = status.counts(Survey(survey))
    assert c["tasks"] == 3
    assert c["phases"] == {"measured": 2, "uncovered": 1}


def test_cli_status_and_dry_run(survey, capsys):
    assert main(["status", str(survey)]) == 0
    assert json.loads(capsys.readouterr().out)["review"] == {"pending": 3}
    assert main(["auto", str(survey), "--dry-run"]) == 0
    assert "would mark 2 not_approved" in capsys.readouterr().out
    assert {r["decision"] for r in Survey(survey).reviews()} == {"pending"}


def test_every_command_has_a_handler():
    import argparse

    from datasmith.verify import cli

    seen = []
    real = argparse.ArgumentParser.parse_args

    def capture(self, args=None, namespace=None):
        if self.prog == "fc-verify":
            sub = next(a for a in self._actions if isinstance(a, argparse._SubParsersAction))
            seen.extend(sub.choices)
            raise SystemExit(0)
        return real(self, args, namespace)

    argparse.ArgumentParser.parse_args = capture
    try:
        cli.main(["status", "x"])
    except SystemExit:
        pass
    finally:
        argparse.ArgumentParser.parse_args = real
    assert set(seen) == set(cli.COMMANDS) | set(cli.NO_SURVEY)


def test_readiness_rows_without_network(survey):
    from datasmith.verify import release

    s = Survey(survey)
    review.set_decision(s, "o/r#1", "approved")
    (survey / "pinned/o__r__1/task.toml").write_text('[environment]\ndocker_image = "fc-task/o__r__1:abc"\n')
    rows = release.readiness(s, registry={"o__r__1__abc"}, giveaway={"o__r__1": 3})
    assert len(rows) == 1
    r = rows[0]
    assert (r["in_registry"], r["oracle_runs"], r["nop_runs"], r["prompt_giveaway"]) == (True, 3, 3, 3)
    assert release.summary(rows)["no_license"] == 1
