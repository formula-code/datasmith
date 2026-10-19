import json
from pathlib import Path

import pytest
import yaml

from datasmith.verify import capture, measure
from datasmith.verify.survey import Survey

TEST_SH = "python /tests/lsv_measure.py --x\nsnapshot step\npython /tests/pytest_runner.py --y\n"


@pytest.fixture
def survey(tmp_path):
    td = tmp_path / "pinned" / "o__r__1"
    for d in ("solution", "tests"):
        (td / d).mkdir(parents=True)
    (td / "solution" / "solve.sh").write_text("+++ b/pkg/a.py\n")
    (td / "tests" / "test.sh").write_text(TEST_SH)
    (tmp_path / "oracle").mkdir()
    (tmp_path / "oracle" / "o__r__1.json").write_text(json.dumps({"task": "o__r__1", "runs": []}))
    s = Survey(tmp_path)
    s.save_state("o__r__1", {"phase": "measured"})
    return s


def fake_harbor(root, entries, nop_summary):
    def run(cmd, **kw):
        cfg = yaml.safe_load(Path(cmd[cmd.index("--config") + 1]).read_text())
        art = root / "jobs" / cfg["job_name"] / "t1" / "verifier" / "artifacts"
        (art / ".snapshots").mkdir(parents=True)
        (art.parent.parent / "result.json").write_text("{}")
        nop = any(a.get("import_path") == measure.NOP_AGENT for a in cfg["agents"])
        if nop:
            (art / "summary_1.json").write_text(json.dumps(nop_summary))
        else:
            assert "FC_AGENT_TAG" not in cfg["environment"]["env"]
            (art / ".snapshots" / "baseline.json").write_text(json.dumps({"entries": entries}))

    return run


def test_capture_task_dir_skips_timing_and_pytest(survey):
    root = capture.capture_task_dir(survey, "o__r__1")
    assert (root / "o__r__1" / "tests" / "test.sh").read_text() == "true\nsnapshot step\ntrue\n"
    assert (survey.task_dir("o__r__1") / "tests" / "test.sh").read_text() == TEST_SH


def test_capture_stores_reference_and_no_change_mismatches(survey):
    run = fake_harbor(
        survey.root, {"a": "pass", "b": "skip"}, {"pass-to-fail": 1, "pass-to-pass": 4, "pass-to-fail-ids": ["a"]}
    )
    assert capture.capture(survey, "o__r__1", measure.Settings(), run=run) == "captured"
    rec = json.loads((survey.root / "oracle" / "o__r__1.json").read_text())
    assert rec["snapshot_source"] == "fresh" and rec["snapshot_nop_mismatch"] == 1
    assert (survey.root / "snapshots" / "o__r__1" / "baseline.json").exists()


def test_capture_with_only_skips_keeps_no_reference(survey):
    run = fake_harbor(survey.root, {"a": "skip"}, {})
    assert capture.capture(survey, "o__r__1", measure.Settings(), run=run) == "no_snapshots"
    assert not (survey.root / "snapshots").exists()
