import json
import math
from pathlib import Path

import yaml

from datasmith.verify import measure
from datasmith.verify.survey import Survey


def make_trial(job_dir, name, agent, speedups, *, exc=None, gate="1", sid="mg-x", reward=True, cpu="Intel Xeon"):
    t = job_dir / name
    art = t / "verifier" / "artifacts"
    (art / "lsv").mkdir(parents=True)
    (t / "config.json").write_text(json.dumps({"agent": agent}))
    result = {"started_at": "2026-10-19T00:00:00Z", "finished_at": "2026-10-19T01:00:00Z", "task_checksum": "c1"}
    if exc:
        result["exception_info"] = {"exception_type": exc}
    (t / "result.json").write_text(json.dumps(result))
    if reward:
        (t / "verifier" / "reward.json").write_text(
            json.dumps({"per_benchmark_speedups": speedups, "tests_passed": True})
        )
    (art / "lsv" / "lsv_init_results.json").write_text(
        json.dumps({"baseline_reuse": {"reused": True, "trial_fingerprint": {"cpu_model": cpu}}})
    )
    (art / "measure_gate.txt").write_text(f"init {sid}-i 1\nmeasure {sid} {gate}\n")
    return t


def fake_harbor(survey_root, task, *, broken=False):
    """A harbor stand-in that writes the job's trials from the config it is given."""

    def run(cmd, **kw):
        cfg = yaml.safe_load(Path(cmd[cmd.index("--config") + 1]).read_text())
        job_dir = survey_root / "jobs" / cfg["job_name"]
        for i, agent in enumerate(cfg["agents"]):
            nop = agent.get("import_path") == measure.NOP_AGENT
            speed = {"b.time_a": 1.0 if nop else 1.5}
            make_trial(
                job_dir,
                f"{task}__{i}",
                agent,
                speed,
                exc="SetupScriptError" if broken and i == 0 else None,
                sid=f"mg-{i}",
            )

    return run


def setup_task(tmp_path):
    for d in ("state", "pinned/o__r__1/solution"):
        (tmp_path / d).mkdir(parents=True)
    (tmp_path / "pinned/o__r__1/solution/solve.sh").write_text("+++ b/pkg/a.py\n+++ b/pkg/b.py\n")
    survey = Survey(tmp_path)
    survey.save_state("o__r__1", {"phase": "measure", "tag": "fc-task/o__r__1:x"})
    return survey


def test_trial_run_reads_role_speedups_and_confirmation(tmp_path):
    t = make_trial(tmp_path, "t1", {"name": "oracle", "model_name": "run2"}, {"b.x": 2.0, "b.y": 1.0, "b.bad": 0})
    (t / "verifier/artifacts/lsv/lsv_measure_results.json").write_text(
        json.dumps({"benchmarks": {"b.y": {"paired": {"log_ratios": [0.1, 0.3]}}}})
    )
    (t / "verifier/artifacts/lsv/lsv_measure_confirm.json").write_text(
        json.dumps({"benchmarks": {"b.y": {"paired": {"log_ratios": [0.2]}}}})
    )
    r = measure.trial_run(t, [])
    assert (r["role"], r["run"], r["cpu_model"], r["infra"]) == ("oracle", 2, "Intel Xeon", False)
    assert math.isclose(r["log_speedups"]["b.x"], math.log(2.0))
    assert math.isclose(r["log_speedups"]["b.y"], 0.2)
    assert "b.bad" not in r["log_speedups"]


def test_trial_run_marks_ungated_and_missing_reward_as_infra(tmp_path):
    assert measure.trial_run(make_trial(tmp_path, "a", {"name": "oracle"}, {}, gate="0"), [])["infra"]
    assert measure.trial_run(make_trial(tmp_path, "b", {"name": "oracle"}, {}, reward=False), [])["infra"]
    nop = measure.trial_run(make_trial(tmp_path, "c", {"import_path": measure.NOP_AGENT, "model_name": "run3"}, {}), [])
    assert (nop["role"], nop["run"]) == ("nop", 3)


def test_job_config_runs_oracle_and_no_change_runs(tmp_path):
    survey = setup_task(tmp_path)
    cfg = measure.job_config(survey, "o__r__1", "o__r__1__oracle", measure.Settings(), tmp_path / "pinned")
    agents = cfg["agents"]
    assert [a.get("model_name") for a in agents] == [None, "run2", "run3", "run1", "run2", "run3"]
    assert agents[-1]["kwargs"]["files"] == ["pkg/a.py", "pkg/b.py"]
    assert cfg["n_concurrent_trials"] == 6
    assert cfg["environment"]["env"]["MEASURE_GATE_URL"] == "http://127.0.0.1:8267"


def test_measure_task_writes_record_and_state(tmp_path, monkeypatch):
    survey = setup_task(tmp_path)
    monkeypatch.setattr(measure, "gate_timeouts", lambda s: [])
    phase = measure.measure_task(survey, "o__r__1", measure.Settings(), run=fake_harbor(tmp_path, "o__r__1"))
    assert phase == "measured"
    rec = json.loads((tmp_path / "oracle/o__r__1.json").read_text())
    assert [r["role"] for r in rec["runs"]] == ["oracle_baseline", "oracle", "oracle", "nop", "nop", "nop"]
    assert rec["cpu_models"] == ["Intel Xeon"]
    st = survey.state("o__r__1")
    assert (st["phase"], st["measure_jobs"], st["cpu_model"]) == ("measured", 1, "Intel Xeon")


def test_failed_job_gets_a_new_job_name_on_rerun(tmp_path, monkeypatch):
    survey = setup_task(tmp_path)
    monkeypatch.setattr(measure, "gate_timeouts", lambda s: [])
    assert (
        measure.measure_task(survey, "o__r__1", measure.Settings(), run=fake_harbor(tmp_path, "o__r__1", broken=True))
        == "error"
    )
    assert "infra failures" in survey.state("o__r__1")["last_error"]
    assert (
        measure.measure_task(survey, "o__r__1", measure.Settings(), run=fake_harbor(tmp_path, "o__r__1")) == "measured"
    )
    assert json.loads((tmp_path / "oracle/o__r__1.json").read_text())["job"] == "o__r__1__oracle-1"


def test_job_leases_and_release(tmp_path, monkeypatch):
    survey = setup_task(tmp_path)
    monkeypatch.setattr(measure, "gate_timeouts", lambda s: [])
    measure.measure_task(survey, "o__r__1", measure.Settings(), run=fake_harbor(tmp_path, "o__r__1"))
    job = tmp_path / "jobs" / "o__r__1__oracle"
    assert len(measure.job_leases(job)) == 12
    calls = []

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(measure.urllib.request, "urlopen", lambda req, timeout: calls.append(req.full_url) or Resp())
    assert len(measure.release_leases(job, "http://gate")) == 12
    assert calls[0].startswith("http://gate/release?sid=mg-")


def test_old_gate_logs_without_ids_are_skipped(tmp_path):
    (tmp_path / "t" / "artifacts").mkdir(parents=True)
    (tmp_path / "t" / "artifacts" / "measure_gate.txt").write_text("init 1\nmeasure 1\n")
    assert measure.job_leases(tmp_path) == []
