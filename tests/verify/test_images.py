import json
from pathlib import Path

import pytest

from datasmith.verify import images, pin, registry


def task(root: Path, name: str, dockerfile: str = "FROM base\n") -> Path:
    d = root / name
    (d / "environment").mkdir(parents=True)
    (d / "environment" / "Dockerfile").write_text(dockerfile)
    (d / "task.toml").write_text('[task]\nname = "x"\n\n[environment]\ncpus = 2\n')
    return d


def test_env_digest_changes_with_the_build_context(tmp_path):
    d = task(tmp_path, "o__r__1")
    first = images.env_digest(d / "environment")
    (d / "environment" / "Dockerfile").write_text("FROM other\n")
    assert images.env_digest(d / "environment") != first
    assert images.image_tag(d).startswith("fc-task/o__r__1:")


def test_set_and_read_pin():
    toml = '[task]\nname = "x"\n\n[environment]\ncpus = 2\n'
    pinned = pin.set_pin(toml, "fc-task/a:1")
    assert pin.read_pin(pinned) == "fc-task/a:1"
    assert pin.read_pin(pin.set_pin(pinned, "fc-task/a:2")) == "fc-task/a:2"
    assert pin.read_pin(pin.set_pin('[task]\nname = "x"\n', "t:1")) == "t:1"
    assert pin.read_pin('[task]\ndocker_image = "not-env"\n') is None
    assert pin.read_pin('[environment]\ndocker_image = ""\n') is None


def test_plan_pins_only_a_verified_image_of_the_same_context(tmp_path):
    d = task(tmp_path, "o__r__1")
    row = {"tag": "fc-task/o__r__1:abc", "verified": True, "env_digest": images.env_digest(d / "environment")}
    assert pin.plan(d, None) == ("unbuilt", None)
    assert pin.plan(d, {**row, "verified": False}) == ("unverified", None)
    assert pin.plan(d, {**row, "env_digest": "other"}) == ("env-mismatch", None)
    assert pin.plan(d, row) == ("unpinned", row["tag"])
    pin.write_pin(d, row["tag"])
    assert pin.plan(d, row) == ("ok", None)


def test_rows_by_task_keeps_the_last_row(tmp_path):
    m = tmp_path / "manifest.jsonl"
    for tag in ("a:1", "a:2"):
        images.append_jsonl(m, {"task_id": "a", "tag": tag})
    assert images.rows_by_task(m)["a"]["tag"] == "a:2"


def test_build_claims_each_task_once_and_records_rows(tmp_path, monkeypatch):
    for n in (1, 2, 3):
        task(tmp_path / "tasks", f"o__r__{n}")
    built = []

    def fake_build_one(task_dir, out, limits, preexisting):
        built.append(task_dir.name)
        if task_dir.name == "o__r__2":
            return None, {"task_id": task_dir.name, "stage": "build", "error": "boom"}
        tar = out / f"{task_dir.name}.tar.zst"
        tar.write_text("x")
        return {
            "task_id": task_dir.name,
            "tag": "t",
            "verified": True,
            "tar_path": str(tar),
            "tar_bytes": 1,
            "bake_seconds": 1,
        }, None

    monkeypatch.setattr(images, "build_one", fake_build_one)
    monkeypatch.setattr(images, "sh", lambda cmd, **kw: type("P", (), {"stdout": "", "returncode": 0})())
    monkeypatch.setattr(images, "root_free_gb", lambda: 1000.0)
    out = tmp_path / "out"
    ids = ["o__r__1", "o__r__2", "o__r__3", "o__r__1"]
    images.build(tmp_path / "tasks", ids, out, workers=2, limits=images.Limits(out_min_free_gb=0))
    assert sorted(built) == ["o__r__1", "o__r__2", "o__r__3"]
    assert {r["task_id"] for r in images.read_jsonl(out / "manifest.jsonl")} == {"o__r__1", "o__r__3"}
    assert [r["task_id"] for r in images.read_jsonl(out / "failures.jsonl")] == ["o__r__2"]
    images.build(tmp_path / "tasks", ids, out, workers=2, limits=images.Limits(out_min_free_gb=0))
    assert len(built) == 3


def test_build_stops_at_a_stop_file(tmp_path, monkeypatch):
    task(tmp_path / "tasks", "o__r__1")
    monkeypatch.setattr(images, "build_one", lambda *a: pytest.fail("built after STOP"))
    monkeypatch.setattr(images, "sh", lambda cmd, **kw: type("P", (), {"stdout": "", "returncode": 0})())
    out = tmp_path / "out"
    out.mkdir()
    (out / "STOP").touch()
    images.build(tmp_path / "tasks", ["o__r__1"], out)


class FakeRegistry:
    def __init__(self):
        self.pushed = []

    def push(self, task_id, tag):
        self.pushed.append(tag)
        return {"task_id": task_id, "tag": tag, "uri": f"reg/{tag}", "digest": "sha256:1"}


def test_publish_pins_scrubs_probes_and_pushes(tmp_path, monkeypatch):
    d = task(tmp_path / "tasks", "o__r__1")
    row = {
        "task_id": "o__r__1",
        "tag": "fc-task/o__r__1:abc",
        "image_id": "sha256:x",
        "verified": True,
        "env_digest": images.env_digest(d / "environment"),
        "tar_path": "/x.tar.zst",
        "tar_bytes": 1,
    }
    manifest = tmp_path / "manifest.jsonl"
    images.append_jsonl(manifest, row)
    monkeypatch.setattr(registry.images, "load", lambda r: r["tag"])
    monkeypatch.setattr(
        registry.scrub, "scrub", lambda image: (image + "-scrub", {"nested_git_removed": "", "nested_tree_same": True})
    )
    monkeypatch.setattr(registry.scrub, "probe", lambda t, image, gt: {"status": "clean"})
    monkeypatch.setattr(
        registry,
        "sh",
        lambda cmd, **kw: type("P", (), {"stdout": json.dumps([{"Id": "sha256:s", "Size": 5}]), "returncode": 0})(),
    )
    reg = FakeRegistry()
    out = tmp_path / "pub"
    registry.publish(tmp_path / "tasks", manifest, out, reg)
    assert reg.pushed == ["fc-task/o__r__1:abc-scrub"]
    assert pin.read_pin((d / "task.toml").read_text()) == "fc-task/o__r__1:abc-scrub"
    published = images.read_jsonl(out / "published.jsonl")
    assert published[0]["scrubbed_from"] == row["tag"] and published[0]["tar_path"] is None
    registry.publish(tmp_path / "tasks", manifest, out, reg)
    assert len(reg.pushed) == 1


def test_publish_refuses_a_leaking_image(tmp_path, monkeypatch):
    d = task(tmp_path / "tasks", "o__r__1")
    row = {"task_id": "o__r__1", "tag": "t:1", "verified": True, "env_digest": images.env_digest(d / "environment")}
    monkeypatch.setattr(registry.images, "load", lambda r: r["tag"])
    monkeypatch.setattr(registry.scrub, "scrub", lambda image: (image + "-scrub", {}))
    monkeypatch.setattr(registry.scrub, "probe", lambda t, image, gt: {"status": "leak", "leaks": "gt_commit"})
    monkeypatch.setattr(registry, "sh", lambda cmd, **kw: None)
    with pytest.raises(RuntimeError, match="still leaks"):
        registry.publish_one(d, row, FakeRegistry(), tmp_path)


def guarded(monkeypatch, tmp_path, frees, idle_s=10**12, step=60, cmd=("sleep", "1")):
    clock, readings = iter(range(0, 10**9, step)), iter(frees)
    monkeypatch.setattr(images, "root_free_gb", lambda: next(readings, frees[-1]))
    monkeypatch.setattr(images.time, "monotonic", lambda: next(clock))
    real_sleep = images.time.sleep
    monkeypatch.setattr(images.time, "sleep", lambda s: real_sleep(0.01))
    return images.run_guarded(list(cmd), tmp_path / "log", images.Limits(min_free_gb=12.0, idle_s=idle_s))[1]


def test_a_short_disk_spike_does_not_kill_the_build(monkeypatch, tmp_path):
    assert guarded(monkeypatch, tmp_path, [26, 9, 26]) == ""


def test_low_disk_that_lasts_kills_the_build(monkeypatch, tmp_path):
    assert "killed" in guarded(monkeypatch, tmp_path, [26, 9, 9, 9, 9, 9])


def test_below_the_hard_floor_kills_at_once(monkeypatch, tmp_path):
    assert "killed" in guarded(monkeypatch, tmp_path, [2])


def test_a_build_without_output_is_killed(monkeypatch, tmp_path):
    assert "no output" in guarded(monkeypatch, tmp_path, [26], idle_s=14400, step=600, cmd=("sleep", "30"))


def test_only_low_disk_is_a_disk_stop(tmp_path, monkeypatch):
    d = task(tmp_path, "o__r__1")
    (d / "render_stamp.json").write_text('{"params": {"base_image": "x"}}')
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "o__r__1.log").write_text("")
    monkeypatch.setattr(images, "image_exists", lambda ref: True)
    monkeypatch.setattr(images, "sh", lambda cmd, **kw: type("P", (), {"stdout": "", "returncode": 0})())
    for why, disk in (("no output for 14400s; killed", False), ("root free 9.0G < 12.0G; killed", True)):
        monkeypatch.setattr(images, "run_guarded", lambda *a, why=why, **kw: (-9, why))
        _, err = images.build_one(d, tmp_path, images.Limits(keep_if_free_gb=0.0), set())
        assert (err["stage"], err["disk"]) == ("build", disk)


def test_image_check_records_added_modules(monkeypatch):
    stdout = "\n".join([
        "QWEN=0.22.3",
        "IMPORTS=ok",
        "SNAPSHOT_TOOL=ok",
        "HEAD=abc",
        'LSV_INIT={"baseline_sha": "abc", "env_fingerprint": {"cpus": 2}}',
        'ADDED_DEPS={"discovery_retry": [{"module": "distributed", "installed": true}]}',
    ])
    monkeypatch.setattr(images, "sh", lambda cmd, **kw: type("P", (), {"stdout": stdout, "stderr": ""})())
    info, problems = images.check_image("t")
    assert problems == []
    assert info["added_deps"]["discovery_retry"][0]["module"] == "distributed"
    monkeypatch.setattr(
        images, "sh", lambda cmd, **kw: type("P", (), {"stdout": "HEAD=abc\nADDED_DEPS=", "stderr": ""})()
    )
    info, problems = images.check_image("t")
    assert info["added_deps"] is None and "no stored baseline in /opt/lsv/cache/lsv_init_results.json" in problems


def test_pr_number_sources():
    from datasmith.verify.scrub import pr_number

    assert pr_number("numpy__numpy__21464") == "21464"
    assert pr_number("", "fc-task/numpy__numpy__21464:cc3df4e2979b") == "21464"
    assert pr_number("", "formulacode/networkx-networkx:8138") == "8138"
    assert pr_number("", "formulacode/base:latest") == ""


def test_push_rows_skips_unverified_and_pushed(tmp_path, monkeypatch):
    m = tmp_path / "m.jsonl"
    for r in ({"task_id": "a", "tag": "t:a", "verified": True}, {"task_id": "b", "tag": "t:b", "verified": False}):
        images.append_jsonl(m, r)
    monkeypatch.setattr(registry.images, "load", lambda r: r["tag"])
    reg = FakeRegistry()
    out = tmp_path / "pushed.jsonl"
    assert [r["tag"] for r in registry.push_rows(m, out, reg)] == ["t:a"]
    assert registry.push_rows(m, out, reg) == []
