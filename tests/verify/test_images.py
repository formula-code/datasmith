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
