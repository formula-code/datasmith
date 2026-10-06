"""The built image gets no oracle diff, and the prompt's tooling section comes from the template."""

import importlib.util
import json
from pathlib import Path

from datasmith.harbor_adapter.adapter import FormulaCodeAdapter, FormulaCodeRecord
from datasmith.harbor_adapter.utils import render_instruction_md

_LSV_INIT = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "lsv_init.py"
_PATCH = "diff --git a/pkg/src/f.c b/pkg/src/f.c\n+x\ndiff --git a/asv_bench/b.py b/asv_bench/b.py\n+y\n"


def _render(tmp_path: Path) -> Path:
    rec = FormulaCodeRecord(
        container_name="img:tag",
        patch=_PATCH,
        owner="o",
        repo="r",
        issue_number=1,
        gt_hash="a" * 40,
        base_commit="b" * 40,
        instructions="**Tooling:**\nold commands\n\n**Task Description**\n\nmake it fast",
        repo_name="o/r",
    )
    return FormulaCodeAdapter(harbor_tasks_root=tmp_path / "tasks").generate_task(rec, rounds=1)


def test_image_config_has_no_oracle_diff_and_finds_the_same_source_root(tmp_path, monkeypatch):
    out = _render(tmp_path)
    image_cfg = out / "environment" / "config.json"
    trial_cfg = out / "tests" / "config.json"
    assert not {"patch", "gt_hash"} & set(json.loads(image_cfg.read_text()))
    assert json.loads(trial_cfg.read_text())["patch"] == _PATCH

    spec = importlib.util.spec_from_file_location("fc_lsv_init_render", _LSV_INIT)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    repo = tmp_path / "repo"
    for d in ("pkg/src", "other"):
        (repo / d).mkdir(parents=True)
    (repo / "other" / "__init__.py").write_text("")
    monkeypatch.setattr(m, "REPO_ROOT", repo)
    real_path = m.Path
    for cfg in (image_cfg, trial_cfg):
        monkeypatch.setattr(
            m, "Path", lambda p, *a, cfg=cfg: real_path(cfg) if p == "/tests/config.json" else real_path(p, *a)
        )
        assert m.detect_source_root() == repo / "pkg"


def test_prompt_replaces_stored_tooling_notes_and_keeps_the_task(tmp_path):
    text = render_instruction_md("**Tooling:**\nold commands\n\n**Task Description**\n\nmake it fast")
    assert "old commands" not in text
    assert "**Task Description**\n\nmake it fast" in text
    assert "--config=/workspace/asv.conf.json" in text
    assert (_render(tmp_path) / "instruction.md").read_text() == text
