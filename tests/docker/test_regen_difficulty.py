"""Records-file difficulty survives verified-rl's curriculum rename."""

import json

import pytest

from datasmith.harbor_adapter.adapter import FormulaCodeRecord
from datasmith.harbor_adapter.regen import main, record_from_config

_BASE = {
    "owner": "o",
    "repo": "r",
    "issue_number": 1,
    "base_commit": "b" * 40,
    "container_name": "formulacode/o-r:1",
    "gt_hash": "g" * 40,
    "patch": "",
    "instructions": "x",
}
_DEFAULT = FormulaCodeRecord.__dataclass_fields__["difficulty"].default


@pytest.mark.parametrize(
    "extra,expected",
    [
        ({"difficulty": "medium"}, "medium"),
        (
            {"difficulty": "balanced", "difficulty_source": "curriculum_spec", "difficulty_datasmith": "medium"},
            "medium",
        ),
        ({"difficulty": "balanced", "difficulty_source": "curriculum_spec"}, _DEFAULT),
    ],
)
def test_record_difficulty(extra, expected):
    assert record_from_config({**_BASE, **extra}).difficulty == expected


def test_render_uses_datasmith_difficulty(tmp_path):
    row = {**_BASE, "difficulty": "very_hard", "difficulty_source": "curriculum_spec", "difficulty_datasmith": "easy"}
    records = tmp_path / "r.jsonl"
    records.write_text(json.dumps(row) + "\n")
    main(["render", "--records", str(records), "--out", str(tmp_path / "out"), "--no-pytest"])
    task = tmp_path / "out" / "o__r__1"
    assert 'difficulty = "easy"' in (task / "task.toml").read_text()
    assert json.loads((task / "tests" / "config.json").read_text())["difficulty"] == "easy"
