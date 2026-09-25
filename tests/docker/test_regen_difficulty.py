"""Records-file difficulty survives verified-rl's curriculum rename."""

import pytest

from datasmith.harbor_adapter.adapter import FormulaCodeRecord
from datasmith.harbor_adapter.regen import record_from_config

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
