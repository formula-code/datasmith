import json
import re
import subprocess
import sys
from pathlib import Path

TEST_SH = Path(__file__).resolve().parents[2] / "src/datasmith/harbor_adapter/template/tests/test.sh"


def test_failed_rebuild_writes_a_failure_reward(tmp_path):
    text = TEST_SH.read_text()
    block = text[text.index("rebuild after the patch failed"):]
    code = re.search(r"<<'PYEOF'\n(.*?)\nPYEOF", block, re.S).group(1)
    subprocess.run([sys.executable, "-", str(tmp_path)], input=code, text=True, check=True)
    reward = json.loads((tmp_path / "reward.json").read_text())
    assert reward["build_failed"] is True and reward["pytest_ran"] is False and reward["snapshot_verify_ran"] is False
