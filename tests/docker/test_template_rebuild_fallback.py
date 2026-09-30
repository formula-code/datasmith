"""The rebuild after a patch falls back to an isolated build, as the image build does (old numpy)."""

import re
import subprocess
from pathlib import Path

_TEST_SH = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "test.sh"


def _rebuild_script() -> str:
    text = _TEST_SH.read_text()
    body = re.search(r"cat > /tmp/fc_rebuild.sh <<'SHEOF'\n(.*?)\nSHEOF", text, re.S).group(1)
    return re.sub(r"\{\{[^}]*\}\}", "X", body)


def _run(tmp_path: Path, fail_no_isolation: bool) -> subprocess.CompletedProcess:
    log = tmp_path / "calls.txt"
    fake = f"""python() {{
  echo "$*" >> {log}
  case "$*" in *numpy*|*pkg_resources*) return 1;; esac
  case "$*" in *--no-build-isolation*) return {1 if fail_no_isolation else 0};; esac
  return 0
}}
cd() {{ :; }}
"""
    script = fake + _rebuild_script().replace("set -eo pipefail\n", "set -eo pipefail\n", 1)
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30), log


def test_isolated_build_after_a_failed_no_isolation_build(tmp_path):
    out, log = _run(tmp_path, fail_no_isolation=True)
    installs = [line for line in log.read_text().splitlines() if "pip install" in line]
    assert out.returncode == 0, out.stderr
    assert len(installs) == 2 and "--no-build-isolation" in installs[0] and "--no-build-isolation" not in installs[1]


def test_no_isolation_build_that_works_runs_once(tmp_path):
    out, log = _run(tmp_path, fail_no_isolation=False)
    assert out.returncode == 0, out.stderr
    assert sum("pip install" in line for line in log.read_text().splitlines()) == 1
