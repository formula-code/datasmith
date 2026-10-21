import re
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[2] / "src" / "datasmith" / "harbor_adapter" / "template"


def test_snapshot_steps_have_an_overall_time_limit():
    text = (TEMPLATE / "tests" / "test.sh").read_text()
    calls = re.findall(r"^\s*(.*)snapshot-tool (baseline|verify) \\$", text, re.M)
    assert sorted(step for _, step in calls) == ["baseline", "verify"]
    assert all(prefix.startswith('timeout --kill-after=60 "${SNAPSHOT_STEP_TIMEOUT}"') for prefix, _ in calls)
    assert 'SNAPSHOT_STEP_TIMEOUT="${FC_SNAPSHOT_STEP_TIMEOUT:-1800}"' in text


def test_images_and_template_pin_the_same_snapshot_tool():
    build = (TEMPLATE.parents[1] / "docker" / "templates" / "docker_build_final.sh").read_text()
    docker = (TEMPLATE / "environment" / "Dockerfile").read_text()
    ref = re.search(r"snapshot-tester\.git@([0-9a-f]{40})", build).group(1)
    assert f"ARG SNAPSHOT_TESTER_REF={ref}" in docker
