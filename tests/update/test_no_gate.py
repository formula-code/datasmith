"""Nothing is excluded for its probe result; it only decides who runs first."""

import subprocess
from pathlib import Path

from datasmith.resolution.probe import PROBE_RANK
from datasmith.update.pipeline import order_by_probe

# Anchor the grep at the repository, not at the current directory: run from
# anywhere else, a relative path makes grep exit 2, stdout comes back empty, and
# the guard passes while the gate is still there.
SRC = Path(__file__).resolve().parents[2] / "src" / "datasmith"
STAGES = (SRC / "update", SRC / "runners")

# ``-I`` skips binary files. Without it a ``__pycache__/*.pyc`` compiled from a
# source file that names can_install in a docstring makes grep report "binary
# file matches" on *stderr* -- so the guard would fail on build output rather
# than on a gate, which is not what it is asking about.
GREP = ["grep", "-rn", "-I"]


def _gate_lines(stdout: str) -> list[str]:
    """The lines that would mean ``can_install`` is still deciding something."""
    return [ln for ln in stdout.splitlines() if "filters=" in ln or '"can_install": True' in ln]


def test_no_source_file_filters_on_can_install():
    for path in STAGES:
        assert path.is_dir(), f"source tree not found at {path}"
    cp = subprocess.run(
        [*GREP, "can_install", *(str(p) for p in STAGES)],
        capture_output=True,
        text=True,
    )
    # grep exits 0 on a match and 1 on no match; anything else is grep itself
    # failing, which would make an empty stdout meaningless.
    assert cp.returncode in (0, 1), f"grep failed ({cp.returncode}): {cp.stderr}"
    assert cp.stderr == "", f"grep wrote to stderr: {cp.stderr}"
    offending = _gate_lines(cp.stdout)
    assert offending == [], f"can_install is still a filter:\n{chr(10).join(offending)}"


def test_the_guard_sees_a_planted_gate(tmp_path):
    # The guard above is only worth having if it can fail. A mention of
    # can_install is not enough -- both stages still name it in a docstring --
    # so the decoy has to carry the shape the guard actually looks for.
    (tmp_path / "decoy.py").write_text('rows = fetch_all("packages", filters={"can_install": True})\n')
    cp = subprocess.run([*GREP, "can_install", str(tmp_path)], capture_output=True, text=True)
    assert cp.returncode == 0
    assert _gate_lines(cp.stdout), f"the decoy did not read as a gate:\n{cp.stdout}"


def test_ordering_puts_installable_first():
    rows = [
        {"sha": "c", "probe_status": "failed"},
        {"sha": "a", "probe_status": "installable"},
        {"sha": "d", "probe_status": "empty"},
        {"sha": "b", "probe_status": "unresolved"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["a", "b", "c", "d"]


def test_ordering_keeps_every_row():
    rows = [{"sha": str(i), "probe_status": s} for i, s in enumerate(PROBE_RANK)]
    assert len(order_by_probe(rows)) == len(rows)


def test_unknown_and_missing_status_sort_last_but_survive():
    rows = [
        {"sha": "x", "probe_status": None},
        {"sha": "y", "probe_status": "installable"},
        {"sha": "z", "probe_status": "bogus"},
    ]
    out = order_by_probe(rows)
    assert out[0]["sha"] == "y"
    assert len(out) == 3


def test_ordering_is_stable_within_a_status():
    rows = [
        {"sha": "b", "probe_status": "installable"},
        {"sha": "a", "probe_status": "installable"},
    ]
    assert [r["sha"] for r in order_by_probe(rows)] == ["b", "a"]
