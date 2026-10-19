"""Commands the verifier runs inside the container under test.

Returns facts only; severity.py decides. A command that crashes yields a fact
with crashed=True and a non-zero rc, which the caller treats as a failing check,
not a skipped one, because the verifier chose to run it.
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DATASMITH_PV_BATTERY_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_BATTERY_TIMEOUT_S", "1800"))

# Cores one battery container may use; 0 disables the limit. modin, dask and pymc
# start one worker per visible core, so an uncapped container starves the host.
DATASMITH_PV_BATTERY_CPUS: float = float(os.environ.get("DATASMITH_PV_BATTERY_CPUS", "8"))

# Each entry is (fact_name, argv), run read-only inside the image. Env-only binaries such as
# asv are not on PATH, so they go through `micromamba run -n "$ENV_NAME"`.
# Every piped command needs `set -o pipefail`, or `cmd | tail` reports the exit status of tail.
_IN_ENV = 'set -o pipefail; cd "$REPO_ROOT" && micromamba run -n "$ENV_NAME"'

# Counts asv benchmark functions by parsing source, so a module with a missing
# optional dependency still counts.
_COUNT_SOURCE = r"""
import ast, os

conf = os.environ.get("CONF_NAME") or ""
if not os.path.isabs(conf):
    conf = os.path.join(os.environ.get("REPO_ROOT", "/workspace/repo"), conf)


def load_conf(path):
    # asv.conf.json is JSONC, not JSON. 12 of the 16 validation-set configs
    # carry an inline `// for git` after a value or a trailing comma before a
    # brace, and the first draft of this script dropped only WHOLE comment
    # lines -- so json.loads raised on 12 of 16 and every one of them printed
    # ERR, which the verifier graded as a hard failure of a healthy container.
    # asv's own loader is the definition of what this file means, so use it.
    try:
        from asv import util
        try:
            return util.load_json(path, js_comments=True)
        except TypeError:
            return util.load_json(path)
    except Exception:
        return {}


# benchmark_dir is optional -- asv resolves it relative to the config's own
# directory and most configs omit it. An unparseable config must not stop the
# count either: walking the config's directory still finds the benchmark
# package underneath it, which is a superset, never a silent zero.
bench = load_conf(conf).get("benchmark_dir") or ""
if not os.path.isabs(bench):
    bench = os.path.join(os.path.dirname(conf), bench)
pref = ("time_", "mem_", "peakmem_", "track_", "timeraw_")
n = 0
for root, _d, files in os.walk(bench):
    for name in files:
        if not name.endswith(".py"):
            continue
        try:
            tree = ast.parse(open(os.path.join(root, name), encoding="utf-8", errors="replace").read())
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(pref):
                n += 1
print(n)
"""

# `shlex.quote`, not `repr`: repr turns newlines into a literal backslash n, which breaks `python -c`.
_COUNT_SOURCE_ARG = shlex.quote(_COUNT_SOURCE)

BATTERY_COMMANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "pytest_collect",
        (
            "bash",
            "-lc",
            f"{_IN_ENV} python -m pytest --collect-only -q --no-header -p no:cacheprovider 2>&1 | tail -40",
        ),
    ),
    (
        "pytest_run",
        (
            "bash",
            "-lc",
            f"{_IN_ENV} python -m pytest -q --no-header -p no:cacheprovider "
            "--continue-on-collection-errors 2>&1 | tail -80",
        ),
    ),
    (
        "asv_discover",
        (
            "bash",
            "-lc",
            # --machine is required: each container gets a random hostname with no asv machine
            # record, and the templates seal a "dockertest" record at build time.
            f'{_IN_ENV} asv run --bench just-discover --config "$CONF_NAME" --python=same '
            "--machine=dockertest 2>&1 | tail -20",
        ),
    ),
    ("source_benchmark_count", ("bash", "-lc", f"{_IN_ENV} python -c {_COUNT_SOURCE_ARG} 2>&1 | tail -3")),
    (
        "import_sweep",
        (
            "bash",
            "-lc",
            f"{_IN_ENV} python -c \"import os, importlib; importlib.import_module(os.environ['IMPORT_NAME'])\""
            " 2>&1 | tail -20",
        ),
    ),
    ("pip_freeze", ("bash", "-lc", f"{_IN_ENV} python -m pip freeze 2>&1")),
    ("build_manifest", ("cat", "/opt/formulacode/build_manifest.json")),
    # Integrity facts. Tampered images such as attack-demo:1 pass every functional check
    # above, so these facts are the only basis to reject them. The probe imports nothing
    # from datasmith and runs read-only under the container's interpreter.
    (
        "integrity_probe",
        ("bash", "-lc", 'set -o pipefail; micromamba run -n "$ENV_NAME" python /opt/fc_probe.py 2>&1 | tail -60'),
    ),
)


@dataclass(frozen=True)
class BatteryFact:
    name: str
    command: str
    stdout: str
    stderr: str
    rc: int
    crashed: bool


# Mounted read-only at run time, because a probe baked in at build time could be edited by the producer.
_PROBE_SRC = Path(__file__).resolve().parents[4] / "scripts" / "honesty_probe.py"


def _docker_runner(image_tag: str, argv: list[str], timeout_s: int) -> tuple[str, str, int]:
    """Run one battery command in the image, and make sure it cannot outlive us.

    `subprocess.run(timeout=...)` kills only the docker client, so `--name` gives the
    timeout path a container to kill. `--cpus` caps one container's share of the host.
    """
    mounts: list[str] = []
    if _PROBE_SRC.is_file():
        mounts = ["-v", f"{_PROBE_SRC}:/opt/fc_probe.py:ro"]
    name = f"fc-battery-{uuid.uuid4().hex[:12]}"
    limits = ["--cpus", str(DATASMITH_PV_BATTERY_CPUS)] if DATASMITH_PV_BATTERY_CPUS else []
    try:
        proc = subprocess.run(
            ["docker", "run", "--rm", "--name", name, *limits, *mounts, "--entrypoint", argv[0], image_tag, *argv[1:]],
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, check=False, timeout=120)
        logger.warning("battery command timed out after %ss; killed container %s", timeout_s, name)
        raise
    return proc.stdout, proc.stderr, proc.returncode


def run_battery(
    image_tag: str,
    runner: Callable[[str, list[str], int], tuple[str, str, int]] | None = None,
    timeout_s: int = DATASMITH_PV_BATTERY_TIMEOUT_S,
) -> list[BatteryFact]:
    """Run every battery command and return one fact each.

    Never raises. A crash becomes a fact, because the alternative is an
    exception that some caller eventually swallows into an acceptance.
    """
    run = runner or _docker_runner
    facts: list[BatteryFact] = []
    for name, argv in BATTERY_COMMANDS:
        command = " ".join(argv)
        try:
            stdout, stderr, rc = run(image_tag, list(argv), timeout_s)
            facts.append(BatteryFact(name=name, command=command, stdout=stdout, stderr=stderr, rc=rc, crashed=False))
        except Exception as exc:  # a crash is a finding, not a raise
            logger.warning("battery command %s crashed on %s: %s", name, image_tag, exc)
            facts.append(
                BatteryFact(
                    name=name,
                    command=command,
                    stdout="",
                    stderr=f"{type(exc).__name__}: {exc}",
                    rc=-1,
                    crashed=True,
                )
            )
    return facts
