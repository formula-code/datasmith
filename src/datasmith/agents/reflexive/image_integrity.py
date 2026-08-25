"""Integrity facts read from the image, WITHOUT running the image.

This module exists because the previous design asked the container about
itself. `scripts/honesty_probe.py` runs inside the container, under an
interpreter the image's builder configured, and `sitecustomize.py` is imported
before the probe's first line. `formulacode/attack-demo:1` carries 19 lines
that patch `Path.is_file` and `shutil.which`; the probe then reported a clean
container, the verifier reasoned correctly from those false facts, and the
negative control was ACCEPTED. Anything the container runs, the container can
lie about.

So nothing here executes code from the image. `docker create` makes a
container and never starts it; `docker export` streams that container's flat
filesystem out through the daemon; we walk the tar on the host. The only other
input is `docker image inspect`, which reads daemon-side metadata. A builder
can put whatever it likes in the filesystem, but it cannot change what the
filesystem IS while we read it from outside.

Facts, then a verdict, in that order and in that separation -- the same split
`docker/manifest.py` and `scripts/container_honesty.py` use. `collect()` only
measures. `evaluate()` only judges. A script that does both can quietly stop
doing one of them.

Three-valued does NOT apply here. In `docker/manifest.py` an invariant whose
inputs are absent is skipped, because a manifest is read against images that
may never have run. Here the input is the image itself: if the scan cannot
complete we know nothing about the container, and "we know nothing" is a
rejection. Every failure path in this module ends in a fatal finding.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import tarfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import IO, Any

logger = logging.getLogger(__name__)

# A full `docker export` of an 8-15 GB task image walks ~400k tar members in
# 90-120s on an NVMe host. The cap is generous because blowing it is a
# rejection, and a rejection costs a rebuild round.
DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S", "900"))

# How much of an interesting file to read. Everything we read is a script, a
# config or a `.pth`; a genuinely large one is itself the finding.
DATASMITH_PV_IMAGE_SCAN_MAX_FILE_BYTES: int = int(os.environ.get("DATASMITH_PV_IMAGE_SCAN_MAX_FILE_BYTES", "65536"))

# Cap on how many findings of one kind we keep, so a pathological image cannot
# produce a prompt that crowds out every other fact.
DATASMITH_PV_IMAGE_SCAN_MAX_HITS: int = int(os.environ.get("DATASMITH_PV_IMAGE_SCAN_MAX_HITS", "200"))

_ELF_MAGIC = b"\x7fELF"

# ---------------------------------------------------------------- path rules
#
# Paths inside a `docker export` tar are relative and carry no leading slash.
# Everything below matches that form.

# The interpreters that actually measure. `/opt/conda` and `/workspace/repo`
# are protocol paths baked into docker/templates, not tunables.
_ENV_PREFIX = r"opt/conda/(?:envs/[^/]+/)?"

# sitecustomize and usercustomize are imported by CPython at startup in EVERY
# process, including the benchmark subprocess, which makes them the cheapest
# place to change measured behaviour without touching a measured file. Fatal
# only as a DIRECT child of a directory that is on the environment's sys.path:
# that exempts Debian's `/etc/python3.10/sitecustomize.py` (the apport handler,
# present in the pristine base) and debugpy's vendored
# `pydevd/pydev_sitecustomize/sitecustomize.py` without naming either.
# `.pyc` is included because a sourceless `sitecustomize.pyc` sitting directly
# in a site directory imports exactly as the `.py` would. It does NOT match
# `__pycache__/sitecustomize.cpython-312.pyc`, which is the ordinary bytecode
# cache of a hook already caught by its source and is not itself importable.
_HOOK_NAME = r"(?:sitecustomize|usercustomize)\.pyc?"
_STARTUP_HOOK_ON_SYS_PATH = re.compile(rf"^{_ENV_PREFIX}lib/python[0-9]+\.[0-9]+/(?:site-packages/)?{_HOOK_NAME}$")
_STARTUP_HOOK_IN_REPO = re.compile(rf"^workspace/repo/{_HOOK_NAME}$")
_STARTUP_HOOK_ANY = re.compile(rf"(?:^|/){_HOOK_NAME}$")

# `.pth` files run any line that starts with `import`. The pristine base
# already carries five of them (distutils-precedence, conda-site, an editable
# install, an nspkg shim, a coverage hook), so presence is not a finding --
# the CONTENT is shown to the verifier instead.
_PTH_FILE = re.compile(rf"^{_ENV_PREFIX}lib/python[0-9]+\.[0-9]+/site-packages/[^/]+\.pth$")

# Any `bin/python`, `bin/python3`, `bin/python3.12`. In a pristine image these
# are ELF binaries or symlinks to one. pysindy#139 replaced
# `envs/asv_3.10/bin/python` with a 317-byte bash script.
_INTERPRETER = re.compile(r"(?:^|/)bin/python[0-9.]*$")

# `python.real` beside `python` is the wrapper signature: the real interpreter
# moved aside so a script can sit in its place. Scoped to interpreter names, so
# conda's own `locale-archive.real` and `_sysconfigdata_*.py.orig` do not fire.
_INTERPRETER_SHADOW = re.compile(r"(?:^|/)bin/python[0-9.]*\.(?:real|orig|bak|old|bin|dist)$")

# Tools the harness shells out to. pysindy#139 put a bash script named `grep`
# in the environment's `bin/`, which precedes `/usr/bin` on PATH, so the secret
# scan reported clean without scanning. conda-forge ships all of these as real
# binaries, so "is a script" is unambiguous rather than a heuristic.
_SHELL_TOOLS = ("grep", "sed", "awk", "find", "cat", "head", "tail", "wc", "cut", "sort", "uniq", "tr", "diff")
_SHELL_TOOL = re.compile(r"(?:^|/)bin/(" + "|".join(_SHELL_TOOLS) + r")$")

# `asv` and `pytest` are console entry points and are LEGITIMATELY `#!` scripts
# in every healthy image, so they are recorded and never graded.
_ENTRY_POINT = re.compile(r"(?:^|/)bin/(?:asv|pytest)$")

_LD_SO_PRELOAD = "etc/ld.so.preload"
_PROFILE_D = re.compile(r"^etc/profile\.d/[^/]+$")
_FORMULACODE_DIR = re.compile(r"^opt/formulacode/[^/]+$")

# Environment variables that change what an interpreter loads before its first
# line of user code. None has a legitimate use in a task image, and none is set
# in any of the 16 validation images -- checked against the stored `env` block
# of every one, so the rule is measured rather than assumed. PYTHONHOME is here
# because it relocates the whole standard library, `site.py` included, which is
# the module that imports every other hook this file looks for.
#
# PYTHONPATH is deliberately NOT here: an editable install is a real use. It is
# recorded, and `_pythonpath_hook_paths` searches its directories for startup
# hooks instead.
_FATAL_ENV_KEYS = ("LD_PRELOAD", "PYTHONSTARTUP", "PYTHONHOME")

# One alternation rather than nine `search` calls per member. A task image
# walks ~400k members, so the per-member cost is the scan's inner loop.
_INTERESTING = re.compile(
    "|".join(
        pattern.pattern
        for pattern in (
            _STARTUP_HOOK_ANY,
            _PTH_FILE,
            _INTERPRETER,
            _INTERPRETER_SHADOW,
            _SHELL_TOOL,
            _ENTRY_POINT,
            _PROFILE_D,
            _FORMULACODE_DIR,
        )
    )
    + rf"|^{re.escape(_LD_SO_PRELOAD)}$"
)


@dataclass(frozen=True)
class IntegrityFinding:
    """One fatal fact. `check_id` is the id `severity.py` will grade."""

    check_id: str
    detail: str


@dataclass(frozen=True)
class ImageIntegrity:
    image: str
    collected: bool
    error: str = ""
    members_scanned: int = 0
    duration_s: float = 0.0
    facts: dict[str, Any] = field(default_factory=dict)
    findings: tuple[IntegrityFinding, ...] = ()

    @property
    def clean(self) -> bool:
        return self.collected and not self.findings


# ----------------------------------------------------------------- collection


def _inspect(image: str) -> dict[str, Any]:
    """Daemon-side image metadata. Runs nothing from the image."""
    proc = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"docker image inspect failed: {(proc.stderr or proc.stdout).strip()[:500]}")
    parsed = json.loads(proc.stdout)
    if not parsed:
        raise RuntimeError("docker image inspect returned no object")
    return dict(parsed[0])


def _env_map(config_env: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for entry in config_env or []:
        key, _, value = entry.partition("=")
        out[key] = value
    return out


def _normalise(name: str) -> str:
    """Tar member name as a rooted-but-slashless path."""
    return name[2:] if name.startswith("./") else name.lstrip("/")


def _read_head(tf: tarfile.TarFile, member: tarfile.TarInfo, limit: int) -> bytes:
    """First `limit` bytes of a member, or b"" when it cannot be read.

    Reading a prefix and moving on is safe in stream mode: tarfile skips the
    remainder of the member for us.
    """
    try:
        handle = tf.extractfile(member)
    # A member the stream cannot open is a fact about the image, not a crash.
    except Exception:
        return b""
    if handle is None:
        return b""
    try:
        return handle.read(limit)
    except Exception:
        return b""


def _describe(member: tarfile.TarInfo, head: bytes) -> dict[str, Any]:
    kind = "file"
    if member.issym():
        kind = "symlink"
    elif member.islnk():
        kind = "hardlink"
    elif member.isdir():
        kind = "dir"
    entry: dict[str, Any] = {
        "path": "/" + _normalise(member.name),
        "kind": kind,
        "size": member.size,
        "mode": oct(member.mode),
    }
    if member.linkname:
        entry["link_target"] = member.linkname
    if kind == "file":
        entry["is_elf"] = head[:4] == _ELF_MAGIC
        if head[:2] == b"#!":
            entry["shebang"] = head.split(b"\n", 1)[0].decode("utf-8", "replace")[:200]
    return entry


def _text(head: bytes) -> str:
    return head.decode("utf-8", "replace")


def _walk(image: str, deadline: float) -> tuple[list[tuple[tarfile.TarInfo, bytes]], int]:
    """Stream the image's flat filesystem and keep the interesting members.

    The container is created and never started. `docker export` asks the daemon
    to tar the container's filesystem; no process from the image runs.
    """
    created = subprocess.run(
        ["docker", "create", image],
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    if created.returncode != 0:
        raise RuntimeError(f"docker create failed: {(created.stderr or created.stdout).strip()[:500]}")
    container_id = created.stdout.strip()

    hits: list[tuple[tarfile.TarInfo, bytes]] = []
    scanned = 0
    proc: subprocess.Popen[bytes] | None = None
    try:
        proc = subprocess.Popen(["docker", "export", container_id], stdout=subprocess.PIPE)
        if proc.stdout is None:
            raise RuntimeError("docker export gave no stdout to read")
        hits, scanned = scan_tar_stream(proc.stdout, deadline)
    finally:
        if proc is not None:
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.poll() is None:
                proc.kill()
            proc.wait()
        subprocess.run(["docker", "rm", "-f", container_id], capture_output=True, check=False)
    return hits, scanned


def scan_tar_stream(
    fileobj: IO[bytes], deadline: float | None = None
) -> tuple[list[tuple[tarfile.TarInfo, bytes]], int]:
    """Walk a flat-filesystem tar and keep the interesting members.

    Split out of `_walk` so the policy can be tested against a tar built in
    memory rather than an 8 GB image. The docker path calls this same
    function, so a test that exercises it is testing what production runs --
    not a re-implementation that can drift from it.
    """
    hits: list[tuple[tarfile.TarInfo, bytes]] = []
    scanned = 0
    with tarfile.open(fileobj=fileobj, mode="r|*") as tf:
        for member in tf:
            scanned += 1
            # Checking the clock every member costs nothing next to the I/O
            # and bounds a scan that would otherwise run for hours against a
            # stalled daemon.
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(f"image scan exceeded {DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S}s")
            path = _normalise(member.name)
            if not _is_interesting(path):
                continue
            head = b""
            if member.isfile():
                head = _read_head(tf, member, DATASMITH_PV_IMAGE_SCAN_MAX_FILE_BYTES)
            hits.append((member, head))
    return hits, scanned


def integrity_from_tar_stream(image: str, fileobj: IO[bytes], env: dict[str, str] | None = None) -> ImageIntegrity:
    """`collect()` minus docker: scan a tar and grade it.

    The seam the tests use, and the same two steps `collect_and_evaluate`
    performs once docker has produced the tar.
    """
    started = time.monotonic()
    hits, scanned = scan_tar_stream(fileobj)
    facts = _facts_from_hits(hits, env or {})
    facts["env"] = env or {}
    return _with_findings(
        ImageIntegrity(
            image=image,
            collected=True,
            members_scanned=scanned,
            duration_s=round(time.monotonic() - started, 1),
            facts=facts,
        )
    )


def _is_interesting(path: str) -> bool:
    return _INTERESTING.search(path) is not None


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def collect(image: str, timeout_s: int = DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S) -> ImageIntegrity:
    """Read integrity facts out of `image` without running it. Never raises.

    A failure to collect is returned as `collected=False`, which `evaluate()`
    turns into a fatal finding. Nothing about a container we could not read is
    presumed good.
    """
    started = time.monotonic()
    try:
        meta = _inspect(image)
    except Exception as exc:
        return ImageIntegrity(
            image=image,
            collected=False,
            error=f"{type(exc).__name__}: {exc}"[:2000],
            duration_s=round(time.monotonic() - started, 1),
        )

    config = dict(meta.get("Config") or {})
    env = _env_map(list(config.get("Env") or []))

    try:
        hits, scanned = _walk(image, deadline=started + timeout_s)
    except Exception as exc:
        return ImageIntegrity(
            image=image,
            collected=False,
            error=f"{type(exc).__name__}: {exc}"[:2000],
            duration_s=round(time.monotonic() - started, 1),
            facts={"image_id": meta.get("Id", ""), "env": env},
        )

    facts = _facts_from_hits(hits, env)
    facts["image_id"] = meta.get("Id", "")
    facts["created"] = meta.get("Created", "")
    facts["env"] = env
    facts["entrypoint"] = config.get("Entrypoint")
    facts["cmd"] = config.get("Cmd")
    facts["working_dir"] = config.get("WorkingDir", "")
    facts["layers"] = len(list((meta.get("RootFS") or {}).get("Layers") or []))

    return ImageIntegrity(
        image=image,
        collected=True,
        members_scanned=scanned,
        duration_s=round(time.monotonic() - started, 1),
        facts=facts,
    )


def _pythonpath_hook_paths(env: dict[str, str]) -> tuple[str, ...]:
    """`sitecustomize.py` under a PYTHONPATH entry is on sys.path too.

    PYTHONPATH has legitimate uses, so the variable itself is not graded --
    but a startup hook parked in one of its directories runs in every process
    exactly like one in site-packages, and the path patterns above would miss
    it.
    """
    raw = env.get("PYTHONPATH", "")
    out: list[str] = []
    for entry in raw.split(os.pathsep):
        stripped = entry.strip().rstrip("/")
        if not stripped:
            continue
        base = _normalise(stripped)
        out.extend((f"{base}/sitecustomize.py", f"{base}/usercustomize.py"))
    return tuple(out)


def _classify(path: str) -> str | None:
    """Which fact bucket a member belongs in, or None for the singleton.

    Split out of `_facts_from_hits` so the loop there stays a dispatch rather
    than a ladder. Order matters: the shadow rule must be tried before the
    interpreter rule, because `bin/python.real` matches both.
    """
    if _PTH_FILE.match(path):
        return "pth_files"
    if _INTERPRETER_SHADOW.search(path):
        return "interpreter_shadows"
    if _INTERPRETER.search(path):
        return "interpreters"
    if _SHELL_TOOL.search(path):
        return "shell_tools"
    if _ENTRY_POINT.search(path):
        return "entry_points"
    if _PROFILE_D.match(path):
        return "profile_d"
    if _FORMULACODE_DIR.match(path):
        return "formulacode_files"
    return None


def _record_startup_hook(facts: dict[str, Any], entry: dict[str, Any], head: bytes, on_path: bool) -> None:
    entry["sha256"] = _sha256(head)
    if on_path:
        # Only a hook that actually runs gets its source carried into the
        # prompt. The base image ships several that do not (the conda package
        # cache, debugpy's vendored copy), and quoting those would crowd out
        # the one that matters.
        entry["content"] = _text(head)
        facts["startup_hooks_on_sys_path"].append(entry)
    else:
        facts["startup_hooks_elsewhere"].append(entry)


_WANT_CONTENT = ("pth_files", "profile_d")


def _facts_from_hits(hits: list[tuple[tarfile.TarInfo, bytes]], env: dict[str, str]) -> dict[str, Any]:
    pythonpath_hooks = _pythonpath_hook_paths(env)
    facts: dict[str, Any] = {
        "startup_hooks_on_sys_path": [],
        "startup_hooks_elsewhere": [],
        "pth_files": [],
        "interpreters": [],
        "interpreter_shadows": [],
        "shell_tools": [],
        "entry_points": [],
        "profile_d": [],
        "formulacode_files": [],
        "ld_so_preload": None,
    }

    for member, head in hits:
        path = _normalise(member.name)
        entry = _describe(member, head)

        if _STARTUP_HOOK_ANY.search(path):
            on_path = bool(
                _STARTUP_HOOK_ON_SYS_PATH.match(path) or _STARTUP_HOOK_IN_REPO.match(path) or path in pythonpath_hooks
            )
            _record_startup_hook(facts, entry, head, on_path)
            continue

        if path == _LD_SO_PRELOAD:
            entry["content"] = _text(head)
            facts["ld_so_preload"] = entry
            continue

        bucket = _classify(path)
        if bucket is None:
            continue
        if bucket in _WANT_CONTENT or (bucket == "shell_tools" and not entry.get("is_elf", True)):
            entry["content"] = _text(head)
        if bucket == "formulacode_files":
            entry["sha256"] = _sha256(head)
        facts[bucket].append(entry)

    for key, value in facts.items():
        if isinstance(value, list):
            facts[key] = value[:DATASMITH_PV_IMAGE_SCAN_MAX_HITS]
    return facts


# ------------------------------------------------------------------- verdict


def _resolves_to_elf(entry: dict[str, Any], by_path: dict[str, dict[str, Any]]) -> bool | None:
    """Follow links inside the tar namespace. None when the target is absent.

    `bin/python -> python3.12` is the healthy shape, so an unresolved test on
    the link itself would call every image a script.

    Symlinks and hardlinks do NOT resolve the same way, and treating them
    alike loses tampers silently. A symlink's target is relative to the link's
    own directory (`bin/python -> python3.12`) or absolute
    (`usr/lib/python3.10/sitecustomize.py -> /etc/python3.10/sitecustomize.py`).
    A hardlink's target is relative to the TAR ROOT -- `docker export` writes
    `envs/asv_3.11/.../distutils-precedence.pth` with the linkname
    `opt/conda/envs/asv_3.10/.../distutils-precedence.pth`. Joining that onto
    the member's directory yields a path that is in no lookup table, so the
    walk returns None and the check quietly concludes nothing.
    """
    seen: set[str] = set()
    current = entry
    for _ in range(8):
        path = str(current.get("path", ""))
        if path in seen:
            return None
        seen.add(path)
        if current.get("kind") == "file":
            return bool(current.get("is_elf"))
        target = str(current.get("link_target") or "")
        if not target:
            return None
        if current.get("kind") == "hardlink":
            target = "/" + _normalise(target)
        elif not target.startswith("/"):
            target = os.path.normpath(os.path.join(os.path.dirname(path), target))
        nxt = by_path.get(target)
        if nxt is None:
            return None
        current = nxt
    return None


def _pth_added_dirs(facts: dict[str, Any]) -> set[str]:
    """Directories a `.pth` file adds to sys.path.

    A `.pth` line is either code (it starts with `import`, and site runs it) or
    a PATH to append. The second kind is a door the path rules alone cannot
    see: drop `sitecustomize.py` in an arbitrary directory, add that directory
    from a `.pth`, and the hook imports at startup while the scanner files it
    under "elsewhere". This is computed in `evaluate` rather than during the
    walk because it needs every `.pth` collected first.

    The pristine base's five `.pth` files are unaffected -- four are `import`
    lines, and `conda-site.pth` appends conda's own site directories, which the
    path rules already cover.
    """
    dirs: set[str] = set()
    for entry in facts.get("pth_files") or []:
        base = os.path.dirname(str(entry.get("path", "")))
        for raw in str(entry.get("content", "")).splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(("import ", "import\t")):
                continue
            target = line if line.startswith("/") else os.path.normpath(os.path.join(base, line))
            dirs.add(target.rstrip("/") or "/")
    return dirs


def _check_startup_hooks(facts: dict[str, Any]) -> list[IntegrityFinding]:
    """A hook on sys.path runs in every process, including the measured one."""
    findings = [
        IntegrityFinding(
            "tamper_audit",
            "startup hook on the environment's sys.path -- imported before any measured code: "
            f"{hook.get('path')} ({hook.get('size')} bytes)\n{str(hook.get('content', ''))[:1500]}",
        )
        for hook in facts.get("startup_hooks_on_sys_path") or []
    ]
    added = _pth_added_dirs(facts)
    for hook in facts.get("startup_hooks_elsewhere") or []:
        if os.path.dirname(str(hook.get("path", ""))) in added:
            findings.append(
                IntegrityFinding(
                    "tamper_audit",
                    "startup hook in a directory a .pth file puts on sys.path: "
                    f"{hook.get('path')} ({hook.get('size')} bytes)",
                )
            )
    return findings


def _check_interpreter_shadows(facts: dict[str, Any]) -> list[IntegrityFinding]:
    """`python.real` beside `python`: the real interpreter was moved aside."""
    return [
        IntegrityFinding(
            "tamper_audit",
            f"the real interpreter was moved aside: {shadow.get('path')} exists beside the interpreter it shadows",
        )
        for shadow in facts.get("interpreter_shadows") or []
    ]


def _check_binaries(facts: dict[str, Any]) -> list[IntegrityFinding]:
    """Interpreters and shell tools must resolve to real binaries.

    `asv` and `pytest` are excluded by construction -- they live in
    `entry_points`, because a console entry point is a `#!` script in every
    healthy image. Only an UNRESOLVED-to-False verdict fires: a symlink whose
    target is outside the scanned set stays inconclusive rather than fatal.
    """
    by_path = {
        str(entry.get("path")): entry
        for key in ("interpreters", "shell_tools", "interpreter_shadows", "entry_points")
        for entry in (facts.get(key) or [])
    }
    findings: list[IntegrityFinding] = []
    for interpreter in facts.get("interpreters") or []:
        if _resolves_to_elf(interpreter, by_path) is False:
            findings.append(
                IntegrityFinding(
                    "tamper_audit",
                    f"interpreter is not a binary: {interpreter.get('path')} shebang={interpreter.get('shebang', '?')}",
                )
            )
    for tool in facts.get("shell_tools") or []:
        if _resolves_to_elf(tool, by_path) is False:
            findings.append(
                IntegrityFinding(
                    "tamper_audit",
                    f"shell tool replaced by a script: {tool.get('path')} "
                    f"shebang={tool.get('shebang', '?')}\n{str(tool.get('content', ''))[:800]}",
                )
            )
    return findings


def _check_preloads(facts: dict[str, Any]) -> list[IntegrityFinding]:
    """Anything that loads code before the process runs its own first line."""
    findings: list[IntegrityFinding] = []
    preload = facts.get("ld_so_preload")
    if preload:
        findings.append(
            IntegrityFinding(
                "tamper_audit",
                "/etc/ld.so.preload exists, which injects a library into every process: "
                f"{str(preload.get('content', ''))[:500]}",
            )
        )
    env = dict(facts.get("env") or {})
    for key in _FATAL_ENV_KEYS:
        if (env.get(key) or "").strip():
            findings.append(IntegrityFinding("tamper_audit", f"image config sets {key}={env[key]!r}"))
    for script in facts.get("profile_d") or []:
        content = str(script.get("content", ""))
        for key in _FATAL_ENV_KEYS:
            if re.search(rf"^\s*(?:export\s+)?{key}=", content, re.MULTILINE):
                findings.append(
                    IntegrityFinding("tamper_audit", f"{script.get('path')} sets {key} for every login shell")
                )
    return findings


# Every check is fatal. There is no warn tier here on purpose: a warn tier is
# where a tamper finding goes to be ignored.
_CHECKS = (_check_startup_hooks, _check_interpreter_shadows, _check_binaries, _check_preloads)


def evaluate(integrity: ImageIntegrity) -> tuple[IntegrityFinding, ...]:
    """Grade collected facts. Only fatal findings are returned.

    `tamper_audit` is in `severity.HARD_CHECK_IDS`; `image_scan_failed` is not
    and grades HARD through `classify`'s fail-closed default for unrecognised
    ids. Neither can be softened by a cause or a waiver, which is the point: a
    tampered container is tampered whatever the host lacks.

    A check that raises is a FAILED check, never a skipped one. The reverse
    reading -- absent input as "nothing detected" -- is what cost this project
    a corpus.
    """
    if not integrity.collected:
        return (
            IntegrityFinding(
                "image_scan_failed",
                f"integrity facts could not be read from {integrity.image}: {integrity.error}",
            ),
        )

    findings: list[IntegrityFinding] = []
    for check in _CHECKS:
        try:
            findings.extend(check(integrity.facts))
        except Exception as exc:
            logger.exception("integrity check %s raised on %s", check.__name__, integrity.image)
            findings.append(
                IntegrityFinding("image_scan_failed", f"{check.__name__} raised: {type(exc).__name__}: {exc}")
            )
    return tuple(findings)


def _with_findings(integrity: ImageIntegrity) -> ImageIntegrity:
    """Attach `evaluate`'s verdict to collected facts.

    `ImageIntegrity` is frozen, so the verdict cannot be written onto the
    facts after the fact -- which is the point. A record that can be re-graded
    after it is read is a record that can be re-graded.
    """
    return ImageIntegrity(
        image=integrity.image,
        collected=integrity.collected,
        error=integrity.error,
        members_scanned=integrity.members_scanned,
        duration_s=integrity.duration_s,
        facts=integrity.facts,
        findings=evaluate(integrity),
    )


def collect_and_evaluate(image: str, timeout_s: int = DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S) -> ImageIntegrity:
    """`collect()` with the verdict attached. This is what callers want."""
    return _with_findings(collect(image, timeout_s=timeout_s))


# ------------------------------------------------------------------ rendering


def _lines(integrity: ImageIntegrity) -> Iterator[str]:
    facts = integrity.facts
    yield "### host_image_scan"
    yield (
        "Read from the image with `docker create` + `docker export`, on the HOST. "
        "No code from the image ran to produce these facts, so unlike the "
        "in-container probe below they cannot have been faked by the build."
    )
    if not integrity.collected:
        yield f"SCAN FAILED: {integrity.error}"
        return
    yield f"members_scanned={integrity.members_scanned} duration_s={integrity.duration_s} layers={facts.get('layers')}"
    env = dict(facts.get("env") or {})
    yield "config env: " + ", ".join(f"{k}={v}" for k, v in sorted(env.items()) if k != "PATH")

    hooks = facts.get("startup_hooks_on_sys_path") or []
    yield f"startup hooks ON the environment's sys.path: {len(hooks)}"
    for entry in hooks[:10]:
        yield f"  {entry.get('path')} kind={entry.get('kind')} size={entry.get('size')}"
        content = str(entry.get("content", "")).strip()
        if content:
            yield "  ---8<---"
            for line in content.splitlines()[:40]:
                yield f"  | {line[:200]}"
            yield "  --->8---"

    elsewhere = facts.get("startup_hooks_elsewhere") or []
    yield (f"startup hooks NOT on sys.path (informational, the pristine base carries several): {len(elsewhere)}")

    shadows = facts.get("interpreter_shadows") or []
    yield f"interpreter shadows (python.real and friends): {len(shadows)}"
    for entry in shadows[:10]:
        yield f"  {entry.get('path')} kind={entry.get('kind')} -> {entry.get('link_target', '')}"

    non_elf = [
        entry
        for key in ("interpreters", "shell_tools")
        for entry in (facts.get(key) or [])
        if entry.get("kind") == "file" and not entry.get("is_elf")
    ]
    yield f"interpreters and shell tools that are NOT binaries: {len(non_elf)}"
    for entry in non_elf[:10]:
        yield f"  {entry.get('path')} shebang={entry.get('shebang', '?')}"

    yield f"/etc/ld.so.preload: {'PRESENT' if facts.get('ld_so_preload') else 'absent'}"

    pth = facts.get("pth_files") or []
    yield f".pth files on sys.path: {len(pth)}"
    for entry in pth[:20]:
        first = str(entry.get("content", "")).strip().splitlines()[:1]
        yield f"  {entry.get('path')}: {first[0][:200] if first else ''}"

    yield f"verdict: {'CLEAN' if not integrity.findings else 'TAMPERED'}"
    for finding in integrity.findings:
        yield f"  FATAL {finding.check_id}: {finding.detail.splitlines()[0][:300]}"


def facts_block(integrity: ImageIntegrity) -> str:
    """The host-scan section of the verifier's prompt."""
    return "\n".join(_lines(integrity))
