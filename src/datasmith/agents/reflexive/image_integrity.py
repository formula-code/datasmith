"""Integrity facts read from the image without running it.

Anything the container runs can be lied to: the `sitecustomize.py` in
`formulacode/attack-demo:1` patches `Path.is_file` and `shutil.which`, so an
in-container probe reports a clean image. So nothing here runs image code:
`docker create` makes a container that never starts, `docker export` streams its
filesystem, and the tar is walked on the host. `docker image inspect` adds the
daemon-side metadata.

`collect()` only measures and `evaluate()` only judges. A scan that cannot
complete is a fatal finding, never a skip.
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

# Exporting an 8 to 15 GB image takes 90 to 120 s; the cap is generous because exceeding it rejects the image.
DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S: int = int(os.environ.get("DATASMITH_PV_IMAGE_SCAN_TIMEOUT_S", "900"))

# How much of an interesting file to read. Every such file is a script, a config or
# a `.pth`, so a large one is itself a finding.
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

# sitecustomize and usercustomize run at startup in every process, including the benchmark.
# Fatal only directly inside a sys.path directory, which exempts Debian's apport hook and
# debugpy's vendored copy. A sourceless `.pyc` there counts; a `__pycache__` copy does not.
_HOOK_NAME = r"(?:sitecustomize|usercustomize)\.pyc?"
_STARTUP_HOOK_ON_SYS_PATH = re.compile(rf"^{_ENV_PREFIX}lib/python[0-9]+\.[0-9]+/(?:site-packages/)?{_HOOK_NAME}$")
_STARTUP_HOOK_IN_REPO = re.compile(rf"^workspace/repo/{_HOOK_NAME}$")
_STARTUP_HOOK_ANY = re.compile(rf"(?:^|/){_HOOK_NAME}$")

# `.pth` lines that start with `import` run code, but the clean base already has five
# `.pth` files, so their content is shown to the verifier instead of graded.
_PTH_FILE = re.compile(rf"^{_ENV_PREFIX}lib/python[0-9]+\.[0-9]+/site-packages/[^/]+\.pth$")

# Any `bin/python`, `bin/python3`, `bin/python3.12`; in a clean image these are ELF
# binaries or symlinks to one.
_INTERPRETER = re.compile(r"(?:^|/)bin/python[0-9.]*$")

# `python.real` beside `python` means the real interpreter was moved aside for a wrapper.
# Scoped to interpreter names, so conda's `locale-archive.real` does not match.
_INTERPRETER_SHADOW = re.compile(r"(?:^|/)bin/python[0-9.]*\.(?:real|orig|bak|old|bin|dist)$")

# Tools the harness calls. conda-forge ships them as binaries, so a script in their place
# (for example a fake `grep` that skips the secret scan) is tampering.
_SHELL_TOOLS = ("grep", "sed", "awk", "find", "cat", "head", "tail", "wc", "cut", "sort", "uniq", "tr", "diff")
_SHELL_TOOL = re.compile(r"(?:^|/)bin/(" + "|".join(_SHELL_TOOLS) + r")$")

# `asv` and `pytest` are console entry points and are LEGITIMATELY `#!` scripts
# in every healthy image, so they are recorded and never graded.
_ENTRY_POINT = re.compile(r"(?:^|/)bin/(?:asv|pytest)$")

_LD_SO_PRELOAD = "etc/ld.so.preload"
_PROFILE_D = re.compile(r"^etc/profile\.d/[^/]+$")
_FORMULACODE_DIR = re.compile(r"^opt/formulacode/[^/]+$")

# Variables that change what an interpreter loads before user code; none is set in any
# validation image. PYTHONPATH is left out because editable installs use it, and
# `_pythonpath_hook_paths` searches its directories for startup hooks instead.
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

    Split out of `_walk` so tests run the production code on an in-memory tar.
    """
    hits: list[tuple[tarfile.TarInfo, bytes]] = []
    scanned = 0
    with tarfile.open(fileobj=fileobj, mode="r|*") as tf:
        for member in tf:
            scanned += 1
            # Checking the clock per member is cheap and bounds a scan against a stalled daemon.
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

    The variable itself is not graded, but a startup hook in one of its directories
    runs in every process.
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
        # Carry source into the prompt only for hooks that run; the base image ships some
        # that do not, and quoting them would crowd out the one that matters.
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

    A symlink target is relative to the link's directory or absolute. A hardlink
    target is relative to the tar root, as `docker export` writes it.
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

    A line that does not start with `import` is a path to append, so a `.pth` can put a
    hook in any directory on sys.path. Computed in `evaluate` because it needs every `.pth`.
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

    `asv` and `pytest` are entry-point scripts and are excluded. A symlink whose
    target was not scanned is inconclusive, not fatal.
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


# Every check is fatal. There is no warning tier, so a tamper finding cannot be ignored.
_CHECKS = (_check_startup_hooks, _check_interpreter_shadows, _check_binaries, _check_preloads)


def evaluate(integrity: ImageIntegrity) -> tuple[IntegrityFinding, ...]:
    """Grade collected facts. Only fatal findings are returned.

    `tamper_audit` is a hard check id and `image_scan_failed` grades HARD as an
    unknown id, so neither can be softened by a cause or a waiver. A check that
    raises counts as failed.
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

    `ImageIntegrity` is frozen, so the verdict cannot change after it is read.
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
