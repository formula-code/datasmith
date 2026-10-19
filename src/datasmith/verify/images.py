"""Build, check, save and record one image per rendered task.

Each built image gets a manifest row (`manifest.jsonl`, newest row per task wins); a task that fails gets a row in
`failures.jsonl`. A build is stopped when it prints nothing for `FC_BUILD_IDLE_S` seconds (default 4 h) or when the
root disk runs low; a disk stop is not a task failure. Several workers share one task list; a task is claimed by
creating `queue/claimed/<task>`.
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

GIB = 1024**3
QWEN_CODE_VERSION = "0.22.3"
VERIFY_SH = r"""
set -u
export NVM_DIR=/root/.nvm; set +u; . "$NVM_DIR/nvm.sh" >/dev/null; set -u
echo "QWEN=$(qwen --version)"
set +u; . /etc/profile.d/asv_build_vars.sh; set -u
eval "$(micromamba shell hook --shell=bash)"
micromamba run -n "$ENV_NAME" python -c "import asv_runner, coverage, jinja2, asv.contrib.lightspeed" && echo IMPORTS=ok
micromamba run -n "$ENV_NAME" snapshot-tool --help >/dev/null 2>&1 && echo SNAPSHOT_TOOL=ok
echo "HEAD=$(git -C /workspace/repo rev-parse HEAD 2>/dev/null)"
echo "LSV_INIT=$(cat /opt/lsv/cache/lsv_init_results.json 2>/dev/null | tr -d '\n')"
echo "ADDED_DEPS=$(cat /opt/lsv/cache/added_deps.json 2>/dev/null | tr -d '\n')"
"""
LOW_GRACE_S, HARD_FLOOR_GB = 180, 3.0


@dataclass
class Limits:
    min_free_gb: float = 12.0
    need_free_gb: float = 24.0
    out_min_free_gb: float = 50.0
    keep_if_free_gb: float = 60.0
    idle_s: int = int(os.environ.get("FC_BUILD_IDLE_S", "14400"))
    keep_failed: bool = False


def sh(cmd: list[str], **kw: Any) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, **kw)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


_append_lock = threading.Lock()


def append_jsonl(path: Path, row: dict) -> None:
    with _append_lock, path.open("a") as f:
        f.write(json.dumps(row, sort_keys=True) + "\n")


def rows_by_task(manifest: Path) -> dict[str, dict]:
    """A rebuild appends a row; the last row of a task wins."""
    return {r["task_id"]: r for r in read_jsonl(manifest)}


def image_exists(ref: str) -> bool:
    return sh(["docker", "image", "inspect", ref]).returncode == 0


def root_free_gb() -> float:
    return shutil.disk_usage("/").free / GIB


@functools.lru_cache(maxsize=1)
def docker_root() -> str:
    """Docker's data dir; / when the daemon does not answer."""
    try:
        out = sh(["docker", "info", "-f", "{{.DockerRootDir}}"], timeout=20).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        out = ""
    return out if out and os.path.isdir(out) else "/"


def docker_free() -> int:
    return shutil.disk_usage(docker_root()).free


def env_digest(env_dir: Path) -> str:
    """sha256 over the build context's relative paths and bytes; the image tag carries its first 12 hex digits."""
    h = hashlib.sha256()
    for p in sorted(q for q in env_dir.rglob("*") if q.is_file()):
        h.update(str(p.relative_to(env_dir)).encode() + b"\0" + p.read_bytes() + b"\0")
    return h.hexdigest()


def image_tag(task_dir: Path) -> str:
    return f"fc-task/{task_dir.name.lower()}:{env_digest(task_dir / 'environment')[:12]}"


def run_guarded(cmd: list[str], log_path: Path, limits: Limits, env: dict | None = None) -> tuple[int, str]:
    """Run cmd with its output in log_path. Kill it when the log stops growing for idle_s, when root free space stays
    below min_free_gb for LOW_GRACE_S (short spikes from other users recover), or below HARD_FLOOR_GB at once."""
    with log_path.open("a") as lf:
        lf.write(f"$ {' '.join(cmd)}\n")
        lf.flush()
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, env=env)
        reason, low_since, size, active = "", None, -1, time.monotonic()
        while proc.poll() is None:
            if log_path.stat().st_size != size:
                size, active = log_path.stat().st_size, time.monotonic()
            elif time.monotonic() - active >= limits.idle_s:
                reason = f"no output for {limits.idle_s}s; killed"
                proc.kill()
                proc.wait()
                break
            free = root_free_gb()
            low_since = (low_since or time.monotonic()) if free < limits.min_free_gb else None
            if free < HARD_FLOOR_GB or (low_since is not None and time.monotonic() - low_since >= LOW_GRACE_S):
                reason = f"root free {free:.1f}G < {limits.min_free_gb}G; killed"
                proc.kill()
                proc.wait()
                break
            time.sleep(10)
    return proc.returncode, reason


def tail(path: Path, n: int = 40) -> str:
    return "\n".join(path.read_text(errors="replace").splitlines()[-n:])


def _json_or(text: str | None, default: Any) -> Any:
    try:
        return json.loads(text or "null") or default
    except json.JSONDecodeError:
        return default


def check_image(tag: str) -> tuple[dict, list[str]]:
    """Facts about a built image and the problems that keep it from being measured."""
    r = sh(["docker", "run", "--rm", "--network", "none", "--entrypoint", "bash", tag, "-c", VERIFY_SH], timeout=600)
    out = dict(line.split("=", 1) for line in r.stdout.splitlines() if "=" in line and line.split("=", 1)[0].isupper())
    problems = []
    if out.get("QWEN", "").strip() != QWEN_CODE_VERSION:
        problems.append(f"qwen-code version {out.get('QWEN')!r} != {QWEN_CODE_VERSION}")
    if out.get("IMPORTS") != "ok":
        problems.append("import asv_runner/coverage/jinja2/asv.contrib.lightspeed failed")
    if out.get("SNAPSHOT_TOOL") != "ok":
        problems.append("snapshot-tool missing")
    lsv = _json_or(out.get("LSV_INIT"), {})
    info = {
        "head": out.get("HEAD", ""),
        "env_fingerprint": lsv.get("env_fingerprint"),
        "baseline_sha": lsv.get("baseline_sha"),
        "bake_cpus_bounded": lsv.get("bake_cpus_bounded"),
        "benchmarks_discovered": len(lsv.get("benchmarks_discovered") or []),
        "added_deps": _json_or(out.get("ADDED_DEPS"), None),
    }
    if not lsv.get("baseline_sha"):
        problems.append("no stored baseline in /opt/lsv/cache/lsv_init_results.json")
    elif lsv["baseline_sha"] != info["head"]:
        problems.append(f"baseline_sha {lsv['baseline_sha']} != HEAD {info['head']}")
    if not info["env_fingerprint"]:
        problems.append("no env_fingerprint in the stored baseline")
    if not r.stdout.strip():
        problems.append(f"verify container failed: {r.stderr.strip()[-500:]}")
    return info, problems


def save_and_check(tag: str, tar: Path) -> tuple[int, str]:
    """docker save | zstd into tar, check that it lists, and return (bytes, sha256)."""
    tmp = tar.with_suffix(tar.suffix + ".partial")
    r = sh(["bash", "-c", f"set -o pipefail; docker save {tag} | zstd -T8 -3 -q -f -o {tmp}"])
    if r.returncode != 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"docker save failed: {r.stderr[-500:]}")
    listing = sh(["bash", "-c", f"set -o pipefail; zstd -dc {tmp} | tar -tf -"])
    if listing.returncode != 0 or "manifest.json" not in listing.stdout.split():
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"saved tar failed to list: {listing.stderr[-500:]}")
    tmp.rename(tar)
    h = hashlib.sha256()
    with tar.open("rb") as f:
        while chunk := f.read(1 << 24):
            h.update(chunk)
    return tar.stat().st_size, h.hexdigest()


def build_one(task_dir: Path, out: Path, limits: Limits, preexisting: set[str]) -> tuple[dict | None, dict | None]:
    """(manifest row or None, failure row or None) for one task."""
    task_id = task_dir.name
    stamp = json.loads((task_dir / "render_stamp.json").read_text())
    base = stamp["params"]["base_image"]
    digest = env_digest(task_dir / "environment")
    tag = f"fc-task/{task_id.lower()}:{digest[:12]}"
    tar = out / f"{tag.replace('/', '_').replace(':', '__')}.tar.zst"
    blog = out / "logs" / f"{task_id}.log"
    blog.parent.mkdir(parents=True, exist_ok=True)

    def fail(stage: str, err: str, disk: bool = False) -> tuple[None, dict]:
        return None, {
            "task_id": task_id,
            "tag": tag,
            "stage": stage,
            "error": err,
            "disk": disk,
            "log": str(blog),
            "at": time.strftime("%FT%T"),
        }

    pulled_base = False
    if not image_exists(base):
        rc, why = run_guarded(["docker", "pull", base], blog, limits)
        if rc != 0:
            return fail("pull", why or tail(blog), disk=why.startswith("root free"))
        pulled_base = True
    log_offset = blog.stat().st_size if blog.exists() else 0
    t0 = time.time()
    rc, why = run_guarded(
        ["docker", "build", "--rm", "--force-rm", "--network", "host", "-t", tag, str(task_dir / "environment")],
        blog,
        limits,
        env=dict(os.environ, DOCKER_BUILDKIT="0"),
    )
    build_s = round(time.time() - t0, 1)
    try:
        if rc != 0:
            # The legacy builder keeps a failed build's last intermediate image; drop it unless it is the base.
            with blog.open() as f:
                f.seek(log_offset)
                layers = re.findall(r"^ ---> ([0-9a-f]{12})$", f.read(), re.M)
            base_id = sh(["docker", "image", "inspect", "-f", "{{.Id}}", base]).stdout.strip()
            if layers and not base_id.startswith("sha256:" + layers[-1]):
                sh(["docker", "rmi", layers[-1]])
            return fail("build", why or tail(blog), disk=why.startswith("root free"))
        info, problems = check_image(tag)
        size, sha = save_and_check(tag, tar)
        image_id = sh(["docker", "image", "inspect", "-f", "{{.Id}}", tag]).stdout.strip()
        img_size = int(sh(["docker", "image", "inspect", "-f", "{{.Size}}", tag]).stdout.strip() or 0)
        row = {
            "task_id": task_id,
            "tag": tag,
            "image_id": image_id,
            "image_bytes": img_size,
            "base_image": base,
            "tar_path": str(tar),
            "tar_bytes": size,
            "tar_sha256": sha,
            "bake_seconds": build_s,
            "verified": not problems,
            "problems": problems,
            "render_source_sha256": stamp["render_source_sha256"],
            "datasmith_commit": stamp["datasmith"]["commit"],
            "env_digest": digest,
            "built_at": time.strftime("%FT%T"),
            **info,
        }
        return row, (fail("verify", "; ".join(problems))[1] if problems else None)
    except Exception as e:
        return fail("save", str(e))
    finally:
        if image_exists(tag) and (root_free_gb() < limits.keep_if_free_gb or rc != 0 or not tar.exists()):
            sh(["docker", "rmi", tag])
        if pulled_base and base not in preexisting and image_exists(base) and root_free_gb() < limits.keep_if_free_gb:
            sh(["docker", "rmi", base])


def _say(msg: str) -> None:
    print(f"[build {time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _blocked(out: Path, limits: Limits) -> str | None:
    if (out / "STOP").exists():
        return "STOP file present"
    if root_free_gb() < limits.need_free_gb:
        return f"root free {root_free_gb():.1f}G < {limits.need_free_gb}G"
    if shutil.disk_usage(out).free / GIB < limits.out_min_free_gb:
        return f"{out} free below {limits.out_min_free_gb}G"
    return None


def _build_claimed(task_dir: Path, out: Path, limits: Limits, preexisting: set[str]) -> str:
    """Build one claimed task and record the result: `ok`, `failed` or `disk`."""
    task_id = task_dir.name
    if not (task_dir / "environment").is_dir():
        append_jsonl(
            out / "failures.jsonl",
            {"task_id": task_id, "stage": "render", "error": f"no task dir {task_dir}", "at": time.strftime("%FT%T")},
        )
        return "failed"
    _say(f"{task_id}: start")
    row, err = build_one(task_dir, out, limits, preexisting)
    if row:
        # An image that fails its check is never measured; its archive only fills the disk.
        if not row["verified"] and not limits.keep_failed:
            Path(row["tar_path"]).unlink(missing_ok=True)
        append_jsonl(out / "manifest.jsonl", row)
        _say(f"{task_id}: saved {row['tar_bytes'] / GIB:.2f}G in {row['bake_seconds']}s verified={row['verified']}")
    if err and err.get("disk"):
        _say(f"{task_id}: stopped for disk at {err['stage']}: {err['error']}")
        return "disk"
    if err:
        append_jsonl(out / "failures.jsonl", err)
        _say(f"{task_id}: FAILED at {err['stage']}: {err['error'][-300:]}")
        return "failed"
    return "ok"


def build(
    tasks_root: Path,
    task_ids: Iterable[str],
    out: Path,
    workers: int = 1,
    limits: Limits | None = None,
    retry_failed: bool = False,
) -> None:
    """Build every task not yet in the manifest (or failures, unless retry_failed), `workers` at a time.

    Stops starting new tasks when `out/STOP` exists or a disk limit is reached.
    """
    limits = limits or Limits()
    out.mkdir(parents=True, exist_ok=True)
    claimed = out / "queue" / "claimed"
    claimed.mkdir(parents=True, exist_ok=True)
    manifest, failures = out / "manifest.jsonl", out / "failures.jsonl"
    done = {r["task_id"] for r in read_jsonl(manifest)}
    if not retry_failed:
        done |= {r["task_id"] for r in read_jsonl(failures)}
    todo = [t for t in dict.fromkeys(task_ids) if t not in done]
    preexisting = set(sh(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"]).stdout.split())
    stop = threading.Event()
    _say(f"{len(todo)} task(s) to build with {workers} worker(s); root free {root_free_gb():.1f}G")

    def worker(task_id: str) -> None:
        if stop.is_set():
            return
        if why := _blocked(out, limits):
            _say(f"{why}; stopping")
            stop.set()
            return
        try:
            (claimed / task_id).mkdir()
        except FileExistsError:
            return
        if _build_claimed(tasks_root / task_id, out, limits, preexisting) == "disk":
            # Not a task failure: release the claim so a later run builds it.
            (claimed / task_id).rmdir()
            stop.set()

    with ThreadPoolExecutor(max(1, workers)) as pool:
        list(pool.map(worker, todo))


def load(row: dict, ecr_repo: str | None = None, min_free_gb: float = 12.0) -> str:
    """Make the image of a manifest row present on the daemon (from its archive, else from the registry) and check its id."""
    tag, want = row["tag"], row["image_id"]
    have = sh(["docker", "image", "inspect", "-f", "{{.Id}}", tag]).stdout.strip()
    if not have:
        if docker_free() < row.get("image_bytes", 0) + min_free_gb * GIB:
            raise RuntimeError(f"{row['task_id']}: Docker disk too full to load {tag}")
        tar = row.get("tar_path")
        if tar and Path(tar).exists():
            r = sh(["docker", "load", "-i", tar])
        elif ecr_repo:
            uri = f"{ecr_repo}:{tag.removeprefix('fc-task/').replace(':', '__')}"
            r = sh(["bash", "-c", f"docker pull -q {uri} && docker tag {uri} {tag} && docker rmi {uri}"])
        else:
            raise RuntimeError(f"{row['task_id']}: {tag} is not loaded and has no archive or registry")
        if r.returncode:
            raise RuntimeError(f"{row['task_id']}: load failed: {r.stderr.strip()[-300:]}")
        have = sh(["docker", "image", "inspect", "-f", "{{.Id}}", tag]).stdout.strip()
    if have != want:
        raise RuntimeError(f"{row['task_id']}: {tag} is {have or 'missing'}, manifest image_id is {want}")
    return tag
