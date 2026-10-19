"""Publish built images: pin, remove git history, check for leaks, push to a registry, and record each step.

`published.jsonl` holds the -scrub rows (no archive); `pushed.jsonl` holds the registry records. A rerun skips
tasks already published.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from datasmith.verify import images, pin, scrub
from datasmith.verify.images import append_jsonl, read_jsonl, sh

LOGIN_EVERY_S = 6 * 3600


class Registry(Protocol):
    def push(self, task_id: str, tag: str) -> dict: ...


class EcrRegistry:
    """One ECR repository; an image `fc-task/<task>:<id>` is pushed as `<repo>:<task>__<id>`."""

    def __init__(self, region: str = "us-east-1", repo: str = "formulacode") -> None:
        self.region, self.repo = region, repo
        account = sh(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"], check=True)
        self.registry = f"{account.stdout.strip()}.dkr.ecr.{region}.amazonaws.com"
        self.uri = f"{self.registry}/{repo}"
        self.logged_in = 0.0

    def login(self) -> None:
        if time.time() - self.logged_in < LOGIN_EVERY_S:
            return
        password = sh(["aws", "ecr", "get-login-password", "--region", self.region], check=True).stdout
        sh(["docker", "login", "--username", "AWS", "--password-stdin", self.registry], input=password, check=True)
        self.logged_in = time.time()

    def ensure_repo(self) -> None:
        have = sh(["aws", "ecr", "describe-repositories", "--region", self.region, "--repository-names", self.repo])
        if have.returncode:
            sh(
                [
                    "aws",
                    "ecr",
                    "create-repository",
                    "--region",
                    self.region,
                    "--repository-name",
                    self.repo,
                    "--tags",
                    "Key=project,Value=formulacode",
                ],
                check=True,
            )

    def push(self, task_id: str, tag: str) -> dict:
        name, ident = tag.split(":", 1)
        ecr_tag = f"{name.split('/')[-1]}__{ident}"
        uri = f"{self.uri}:{ecr_tag}"
        self.login()
        self.ensure_repo()
        try:
            sh(["docker", "tag", tag, uri], check=True)
            sh(["docker", "push", "--quiet", uri], check=True)
            digest = sh(
                [
                    "aws",
                    "ecr",
                    "describe-images",
                    "--region",
                    self.region,
                    "--repository-name",
                    self.repo,
                    "--image-ids",
                    f"imageTag={ecr_tag}",
                    "--query",
                    "imageDetails[0].imageDigest",
                    "--output",
                    "text",
                ],
                check=True,
            ).stdout.strip()
        finally:
            sh(["docker", "rmi", uri])
        return {
            "task_id": task_id,
            "tag": tag,
            "uri": uri,
            "digest": digest,
            "pushed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }


def gt_hash(task_dir: Path) -> str:
    cfg = task_dir / "tests" / "config.json"
    return json.loads(cfg.read_text()).get("gt_hash", "") if cfg.exists() else ""


def publish_one(task_dir: Path, row: dict, registry: Registry, out: Path) -> str:
    """Pin the built image, load it, scrub it, probe the result, push it; returns the pushed tag."""
    task = task_dir.name
    status, tag = pin.plan(task_dir, row)
    if status not in ("ok", "unpinned", "drift"):
        raise RuntimeError(f"{task}: cannot pin ({status})")
    if tag:
        pin.write_pin(task_dir, tag)
    images.load(row)
    new, res = scrub.scrub(row["tag"])
    if not new:
        raise RuntimeError(f"{task}: {res.get('error')}")
    check = scrub.probe(task, new, gt_hash(task_dir))
    if check.get("status") != "clean":
        sh(["docker", "rmi", new])
        raise RuntimeError(f"{task}: scrubbed image still leaks: {check.get('leaks') or check.get('error')}")
    info = json.loads(sh(["docker", "image", "inspect", new]).stdout)[0]
    scrubbed = {
        **row,
        "tag": new,
        "image_id": info["Id"],
        "image_bytes": info["Size"],
        "scrubbed_from": row["tag"],
        "scrubbed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tar_path": None,
        "scrub_check": {
            **{k: check.get(k) for k in ("unreachable_newer", "refs_ahead", "pr_commits", "remotes")},
            "nested_git_removed": res["nested_git_removed"],
            "nested_tree_same": res["nested_tree_same"],
        },
    }
    for k in ("tar_bytes", "tar_sha256"):
        scrubbed.pop(k, None)
    append_jsonl(out / "pushed.jsonl", registry.push(task, new))
    pin.write_pin(task_dir, new)
    append_jsonl(out / "published.jsonl", scrubbed)
    for t in (new, row["tag"]):
        sh(["docker", "rmi", t])
    return new


def publish(
    tasks_root: Path,
    manifest: Path,
    out: Path,
    registry: Registry,
    until_idle: bool = False,
    building: Callable[[], bool] = lambda: False,
) -> None:
    """Publish every verified, unpublished manifest row; with until_idle, keep going until `building()` is False."""
    out.mkdir(parents=True, exist_ok=True)
    tried: set[str] = set()
    while True:
        done = {r["task_id"] for r in read_jsonl(out / "published.jsonl")}
        todo = [t for t, r in images.rows_by_task(manifest).items() if r.get("verified") and t not in done | tried]
        for task in todo:
            tried.add(task)
            start = time.time()
            try:
                result = publish_one(tasks_root / task, images.rows_by_task(manifest)[task], registry, out)
            except Exception as error:
                result = f"error: {type(error).__name__}: {error}"
            print(f"[{time.strftime('%H:%M:%S')}] {task}: {result} ({time.time() - start:.0f} s)", flush=True)
        if not until_idle or (not building() and not todo):
            return
        time.sleep(60)
