"""Rebuild a task's environment image (formulacode/<owner>-<repo>:<N>) at its base commit.

For tasks whose Docker Hub image was built at the merge commit. The build scripts come from the task database
(candidate_containers), or datasmith's default scripts when none are stored. With a registry, the result is pushed as
`<repo>:<owner>__<repo>__<N>__env` so a build host can pull it and tag it back before `fc-verify build`.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any


def db_rows(client: Any, owner: str, repo: str, num: int) -> dict:
    pr = (
        client.table("pull_requests")
        .select("merge_commit_sha,base_sha")
        .eq("owner", owner)
        .eq("repo", repo)
        .eq("issue_number", num)
        .execute()
        .data[0]
    )
    sha = pr["merge_commit_sha"]
    cc = (
        client.table("candidate_containers")
        .select("*")
        .eq("owner", owner)
        .eq("repo", repo)
        .eq("sha", sha)
        .execute()
        .data
    )
    pkg = (
        client.table("packages")
        .select("env_payload,python_version,primary_root")
        .eq("owner", owner)
        .eq("repo", repo)
        .eq("sha", sha)
        .execute()
        .data
    )
    return {"pr": pr, "cc": cc[0] if cc else {}, "pkg": pkg[0] if pkg else {}}


def build(client: Any, owner: str, repo: str, num: int, registry_uri: str | None = None) -> dict:
    """Build the env image at the base commit; returns its tag, vcs.ref and whether it is at the base commit."""
    os.environ.setdefault("DATASMITH_SKIP_IMAGE_PUSH", "1")
    from datasmith.docker.context import DockerContext
    from datasmith.runners.synthesize_images import _build_pr_image, _ensure_prerequisite_images

    r = db_rows(client, owner, repo, num)
    py = r["pkg"].get("python_version") or r["cc"].get("python_version") or ""
    root = r["pkg"].get("primary_root") or "."
    payload = r["pkg"].get("env_payload") or r["cc"].get("env_payload") or "[]"
    fields = set(DockerContext.model_fields)
    ctx = (
        DockerContext(**{k: v for k, v in r["cc"].items() if k in fields and v is not None})
        if r["cc"]
        else DockerContext()
    )
    _ensure_prerequisite_images(owner, repo, py, root)
    tag = _build_pr_image(
        owner, repo, num, r["pr"]["merge_commit_sha"], payload, ctx, py, base_sha=r["pr"]["base_sha"], build_root=root
    )
    ref = subprocess.run(
        ["docker", "image", "inspect", "-f", '{{index .Config.Labels "vcs.ref"}}', tag], capture_output=True, text=True
    ).stdout.strip()
    out = {"tag": tag, "vcs_ref": ref, "base": r["pr"]["base_sha"], "at_base": ref.startswith(r["pr"]["base_sha"][:12])}
    if registry_uri and out["at_base"]:
        dest = f"{registry_uri}:{owner.lower()}__{repo.lower()}__{num}__env"
        subprocess.run(["docker", "tag", tag, dest], check=True)
        subprocess.run(["docker", "push", "-q", dest], check=True)
        out["pushed"] = dest
    return out
