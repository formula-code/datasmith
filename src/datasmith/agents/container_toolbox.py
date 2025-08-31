from __future__ import annotations

import json
import logging
import os
import shlex
import textwrap
from dataclasses import dataclass
from typing import Callable

import docker
from docker.errors import APIError, NotFound
from docker.models.containers import Container

logger = logging.getLogger(__name__)

_DEFAULT_KEEPALIVE_CMD = "trap : TERM INT; while :; do sleep 2147483647; done"


def _bash(cmd: str, *, timeout_s: int | None = None) -> tuple[list[str], dict]:
    if timeout_s:
        # timeout wraps the shell itself
        return ["timeout", f"{int(timeout_s)}s", "/bin/bash", "-lc", cmd], {}
    else:
        return ["/bin/bash", "-lc", cmd], {}


@dataclass
class ExecResult:
    rc: int
    stdout: str
    stderr: str


class PersistentContainer:
    """
    Creates a long-lived container for interactive 'exec' operations.
    The image is assumed to already contain the target repo at the desired commit.
    """

    def __init__(
        self,
        client: docker.DockerClient,
        image: str,
        *,
        name: str | None = None,
        workdir: str | None = None,
        env: dict | None = None,
        keepalive_cmd: str | None = None,
    ) -> None:
        self.client = client
        self.image = image
        self.name = name
        self.workdir = workdir
        self.env = env or {}
        self.keepalive_cmd = keepalive_cmd or _DEFAULT_KEEPALIVE_CMD
        self.container: Container | None = None

    def start(self) -> None:
        if self.container is not None:
            return
        # Make the entrypoint /bin/bash to ensure we have a shell for exec.
        # if this command fails due to a docker.errors.APIError, then rerun the command after stopping the docker
        # container with the same name (if it exists)
        try:
            self.container = self.client.containers.run(
                self.image,
                command=["trap : TERM INT; while :; do sleep 2147483647; done"],
                name=self.name,
                working_dir=self.workdir,
                environment=self.env,
                stdin_open=False,
                tty=False,
                detach=True,
                entrypoint=["/bin/bash", "-lc"],
            )
        except APIError as e:
            if "Conflict" in str(e) and self.name:
                logger.warning("Container name conflict, trying to remove existing container %s.", self.name)
                try:
                    old_container = self.client.containers.get(self.name)
                    old_container.stop(timeout=3)
                    old_container.remove(force=True)
                except NotFound:
                    pass
                self.container = self.client.containers.run(
                    self.image,
                    command=["trap : TERM INT; while :; do sleep 2147483647; done"],
                    name=self.name,
                    working_dir=self.workdir,
                    environment=self.env,
                    stdin_open=False,
                    tty=False,
                    detach=True,
                    entrypoint=["/bin/bash", "-lc"],
                )
            else:
                raise
        if self.container is None:
            logger.warning("Failed to start container from image %s", self.image)
            return
        self.container.reload()
        if self.container.status != "running":
            logs = self.container.logs(tail=50).decode("utf-8", "replace")
            raise RuntimeError(f"Container failed to stay up. Status={self.container.status}\n{logs}")

    def stop(self) -> None:
        if not self.container:
            return
        try:
            self.container.stop(timeout=3)
        finally:
            try:
                self.container.remove(force=True)
            finally:
                self.container = None

    def exec(self, cmd: str, *, timeout_s: int | None = 30) -> ExecResult:
        if not self.container:
            raise RuntimeError("container not started")
        args, _ = _bash(cmd, timeout_s=timeout_s)
        exec_id = self.client.api.exec_create(self.container.id, args, stdout=True, stderr=True)
        out = self.client.api.exec_start(exec_id, stream=False, demux=True)
        if isinstance(out, tuple) and len(out) == 2:
            stdout_bytes, stderr_bytes = out
        else:
            # Fallback for engines that don't demux
            stdout_bytes, stderr_bytes = out, b""
        insp = self.client.api.exec_inspect(exec_id)
        rc = insp.get("ExitCode", 1)
        return ExecResult(
            rc=rc,
            stdout=(stdout_bytes or b"").decode("utf-8", errors="replace"),  # pyright: ignore[reportAttributeAccessIssue]
            stderr=(stderr_bytes or b"").decode("utf-8", errors="replace"),
        )

    # --- Higher-level helpers ---

    def find_repo_root(self) -> str | None:
        """
        Heuristics to locate the repo root inside the container.
        Tries git, then common roots, then a bounded 'find'.
        """
        # 1) git (fast if .git present)
        res = self.exec("git rev-parse --show-toplevel || true")
        if res.stdout.strip():
            return res.stdout.strip()

        # 2) check common mount points / conventional roots
        candidates = ["/workspace", "/work", "/repo", "/project", "/src", "/opt/src", "/home"]
        script = " || ".join([f"[ -e {shlex.quote(p)}/pyproject.toml ] && echo {shlex.quote(p)}" for p in candidates])
        res = self.exec(f"({script}) || true")
        if res.stdout.strip():
            return res.stdout.strip()

        # 3) bounded search for pyproject/asv files to infer root
        res = self.exec(
            textwrap.dedent("""
            set -euo pipefail
            root=""
            for base in /workspace /work /repo /project /src /opt/src /home /; do
              p=$(find "$base" -maxdepth 5 -type f \\( -name pyproject.toml -o -name asv.conf.json \\) 2>/dev/null | head -n1 || true)
              if [ -n "$p" ]; then
                root=$(dirname "$p"); echo "$root"; exit 0
              fi
            done
            true
            """).strip(),
            timeout_s=25,
        )
        return res.stdout.strip() or None

    def list_tree(self, root: str, *, max_depth: int = 3, max_items: int = 500) -> list[str]:
        cmd = (
            f"cd {shlex.quote(root)} 2>/dev/null && "
            f'find . -maxdepth {int(max_depth)} -type f -print 2>/dev/null | sed "s|^\\./||" | head -n {int(max_items)}'
        )
        res = self.exec(cmd, timeout_s=20)
        return [ln for ln in res.stdout.splitlines() if ln.strip()]

    def read_file(self, path: str, *, max_bytes: int = 256_000) -> str:
        # use Python for robust UTF-8 handling across environments
        py = textwrap.dedent(f"""
        import sys, os, io
        p = {path!r}
        try:
            with open(p, 'rb') as f:
                data = f.read({int(max_bytes)})
            sys.stdout.write(data.decode('utf-8', 'replace'))
        except Exception as e:
            sys.stdout.write("")
            sys.stderr.write(str(e))
        """).strip()
        res = self.exec(f'python - << "PY"\n{py}\nPY', timeout_s=20)
        return res.stdout

    def infer_repo_facts(self, repo_root: str) -> dict:  # noqa: C901
        """
        Extracts asv dir, pyproject/setup files, requirements/env files, package name candidates,
        and python versions from asv.conf.json (if present). Portable across BusyBox/GNU find.
        """
        scan_cmd = textwrap.dedent(f"""
            set -e
            base={shlex.quote(repo_root)}
            [ -d "$base" ] || exit 0
            (
            cd "$base"
            # print relative paths; strip leading ./ so Python sees clean relatives
            find . -maxdepth 6 -type f \\( \
                -name 'asv.conf.json' -o -name 'pyproject.toml' -o -name 'setup.cfg' -o -name 'setup.py' -o \
                -name 'asv.*.json' -o -name 'requirements.txt' -o -name 'requirements-*.txt' -o \
                -name 'environment.yml' -o -name 'environment.yaml' \
            \\) -print | sed 's|^\\./||'
            ) | sort -u
        """).strip()

        scan = self.exec(scan_cmd, timeout_s=25)
        files = [ln for ln in scan.stdout.splitlines() if ln.strip()]

        def _first(pred: Callable[[str], bool]) -> str | None:
            for f in files:
                if pred(f):
                    return f
            return None

        asv_conf = _first(lambda p: os.path.basename(p) == "asv.conf.json")
        pyproject = _first(lambda p: os.path.basename(p) == "pyproject.toml")
        setup_cfg = _first(lambda p: os.path.basename(p) == "setup.cfg")
        setup_py = _first(lambda p: os.path.basename(p) == "setup.py")
        asv_json_candidates = [
            p for p in files if p != asv_conf and os.path.basename(p).startswith("asv.") and p.endswith(".json")
        ]
        requirements = [p for p in files if os.path.basename(p).startswith("requirements") and p.endswith(".txt")]
        env_files = [p for p in files if os.path.basename(p) in ("environment.yml", "environment.yaml")]

        # Parse names/versions using python in-container (unchanged except for robustness).
        py = textwrap.dedent(f"""
            import json, os
            root = {repo_root!r}
            rel_asv_conf = {asv_conf!r}
            rel_pyproject = {pyproject!r}
            project_name_from_pyproject = None
            project_name_from_asv = None
            python_versions_from_asv = []
            # parse asv.conf.json
            if rel_asv_conf:
                try:
                    with open(os.path.join(root, rel_asv_conf), 'rb') as f:
                        j = json.load(f)
                    project_name_from_asv = j.get('project')
                    pyv = j.get('pythons') or j.get('matrix', {{}}).get('pythons') or []
                    python_versions_from_asv = [str(v) for v in pyv]
                except Exception:
                    pass
            # parse pyproject name (PEP 621 / Poetry)
            if rel_pyproject:
                try:
                    import tomllib  # 3.11+
                except Exception:
                    tomllib = None
                try:
                    p = os.path.join(root, rel_pyproject)
                    data = open(p, 'rb').read()
                    if tomllib:
                        t = tomllib.loads(data.decode('utf-8', 'replace'))
                        project_name_from_pyproject = (
                            t.get('project', {{}}).get('name')
                            or t.get('tool', {{}}).get('poetry', {{}}).get('name')
                        )
                    else:
                        import re
                        m = re.search(r'^\\s*name\\s*=\\s*["\\\']([^"\\\']+)["\\\']', data.decode('utf-8','replace'), re.M)
                        if m:
                            project_name_from_pyproject = m.group(1)
                except Exception:
                    pass
            print(json.dumps(dict(
                project_name_from_pyproject=project_name_from_pyproject,
                project_name_from_asv=project_name_from_asv,
                python_versions_from_asv=python_versions_from_asv,
            )))
        """).strip()
        parsed = self.exec(f'python - << "PY"\n{py}\nPY', timeout_s=25)
        try:
            meta = json.loads(parsed.stdout or "{}")
        except Exception:
            meta = {}

        # asv_dir should be the directory containing asv.conf.json; if top-level, use repo_root
        if asv_conf:
            asv_dir_rel = os.path.dirname(asv_conf)
            asv_dir = repo_root if asv_dir_rel in ("", ".") else f"{repo_root}/{asv_dir_rel}"
        else:
            asv_dir = None

        repo_tail = repo_root.strip("/").split("/")[-1] or "repo"
        cands = []
        for src in (meta.get("project_name_from_pyproject"), meta.get("project_name_from_asv"), repo_tail):
            if src and src not in cands:
                cands.append(src)
            if src:
                for v in (src.replace("-", "_"), src.replace("_", "-")):
                    if v not in cands:
                        cands.append(v)

        return {
            "repo_root": repo_root,
            "asv_dir": asv_dir,
            "asv_conf": asv_conf,
            "asv_json_candidates": asv_json_candidates[:16],
            "pyproject": pyproject,
            "setup_cfg": setup_cfg,
            "setup_py": setup_py,
            "requirements": requirements[:16],
            "env_files": env_files[:8],
            "project_name_from_pyproject": meta.get("project_name_from_pyproject"),
            "project_name_from_asv": meta.get("project_name_from_asv"),
            "python_versions_from_asv": meta.get("python_versions_from_asv") or [],
            "pkg_candidates": cands[:8],
        }

    def try_import(self, cmd_python: str, candidates: list[str]) -> dict:
        """
        Run a quick python import test inside the container.
        'cmd_python' can be 'python' or 'micromamba run -n asv_3.11 python', etc.
        """
        body = (
            textwrap.dedent("""
        import importlib, sys
        names = {names}
        for name in names:
            try:
                m = importlib.import_module(name)
                v = getattr(m, "__version__", None)
                print("IMPORTED::%s::%s" % (name, v or "unknown"))
                sys.exit(0)
            except Exception as e:
                print("FAILED::%s::%s" % (name, e))
        sys.exit(1)
        """)
            .format(names=repr(candidates))
            .strip()
        )
        res = self.exec(f'{cmd_python} - << "PY"\n{body}\nPY', timeout_s=60)
        ok = "IMPORTED::" in res.stdout
        succeeded = None
        for line in res.stdout.splitlines():
            if line.startswith("IMPORTED::"):
                succeeded = line.split("::", 2)[1]
                break
        return {
            "ok": ok,
            "tried": candidates,
            "succeeded": succeeded,
            "stdout": res.stdout[-2000:],
            "stderr": res.stderr[-2000:],
            "rc": 0 if ok else 1,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    client = docker.from_env()
    img_name = "asvprobe/textualize/rich/1de94713811101702b8fcf283c64d1a5de5a8213"
    pc = PersistentContainer(
        client, img_name, name=img_name.replace("/", "-").replace(":", "-"), workdir="/workspace/repo"
    )
    import IPython

    IPython.embed()
    pc.stop()
