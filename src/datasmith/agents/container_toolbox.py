from __future__ import annotations

import contextlib
import json
import logging
import os
import shlex
import textwrap
from dataclasses import dataclass
from typing import Callable

import docker
from docker.errors import APIError
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
        run_labels: dict[str, str] | None = None,
        volumes: dict | None = None,
    ) -> None:
        self.client = client
        self.image = image
        self.name = name
        self.workdir = workdir
        self.env = env or {}
        self.keepalive_cmd = keepalive_cmd or _DEFAULT_KEEPALIVE_CMD
        self.container: Container | None = None
        self.run_labels = run_labels or {}
        self.volumes = volumes or {}

    def is_running(self) -> bool:
        if not self.container:
            return False
        self.container.reload()
        return self.container.status == "running"

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
                labels=self.run_labels,
                auto_remove=True,  # auto-remove on stop
                network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                volumes=self.volumes if self.volumes else None,
            )
        except APIError as e:
            if "Conflict" in str(e) and self.name:
                runid = (self.run_labels or {}).get("datasmith.run")
                logger.warning("Container name conflict, trying to remove existing container %s.", self.name)
                try:
                    old = self.client.containers.get(self.name)
                    labels = old.labels or {}
                    if labels.get("datasmith.run") == runid:
                        with contextlib.suppress(Exception):
                            old.stop(timeout=60)
                            old.remove(force=True)
                        new_name = self.name
                    else:
                        suffix = (runid or "run")[:8]
                        new_name = f"{self.name}-{suffix}"
                        logger.warning("Name conflict with foreign run; using %s instead.", new_name)
                except Exception:
                    suffix = (runid or "run")[:8]
                    new_name = f"{self.name}-{suffix}"
                self.container = self.client.containers.run(
                    self.image,
                    command=["trap : TERM INT; while :; do sleep 2147483647; done"],
                    name=new_name,
                    working_dir=self.workdir,
                    environment=self.env,
                    stdin_open=False,
                    tty=False,
                    detach=True,
                    entrypoint=["/bin/bash", "-lc"],
                    labels=self.run_labels,
                    auto_remove=True,  # auto-remove on stop
                    network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                    volumes=self.volumes if self.volumes else None,
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
            self.container.stop(timeout=30)
        finally:
            try:
                self.container.remove(force=True)
            finally:
                self.container = None

    def exec(self, cmd: str, *, timeout_s: int | None = 30) -> ExecResult:
        if not self.is_running():
            self.start()
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
        if not self.is_running():
            self.start()

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
        if not self.is_running():
            self.start()
        cmd = (
            f"cd {shlex.quote(root)} 2>/dev/null && "
            f'find . -maxdepth {int(max_depth)} -type f -print 2>/dev/null | sed "s|^\\./||" | head -n {int(max_items)}'
        )
        res = self.exec(cmd, timeout_s=20)
        return [ln for ln in res.stdout.splitlines() if ln.strip()]

    def read_file(self, path: str, *, max_bytes: int = 256_000) -> str:
        if not self.is_running():
            self.start()
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

    def write_file(self, path: str, content: str) -> None:
        """Write content to a file inside the container using put_archive."""
        import io
        import tarfile

        if not self.is_running():
            self.start()
        if not self.container:
            raise RuntimeError("container not started")

        if self.container.id is None:
            raise RuntimeError("Container ID should be set for a running container")

        # Normalize path and get directory/filename
        path = path.lstrip("/")
        dirname = "/" + os.path.dirname(path) if os.path.dirname(path) else "/"
        basename = os.path.basename(path)

        # Create in-memory tar archive with the file
        tar_stream = io.BytesIO()
        content_bytes = content.encode("utf-8")

        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            tarinfo = tarfile.TarInfo(name=basename)
            tarinfo.size = len(content_bytes)
            tarinfo.mode = 0o644
            tar.addfile(tarinfo, io.BytesIO(content_bytes))

        # Upload to container
        tar_stream.seek(0)
        self.client.api.put_archive(self.container.id, dirname, tar_stream.getvalue())

    def infer_repo_facts(self, repo_root: str) -> dict:  # noqa: C901
        """
        Extracts asv dir, pyproject/setup files, requirements/env files, package name candidates,
        and python versions from asv.conf.json (if present). Portable across BusyBox/GNU find.
        """
        if not self.is_running():
            self.start()
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

        # Collect installed packages exported by build_env (if present)
        installed_packages: dict[str, list[str]] = {}
        try:
            ls = self.exec("ls -1 /etc/asv_env 2>/dev/null | grep '^installed_packages_' || true", timeout_s=10)
            for fname in [ln.strip() for ln in ls.stdout.splitlines() if ln.strip()]:
                if not fname.startswith("installed_packages_"):
                    continue
                version = fname.split("installed_packages_", 1)[1]
                cat = self.exec(f"cat /etc/asv_env/{shlex.quote(fname)} 2>/dev/null || true", timeout_s=10)
                lines = [ln.strip() for ln in cat.stdout.splitlines() if ln.strip()]
                if version:
                    installed_packages[version] = lines
        except Exception:
            logger.warning("Error collecting installed packages", exc_info=True)
            pass

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
            "installed_packages": installed_packages,
        }

    def try_import(self, cmd_python: str, candidates: list[str]) -> dict:
        """
        Run a quick python import test inside the container.
        'cmd_python' can be 'python' or 'micromamba run -n asv_3.11 python', etc.
        """
        if not self.is_running():
            self.start()
        body = (
            textwrap.dedent("""
        import importlib, sys
        names = {names}
        for name in names:
            try:
                m = importlib.import_module(name)
                v = getattr(m, "__version__", None)
                print("IMPORTED::%s::%s" % (name, v or "unknown"))
            except Exception as e:
                print("FAILED::%s::%s" % (name, e))
                sys.exit(1)
        sys.exit(0)
        """)
            .format(names=repr(candidates))
            .strip()
        )
        res = self.exec(f'{cmd_python} - << "PY"\n{body}\nPY', timeout_s=60)
        ok = "IMPORTED::" in res.stdout
        succeeded = [line.split("::", 2)[1] for line in res.stdout.splitlines() if line.startswith("IMPORTED::")]
        stdout_snip = (res.stdout[:1000] + "..." + res.stdout[-1000:]) if len(res.stdout) > 2000 else res.stdout
        stderr_snip = (res.stderr[:1000] + "..." + res.stderr[-1000:]) if len(res.stderr) > 2000 else res.stderr
        return {
            "ok": ok,
            "tried": candidates,
            "succeeded": succeeded,
            "stdout": stdout_snip,
            "stderr": stderr_snip,
            "rc": 0 if ok else 1,
        }


# if __name__ == "__main__":
#     import docker
#     from datasmith.docker.context import Task, ContextRegistry, DockerContext
#     client = docker.from_env()
#     t = Task(owner="arviz-devs", repo="arviz", sha="3a454f7d47092764840b267896da581b90a3244a")
#     ctx = DockerContext()
#     ctx.build_container_streaming(
#         client=client,
#         image_name=t.with_tag("env").get_image_name(),
#         build_args={},
#         probe=True,
#     )
#     pc = PersistentContainer(
#         client=client,
#         image=t.with_tag("env").get_image_name(),
#         workdir="/workspace",
#     )
#     import IPython; IPython.embed()

#     # garbage collection.
#     pc.stop()
#     client.images.remove(t.with_tag("env").get_image_name(), force=True)
#     del pc
#     del ctx
