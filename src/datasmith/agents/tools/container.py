"""Container-oriented tools for agent interactions."""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import textwrap
from dataclasses import dataclass
from typing import Any, Callable, cast

import docker
from docker.errors import APIError
from docker.models.containers import Container

from datasmith.agents.base.tool_interface import Tool, ToolExecutionError, ToolRegistry
from datasmith.logging_config import get_logger

logger = get_logger("agents.tools.container")

_DEFAULT_KEEPALIVE_CMD = "trap : TERM INT; while :; do sleep 2147483647; done"


def _bash(cmd: str, *, timeout_s: int | None = None) -> tuple[list[str], dict]:
    """Return a docker exec argv for running ``cmd`` inside bash."""

    if timeout_s:
        return ["timeout", f"{int(timeout_s)}s", "/bin/bash", "-lc", cmd], {}
    return ["/bin/bash", "-lc", cmd], {}


@dataclass
class ExecResult:
    """Result of executing a command inside the container."""

    rc: int
    stdout: str
    stderr: str


class PersistentContainer:
    """Maintain an interactive container for repeated exec operations."""

    def __init__(
        self,
        client: docker.DockerClient,
        image: str,
        *,
        name: str | None = None,
        workdir: str | None = None,
        env: dict[str, str] | None = None,
        keepalive_cmd: str | None = None,
        run_labels: dict[str, str] | None = None,
    ) -> None:
        self.client = client
        self.image = image
        self.name = name
        self.workdir = workdir
        self.env = env or {}
        self.keepalive_cmd = keepalive_cmd or _DEFAULT_KEEPALIVE_CMD
        self.container: Container | None = None
        self.run_labels = run_labels or {}

    def is_running(self) -> bool:
        """Return ``True`` when the backing container is alive."""

        if not self.container:
            return False
        self.container.reload()
        return self.container.status == "running"

    def start(self) -> None:
        """Start the container if it is not already running."""

        if self.container is not None:
            return
        try:
            self.container = self.client.containers.run(
                self.image,
                command=[self.keepalive_cmd],
                name=self.name,
                working_dir=self.workdir,
                environment=self.env,
                stdin_open=False,
                tty=False,
                detach=True,
                entrypoint=["/bin/bash", "-lc"],
                labels=self.run_labels,
                auto_remove=True,
                network_mode=os.environ.get("DOCKER_NETWORK_MODE"),
            )
        except APIError as err:
            if "Conflict" not in str(err) or not self.name:
                raise
            run_id = (self.run_labels or {}).get("datasmith.run")
            logger.warning("Container name conflict; attempting replacement", exc_info=False)
            try:
                old = self.client.containers.get(self.name)
                labels = old.labels or {}
                if labels.get("datasmith.run") == run_id:
                    with contextlib.suppress(Exception):
                        old.stop(timeout=60)
                        old.remove(force=True)
                    new_name = self.name
                else:
                    suffix = (run_id or "run")[:8]
                    new_name = f"{self.name}-{suffix}"
                    logger.warning("Name conflict with foreign run; using %s", new_name)
            except Exception:
                suffix = (run_id or "run")[:8]
                new_name = f"{self.name}-{suffix}"
            self.container = self.client.containers.run(
                self.image,
                command=[self.keepalive_cmd],
                name=new_name,
                working_dir=self.workdir,
                environment=self.env,
                stdin_open=False,
                tty=False,
                detach=True,
                entrypoint=["/bin/bash", "-lc"],
                labels=self.run_labels,
                auto_remove=True,
                network_mode=os.environ.get("DOCKER_NETWORK_MODE"),
            )
        if self.container is None:
            logger.warning("Failed to start container from image %s", self.image)
            return
        self.container.reload()
        if self.container.status != "running":
            logs = self.container.logs(tail=50).decode("utf-8", "replace")
            raise RuntimeError(f"Container failed to stay up. Status={self.container.status}\n{logs}")

    def stop(self) -> None:
        """Stop and remove the backing container."""

        if not self.container:
            return
        try:
            self.container.stop(timeout=30)
        finally:
            with contextlib.suppress(Exception):
                self.container.remove(force=True)
            self.container = None

    def exec(self, cmd: str, *, timeout_s: int | None = 30) -> ExecResult:
        """Execute ``cmd`` in the container and capture output."""

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
            stdout_bytes, stderr_bytes = out, b""
        inspection = self.client.api.exec_inspect(exec_id)
        rc = inspection.get("ExitCode", 1)
        return ExecResult(
            rc=rc,
            stdout=(stdout_bytes or b"").decode("utf-8", errors="replace"),
            stderr=(stderr_bytes or b"").decode("utf-8", errors="replace"),
        )

    def find_repo_root(self) -> str | None:
        """Attempt to discover the repository root within the container."""

        if not self.is_running():
            self.start()

        res = self.exec("git rev-parse --show-toplevel || true")
        if res.stdout.strip():
            return res.stdout.strip()

        candidates = ["/workspace", "/work", "/repo", "/project", "/src", "/opt/src", "/home"]
        script = " || ".join([f"[ -e {shlex.quote(p)}/pyproject.toml ] && echo {shlex.quote(p)}" for p in candidates])
        res = self.exec(f"({script}) || true")
        if res.stdout.strip():
            return res.stdout.strip()

        res = self.exec(
            textwrap.dedent(
                """
                set -euo pipefail
                root=""
                for base in /workspace /work /repo /project /src /opt/src /home /; do
                  p=$(find "$base" -maxdepth 5 -type f \\( -name pyproject.toml -o -name asv.conf.json \\) 2>/dev/null | head -n1 || true)
                  if [ -n "$p" ]; then
                    root=$(dirname "$p"); echo "$root"; exit 0
                  fi
                done
                true
                """
            ).strip(),
            timeout_s=25,
        )
        return res.stdout.strip() or None

    def list_tree(self, root: str, *, max_depth: int = 3, max_items: int = 500) -> list[str]:
        """Return a trimmed list of files under ``root``."""

        if not self.is_running():
            self.start()
        cmd = (
            f"cd {shlex.quote(root)} 2>/dev/null && "
            f'find . -maxdepth {int(max_depth)} -type f -print 2>/dev/null | sed "s|^\\./||" | head -n {int(max_items)}'
        )
        res = self.exec(cmd, timeout_s=20)
        return [ln for ln in res.stdout.splitlines() if ln.strip()]

    def read_file(self, path: str, *, max_bytes: int = 256_000) -> str:
        """Read ``path`` within the container and return text contents."""

        if not self.is_running():
            self.start()
        py = textwrap.dedent(
            f"""
            import sys, os, io
            p = {path!r}
            try:
                with open(p, 'rb') as f:
                    data = f.read({int(max_bytes)})
                sys.stdout.write(data.decode('utf-8', 'replace'))
            except Exception as exc:
                sys.stdout.write("")
                sys.stderr.write(str(exc))
            """
        ).strip()
        res = self.exec(f'python - << "PY"\n{py}\nPY', timeout_s=20)
        return res.stdout

    def infer_repo_facts(self, repo_root: str) -> dict:  # noqa: C901
        """Return metadata about the repository layout and python artefacts."""

        if not self.is_running():
            self.start()
        scan_cmd = textwrap.dedent(
            f"""
            set -e
            base={shlex.quote(repo_root)}
            [ -d "$base" ] || exit 0
            (
            cd "$base"
            find . -maxdepth 6 -type f \\( \
                -name 'asv.conf.json' -o -name 'pyproject.toml' -o -name 'setup.cfg' -o -name 'setup.py' -o \
                -name 'asv.*.json' -o -name 'requirements.txt' -o -name 'requirements-*.txt' -o \
                -name 'environment.yml' -o -name 'environment.yaml' \
            \\) -print | sed 's|^\\./||'
            ) | sort -u
            """
        ).strip()

        scan = self.exec(scan_cmd, timeout_s=25)
        files = [ln for ln in scan.stdout.splitlines() if ln.strip()]

        def _first(pred: Callable[[str], bool]) -> str | None:
            for candidate in files:
                if pred(candidate):
                    return candidate
            return None

        asv_conf = _first(lambda p: p.endswith("asv.conf.json"))
        pyproject = _first(lambda p: p.endswith("pyproject.toml"))
        setup_cfg = _first(lambda p: p.endswith("setup.cfg"))
        setup_py = _first(lambda p: p.endswith("setup.py"))
        asv_json_candidates = [p for p in files if p != asv_conf and p.endswith(".json") and p.startswith("asv.")]
        requirements = [p for p in files if p.startswith("requirements") and p.endswith(".txt")]
        env_files = [p for p in files if p.split("/")[-1] in ("environment.yml", "environment.yaml")]

        py = textwrap.dedent(
            f"""
            import json, os
            root = {repo_root!r}
            rel_asv_conf = {asv_conf!r}
            rel_pyproject = {pyproject!r}
            project_name_from_pyproject = None
            project_name_from_asv = None
            python_versions_from_asv = []
            if rel_asv_conf:
                try:
                    with open(os.path.join(root, rel_asv_conf), 'rb') as f:
                        j = json.load(f)
                    project_name_from_asv = j.get('project')
                    pyv = j.get('pythons') or j.get('matrix', {{}}).get('pythons') or []
                    python_versions_from_asv = [str(v) for v in pyv]
                except Exception:
                    pass
            if rel_pyproject:
                try:
                    import tomllib
                except Exception:
                    tomllib = None
                try:
                    data = open(os.path.join(root, rel_pyproject), 'rb').read()
                    if tomllib:
                        t = tomllib.loads(data.decode('utf-8', 'replace'))
                        project_name_from_pyproject = (
                            t.get('project', {{}}).get('name')
                            or t.get('tool', {{}}).get('poetry', {{}}).get('name')
                        )
                    else:
                        import re
                        m = re.search(r'^\\s*name\\s*=\\s*["\\\']([^"\\\']+)["\\\']', data.decode('utf-8', 'replace'), re.M)
                        if m:
                            project_name_from_pyproject = m.group(1)
                except Exception:
                    pass
            print(json.dumps(dict(
                project_name_from_pyproject=project_name_from_pyproject,
                project_name_from_asv=project_name_from_asv,
                python_versions_from_asv=python_versions_from_asv,
            )))
            """
        ).strip()
        parsed = self.exec(f'python - << "PY"\n{py}\nPY', timeout_s=25)
        try:
            meta = json.loads(parsed.stdout or "{}")
        except Exception:
            meta = {}

        if asv_conf:
            asv_dir_rel = asv_conf.rsplit("/", 1)[0] if "/" in asv_conf else ""
            asv_dir = repo_root if asv_dir_rel in ("", ".") else f"{repo_root}/{asv_dir_rel}"
        else:
            asv_dir = None

        repo_tail = repo_root.strip("/").split("/")[-1] or "repo"
        candidates = []
        for src in (
            meta.get("project_name_from_pyproject"),
            meta.get("project_name_from_asv"),
            repo_tail,
        ):
            if src and src not in candidates:
                candidates.append(src)
            if src:
                for alt in (src.replace("-", "_"), src.replace("_", "-")):
                    if alt not in candidates:
                        candidates.append(alt)

        installed_packages: dict[str, list[str]] = {}
        try:
            listing = self.exec("ls -1 /etc/asv_env 2>/dev/null | grep '^installed_packages_' || true", timeout_s=10)
            for fname in [ln.strip() for ln in listing.stdout.splitlines() if ln.strip()]:
                if not fname.startswith("installed_packages_"):
                    continue
                version = fname.split("installed_packages_", 1)[1]
                cat = self.exec(f"cat /etc/asv_env/{shlex.quote(fname)} 2>/dev/null || true", timeout_s=10)
                lines = [ln.strip() for ln in cat.stdout.splitlines() if ln.strip()]
                if version:
                    installed_packages[version] = lines
        except Exception:
            logger.warning("Error collecting installed packages", exc_info=True)

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
            "pkg_candidates": candidates[:8],
            "installed_packages": installed_packages,
        }

    def try_import(self, cmd_python: str, candidates: list[str]) -> dict:
        """Attempt to import ``candidates`` using ``cmd_python``."""

        if not self.is_running():
            self.start()
        body = (
            textwrap.dedent(
                """
                import importlib, sys
                names = {names}
                for name in names:
                    try:
                        module = importlib.import_module(name)
                        version = getattr(module, "__version__", None)
                        print("IMPORTED::%s::%s" % (name, version or "unknown"))
                    except Exception as exc:
                        print("FAILED::%s::%s" % (name, exc))
                        sys.exit(1)
                sys.exit(0)
                """
            )
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


class ProbeRepoTool(Tool):
    """Recompute repository facts inside the running container."""

    def __init__(self, executor: ContainerToolExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:  # pragma: no cover - trivial
        return "probe_repo"

    @property
    def description(self) -> str:  # pragma: no cover - trivial
        return "Recompute repository metadata for the active container."

    def execute(self, *, action_input: str = "", **kwargs: Any) -> str:
        return self._executor.exec_probe_repo()


class ListTreeTool(Tool):
    """List files under the detected repository root."""

    def __init__(self, executor: ContainerToolExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:  # pragma: no cover - trivial
        return "list_tree"

    @property
    def description(self) -> str:  # pragma: no cover - trivial
        return "List a trimmed tree of files for orientation."

    def execute(self, *, action_input: str = "", **kwargs: Any) -> str:
        return self._executor.exec_list_tree(action_input)


class ReadFileTool(Tool):
    """Read files from the repository."""

    def __init__(self, executor: ContainerToolExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:  # pragma: no cover - trivial
        return "read_file"

    @property
    def description(self) -> str:  # pragma: no cover - trivial
        return "Read a file from the repository with a size guard."

    def execute(self, *, action_input: str = "", **kwargs: Any) -> str:
        return self._executor.exec_read_file(action_input)


class TryImportTool(Tool):
    """Import Python modules inside the container."""

    def __init__(self, executor: ContainerToolExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:  # pragma: no cover - trivial
        return "try_import"

    @property
    def description(self) -> str:  # pragma: no cover - trivial
        return "Attempt to import candidate packages within the built image."

    def execute(self, *, action_input: str = "", **kwargs: Any) -> str:
        return json.dumps(self._executor.exec_try_import(action_input), indent=2)


class ExecArbitraryTool(Tool):
    """Execute arbitrary shell commands inside the container."""

    def __init__(self, executor: ContainerToolExecutor) -> None:
        self._executor = executor

    @property
    def name(self) -> str:  # pragma: no cover - trivial
        return "exec_arbitrary"

    @property
    def description(self) -> str:  # pragma: no cover - trivial
        return "Execute an arbitrary shell command (dangerous)."

    def execute(self, *, action_input: str = "", **kwargs: Any) -> str:
        return self._executor.exec_exec_arbitrary(action_input)


@dataclass
class ContainerToolExecutor:
    """Expose docker-backed tools through the agent tool registry."""

    docker_client: docker.DockerClient
    image_name: str
    container_name: str | None = None
    workdir: str | None = None
    env: dict | None = None
    run_labels: dict[str, str] | None = None

    def __post_init__(self) -> None:
        self._pc = PersistentContainer(
            client=self.docker_client,
            image=self.image_name,
            name=self.container_name,
            workdir=self.workdir,
            env=self.env,
            run_labels=self.run_labels,
        )
        self._pc.start()
        self._repo_root = self._pc.find_repo_root()
        self._facts_json = json.dumps(self._pc.infer_repo_facts(self._repo_root or "/"), indent=2)
        self._registry = ToolRegistry()
        self._register_default_tools()

    def _register_default_tools(self) -> None:
        self._registry.register(ProbeRepoTool(self))
        self._registry.register(ListTreeTool(self))
        self._registry.register(ReadFileTool(self))
        self._registry.register(TryImportTool(self))
        self._registry.register(ExecArbitraryTool(self))

    @property
    def registry(self) -> ToolRegistry:
        """Return the backing registry for these tools."""

        return self._registry

    def shutdown(self) -> None:
        """Stop the backing container."""

        try:
            self._pc.stop()
        except Exception:  # pragma: no cover - defensive
            logger.warning("Error stopping container", exc_info=True)

    def choose_action(self, action: str, action_input: str) -> str:
        """Dispatch ``action`` to the matching tool and return the observation."""

        normalized = (action or "").strip().lower()
        tool = self._registry.get(normalized)
        if not tool:
            return f"[noop] Unknown action '{normalized}'"
        try:
            observation = cast(str, tool.execute(action_input=action_input))
        except ToolExecutionError as exc:
            logger.warning("Tool %s failed", normalized, exc_info=True)
            return f"[tool_error] {type(exc).__name__}: {exc}"
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unexpected error executing tool %s", normalized)
            return f"[tool_error] {type(exc).__name__}: {exc}"

        if normalized == "probe_repo":
            self._facts_json = observation
        return observation

    def exec_probe_repo(self) -> str:
        self._repo_root = self._pc.find_repo_root()
        facts = self._pc.infer_repo_facts(self._repo_root or "/")
        self._facts_json = json.dumps(facts, indent=2)
        return self._facts_json

    def exec_list_tree(self, action_input: str) -> str:
        root = self._repo_root or "/"
        items = self._pc.list_tree(root, max_depth=3, max_items=600)
        return json.dumps({"repo_root": root, "files": items[:120]}, indent=2)

    def exec_read_file(self, action_input: str) -> str:
        try:
            args = json.loads(action_input or "{}")
        except Exception as exc:
            raise ToolExecutionError(f"Invalid JSON input: {exc}") from exc
        path = args.get("path")
        max_bytes = int(args.get("max_bytes", 64_000))
        if not path:
            return "[read_file] missing 'path'"
        if not path.startswith("/"):
            base = self._repo_root or "/"
            path = f"{base}" + ("" if path.startswith("/") else "/") + path
        body = self._pc.read_file(path, max_bytes=max_bytes)
        if not body:
            return f"[read_file] empty or not found: {path}"
        return f"--- BEGIN {path} ---\n{body}\n--- END {path} ---"

    def exec_try_import(self, action_input: str) -> dict[str, Any]:
        try:
            args = json.loads(action_input or "{}")
        except Exception as exc:
            raise ToolExecutionError(f"Invalid JSON input: {exc}") from exc
        if not isinstance(args, dict):
            raise ToolExecutionError("Invalid try_import payload: expected JSON object")
        raw_candidates = args.get("candidates", [])
        if not isinstance(raw_candidates, list):
            raise ToolExecutionError("Invalid 'candidates' payload; expected a list")
        candidates = [str(c).strip() for c in raw_candidates if str(c).strip()]
        cmd_py = str(args.get("python", "python"))
        return self._pc.try_import(cmd_py, candidates)

    def exec_exec_arbitrary(self, action_input: str) -> str:
        cmd = action_input.strip().split("\n")[0][:200]
        if not cmd:
            return "[exec_arbitrary] missing command"
        res = self._pc.exec(cmd, timeout_s=30)
        stdout_snip = (res.stdout[:1000] + "..." + res.stdout[-1000:]) if len(res.stdout) > 2000 else res.stdout
        stderr_snip = (res.stderr[:1000] + "..." + res.stderr[-1000:]) if len(res.stderr) > 2000 else res.stderr
        return f"[exec_arbitrary] rc={res.rc}\n--- STDOUT ---\n{stdout_snip}\n--- STDERR ---\n{stderr_snip}"

    def facts_json(self) -> str:
        self._facts_json = json.dumps(self._pc.infer_repo_facts(self._repo_root or "/"), indent=2)
        return self._facts_json

    def import_check(self, cmd_python: str) -> dict:
        facts = self._pc.infer_repo_facts(self._repo_root or "/")
        pkg_candidates = facts.get("pkg_candidates", []) or []
        version = next(iter(facts.get("python_versions_from_asv", [])), None)
        if version:
            env = f"asv_{version}"
            cmd_python = f"micromamba run -n {env} python"
        return self._pc.try_import(cmd_python, pkg_candidates)


__all__ = [
    "ContainerToolExecutor",
    "ExecArbitraryTool",
    "ExecResult",
    "ListTreeTool",
    "PersistentContainer",
    "ProbeRepoTool",
    "ReadFileTool",
    "TryImportTool",
]
