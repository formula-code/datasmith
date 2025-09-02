from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import docker

from datasmith.agents.container_toolbox import PersistentContainer

logger = logging.getLogger(__name__)


@dataclass
class ContainerToolExecutor:
    """
    Wires DSPy 'tools' to a persistent container.
    """

    docker_client: docker.DockerClient
    image_name: str
    container_name: str | None = None
    workdir: str | None = None
    env: dict | None = None

    def __post_init__(self) -> None:
        self._pc = PersistentContainer(
            client=self.docker_client,
            image=self.image_name,
            name=self.container_name,
            workdir=self.workdir,
            env=self.env,
        )
        self._pc.start()
        self._repo_root = self._pc.find_repo_root()
        self._facts_json = json.dumps(self._pc.infer_repo_facts(self._repo_root or "/"), indent=2)

    def shutdown(self) -> None:
        try:
            self._pc.stop()
        except Exception:
            logger.warning("Error stopping container", exc_info=True)
            pass

    # ---- DSPy tool entry points ----

    def choose_action(self, action: str, action_input: str) -> str:
        observation = ""
        try:
            if action == "probe_repo":
                facts_json = self.exec_probe_repo()
                observation = f"[probe_repo] OK\n{facts_json[:2000]}"
                # repo_facts_json = facts_json  # refresh for next step
                self._facts_json = facts_json
            elif action == "list_tree":
                observation = self.exec_list_tree(action_input)
            elif action == "read_file":
                observation = self.exec_read_file(action_input)
            elif action == "try_import":
                observation = self.exec_try_import(action_input)
            elif action == "exec_arbitrary":
                # careful, this is arbitrary code execution!
                cmd = action_input.strip().split("\n")[0][:200]
                if not cmd:
                    observation = "[exec_arbitrary] missing command"
                else:
                    res = self._pc.exec(cmd, timeout_s=30)
                    stdout_snip = (
                        (res.stdout[:1000] + "..." + res.stdout[-1000:]) if len(res.stdout) > 2000 else res.stdout
                    )
                    stderr_snip = (
                        (res.stderr[:1000] + "..." + res.stderr[-1000:]) if len(res.stderr) > 2000 else res.stderr
                    )
                    observation = (
                        f"[exec_arbitrary] rc={res.rc}\n--- STDOUT ---\n{stdout_snip}\n--- STDERR ---\n{stderr_snip}"
                    )
            else:
                observation = f"[noop] Unknown action '{action}'"
        except Exception as e:
            observation = f"[tool_error] {type(e).__name__}: {e}"
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
        except Exception:
            args = {}
        path = args.get("path")
        max_bytes = int(args.get("max_bytes", 64_000))
        if not path:
            return "[read_file] missing 'path'"
        if not path.startswith("/"):
            # interpret relative to repo_root
            path = f"{self._repo_root or '/'}" + ("" if path.startswith("/") else "/") + path
        body = self._pc.read_file(path, max_bytes=max_bytes)
        if not body:
            return f"[read_file] empty or not found: {path}"
        return f"--- BEGIN {path} ---\n{body}\n--- END {path} ---"

    def exec_try_import(self, action_input: str) -> str:
        try:
            args = json.loads(action_input or "{}")
        except Exception:
            args = {}
        cands = args.get("candidates") or []
        cmd_py = args.get("python", "python")
        res = self._pc.try_import(cmd_py, cands)
        return json.dumps(res, indent=2)

    # convenience for callers outside DSPy loop
    def facts_json(self) -> str:
        self._facts_json = json.dumps(self._pc.infer_repo_facts(self._repo_root or "/"), indent=2)
        return self._facts_json

    def import_check(self, cmd_python: str) -> dict:
        facts = self._pc.infer_repo_facts(self._repo_root or "/")
        pkg_candidates = facts.get("pkg_candidates", []) or []
        v = next(iter(facts.get("python_versions_from_asv", [])), None)
        if v:
            env = f"asv_{v}"
            cmd_python = f"micromamba run -n {env} python"
            return self._pc.try_import(cmd_python, pkg_candidates)
        return self._pc.try_import(cmd_python, pkg_candidates)
