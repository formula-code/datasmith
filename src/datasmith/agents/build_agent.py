# # file: reactive_docker_builder.py
# from __future__ import annotations

# import os
# import json
# import shlex
# from typing import Sequence, Dict, Any

# # --- LangChain / LLMs ---
# from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder, PromptTemplate
# from langchain_core.tools import StructuredTool
# from langchain.agents import AgentExecutor, create_react_agent
# from langchain_core.language_models import BaseLanguageModel

# from docker.models.containers import Container

# # Example LLM import (swap to your provider, e.g., langchain_openai, langchain_anthropic, etc.)
# # from langchain_openai import ChatOpenAI

# # Optional: online docs tool (enabled if TAVILY_API_KEY is set)
# _HAVE_TAVILY = False

# # Docker SDK: you provide an already-running docker.models.containers.Container
# # Docs for exec_run demux/stream behavior: https://docker-py.readthedocs.io/en/stable/user_guides/multiplex.html
# # and general container API: https://docker-py.readthedocs.io/en/stable/containers.html


# # -----------------------------
# # Docker exec helpers / tools
# # -----------------------------
# def _exec_in_container(container: Container, cmd: str, workdir: str | None = None, env: dict[str, str] | None = None,
#                        stream: bool = True, demux: bool = True, timeout: int | None = None) -> dict[str, Any]:
#     """
#     Execute a non-interactive bash command inside the container with proper demuxing of stdout/stderr.
#     Returns a dict containing exit_code, stdout, stderr.
#     """
#     # Use bash -lc so users can chain commands, source envs, etc.
#     quoted = shlex.quote(cmd)
#     final_cmd = f"/bin/bash -lc {quoted}"

#     # NOTE: container.exec_run supports demux/stream; see docs.
#     # We keep TTY disabled so demux works predictably.
#     import IPython; IPython.embed()
#     res = container.exec_run(
#         final_cmd,
#         stream=stream,
#         demux=demux,
#         environment=env,
#         workdir=workdir,
#         tty=False,
#     )

#     # Aggregate streamed output if requested
#     stdout_chunks, stderr_chunks = [], []
#     if stream:
#         for out_tup in res.output:
#             if demux:
#                 out, err = out_tup
#                 if out:
#                     stdout_chunks.append(out.decode("utf-8", "replace"))
#                 if err:
#                     stderr_chunks.append(err.decode("utf-8", "replace"))
#             else:
#                 # single stream
#                 stdout_chunks.append(out_tup.decode("utf-8", "replace"))
#     else:
#         if demux:
#             out, err = res.output  # (stdout, stderr)
#             if out:
#                 stdout_chunks.append(out.decode("utf-8", "replace"))
#             if err:
#                 stderr_chunks.append(err.decode("utf-8", "replace"))
#         else:
#             stdout_chunks.append(res.output.decode("utf-8", "replace"))

#     stdout = "".join(stdout_chunks)
#     stderr = "".join(stderr_chunks)
#     exit_code = getattr(res, "exit_code", None)

#     return {"exit_code": exit_code, "stdout": stdout, "stderr": stderr}


# def _truncate(s: str, limit: int = 4000) -> str:
#     if s is None:
#         return ""
#     return s if len(s) <= limit else (s[:limit] + f"\n...[truncated {len(s)-limit} chars]...")


# def make_bash_tool(container, name: str = "bash") -> StructuredTool:
#     """
#     Create a LangChain StructuredTool that executes bash commands in the given Docker container.
#     Important: Each call is a fresh non-interactive shell; 'cd' does not persist across calls.
#     Always chain with 'cd path && do_the_thing'.
#     """
#     def _run_bash(cmd: str) -> str:
#         """
#         Run a bash command inside the connected Docker container and return a JSON payload:
#           {"exit_code": int, "stdout": "...", "stderr": "..."}
#         Keep outputs trimmed to avoid token bloat.
#         """
#         result = _exec_in_container(container, cmd, stream=True, demux=True)
#         return json.dumps({
#             "exit_code": result["exit_code"],
#             "stdout": _truncate(result["stdout"]),
#             "stderr": _truncate(result["stderr"])
#         })

#     return StructuredTool.from_function(
#         name=name,
#         func=_run_bash,
#         description=(
#             "Execute a non-interactive bash command inside the target Docker container. "
#             "Use this to run build tools, pip, python, make, etc. "
#             "IMPORTANT: Shell state is not persistent across calls; if you need to run in a directory, "
#             "prefix commands with 'cd <dir> && ...'. Return value is JSON with keys exit_code, stdout, stderr."
#         ),
#     )


# def import_check(container, package: str) -> Dict[str, Any]:
#     """
#     Validate that 'python -c \"import {package}\"' works inside the container.
#     Returns {ok: bool, exit_code: int, stdout: str, stderr: str}
#     """
#     # Prefer a clean check that prints any exception for the agent/user to see.
#     cmd = f'python -c "import importlib,sys; ' \
#           f'pkg={json.dumps(package)}; ' \
#           f'print(f\'checking import {{pkg}}...\'); ' \
#           f'importlib.import_module(pkg)"'
#     res = _exec_in_container(container, cmd, stream=False, demux=True)
#     ok = (res["exit_code"] == 0)
#     stdout = res.get("stdout") or ""
#     stderr = res.get("stderr") or ""
#     return {"ok": ok, "exit_code": res["exit_code"], "stdout": stdout, "stderr": stderr}


# # ---------------------------------
# # Agent construction & orchestration
# # ---------------------------------
# def make_react_prompt() -> PromptTemplate:
#     """
#     ReAct prompt template compatible with langchain.agents.create_react_agent.
#     (Per LangChain docs, the prompt must expose {tools}, {tool_names}, and {agent_scratchpad}.)
#     """
#     template = """You are a careful build engineer working **inside a Docker container**.

# Goal:
# - {goal}

# Hard stop condition:
# - The task is finished when `python -c "import {package}"` returns exit code 0.

# Environment constraints:
# - Each tool call runs in a fresh, non-interactive shell; **`cd` does not persist**. If you need a working dir, do `cd <path> && ...`.
# - Prefer standards-based Python packaging (PEP 517/518) with `python -m build` or `pip install .` depending on the project layout.
# - Keep commands idempotent when possible.
# - Be explicit; show the entire command you run.

# You have access to these tools:
# {tools}

# Use this strict format:
# Question: restate the next concrete subtask
# Thought: explain briefly what you'll do
# Action: the action to take, must be one of [{tool_names}]
# Action Input: exact input for the action
# Observation: the result
# ...(repeat Thought/Action/Action Input/Observation as needed)
# Thought: I now know the final answer
# Final Answer: a short summary of what was done and why it should satisfy the stop condition

# Begin!

# Question: {input}
# Thought:{agent_scratchpad}
# """
#     return PromptTemplate.from_template(template)


# def build_react_agent(
#     llm: BaseLanguageModel,
#     container: Container,
#     max_iterations: int = 20,
# ) -> AgentExecutor:
#     """
#     Construct a ReAct agent wired to the Docker bash tool and (optionally) a web-docs search tool.
#     """
#     tools: list = [make_bash_tool(container)]
#     prompt = make_react_prompt()
#     agent = create_react_agent(llm, tools, prompt)  # (Legacy LangChain ReAct). See docs.
#     # NOTE: AgentExecutor supports guards like max_iterations and early_stopping_method.
#     executor = AgentExecutor(
#         agent=agent,
#         tools=tools,
#         verbose=True,
#         handle_parsing_errors=True,
#         max_iterations=max_iterations,
#         early_stopping_method="generate",
#         return_intermediate_steps=False,
#     )
#     return executor


# def run_build_agent(
#     agent: AgentExecutor,
#     problem_description: str,
#     package: str,
# ) -> Dict[str, Any]:
#     """
#     Run the agent once (internally it may take many tool-calls up to max_iter).
#     After it finishes, probe the stop condition inside the container.

#     Returns: {
#       "agent_output": str,
#       "import_check": {"ok": bool, "exit_code": int, "stdout": str, "stderr": str}
#     }
#     """
#     user_input = (
#         f"{problem_description}\n"
#         f"Target package name: {package}\n"
#         f"Remember: The task is finished when `python -c \"import {package}\"` succeeds."
#     )
#     # Cap total internal steps safely
#     result = agent.invoke(
#         {
#             "input": user_input,              # your composed instructions
#             "goal": problem_description,      # <— add this
#             "package": package,          # <— and this
#         },
#         config={"configurable": {"thread_id": "build-session-1"}},
#     )
#     # After the agent stops, verify the explicit finish condition:
#     # (This ensures we don't consider 'done' unless the import actually works.)
#     # We rely on the bash tool's statelessness—no residual shell state.
#     # The import check uses python -c import; see Docker exec docs for behavior of exec sessions.
#     executor_tools = getattr(agent, "tools", None)  # not needed, just illustrative
#     # The container reference is captured inside the bash tool; we re-check via helper:
#     # (If you need to pass the container explicitly, expose it to this function too.)
#     # Here, we assume you still have a reference (e.g., close over it or store globally).
#     # For simplicity, require caller to call `import_check` directly and attach to this return.
#     return {
#         "agent_output": result.get("output", str(result)),
#         # Caller should supply the container again for the check:
#         # But we can't access it from AgentExecutor. Return a stub and instruct caller to call import_check().
#     }
