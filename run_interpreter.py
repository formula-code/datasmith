import concurrent.futures
import pathlib
import re

from openai import InternalServerError  # or openai.APIError in older clients
from phi.agent import Agent
from phi.model.openai.like import OpenAILike
from phi.tools.file import FileTools
from phi.tools.shell import ShellTools

MSG_HEADER_RE = re.compile("unexpected tokens remaining in message header")

# 1) Model: OpenAI-compatible server (safe to share across threads)
model = OpenAILike(
    id="qwen3-coder-30b-a3b-instruct",
    api_key="fake_key",
    base_url="http://0.0.0.0:30001/v1",
)


def create_build_agent() -> Agent:
    """Create a fresh Agent + tools for a single directory run."""
    shell_tools = ShellTools()
    file_tools = FileTools(
        base_dir=".",  # repo root
        read_files=True,
        save_files=True,
        list_files=True,
    )

    return Agent(
        name="build-fixing-bot",
        model=model,
        tools=[shell_tools, file_tools],
        show_tool_calls=True,
        instructions=[
            "You are an assistant that debugs Docker build scripts.",
            "verify.py will only pass when both asv and pytest work properly.",
            "I can guarantee that asv already runs and the package compiles.",
            "your job is to figure out what pytest needs; install it; and rerun.",
        ],
    )


def safe_print_response(agent: Agent, prompt: str, max_retries: int = 3):
    """Call agent.print_response with a small retry loop for 5xx-ish issues."""
    for attempt in range(max_retries):
        try:
            return agent.print_response(prompt, markdown=False)
        except InternalServerError as e:
            if attempt < max_retries - 1 and MSG_HEADER_RE.search(str(e)):
                # log and retry
                print(f"[WARN] Harmony header parse failed, retrying ({attempt + 1}/{max_retries})")
                continue
            raise


def run_directory(task_dir: pathlib.Path):
    agent = create_build_agent()

    msg = f"""
Task directory: {task_dir.resolve()}
You may only edit these files in the directory:
 - docker_build_run.sh (if you need to install dependencies)
 - run_tests.sh (if you need to modify how/where pytest is called)

Steps you MUST follow:

1. Use ShellTools to run the verification command from repo root (Give it 36000 seconds max):
    The verification command runs the full build process in Docker and then runs profile and tests:

   uv run python dataset/verify.py --task "{task_dir.resolve()}"

2. Use FileTools (list_files/read_file/save_file) scoped to this path
   to inspect and edit ONLY files under: {task_dir.resolve()}. Always read a file before editing it.

3. If tests fail:
   - Read the error output.
   - Decide which files in {task_dir.resolve()} to edit.
   - Read the files you want to edit.
   - Decide what changes to make.
   - Edit them with FileTools.
   - Re-run the verify command.

4. Give up after 10 failed verification attempts for this directory.

5. At the end, print:
   - The commands you executed.
   - A bullet list of files you changed and a short summary of each change.
"""
    print("=" * 80)
    print(f"[START] Running task dir: {task_dir}")
    for _ in range(5):
        try:
            safe_print_response(agent, msg)
            print(f"[DONE ] Task dir: {task_dir}")
            break
        except Exception as e:
            print(f"[ERROR] Task dir: {task_dir} -> {e}")


if __name__ == "__main__":
    root = pathlib.Path("dataset/formulacode_verified")
    task_dirs = [d for d in root.rglob("*pandas*/*") if d.is_dir()]

    max_workers = 16
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_dir = {pool.submit(run_directory, d): d for d in task_dirs}

        # Optional: iterate as they complete to log / surface errors
        for future in concurrent.futures.as_completed(future_to_dir):
            d = future_to_dir[future]
            try:
                future.result()
            except Exception as exc:
                print(f"[FUTURE ERROR] {d} generated an exception: {exc}")
