import concurrent.futures
import logging
import pathlib
import re
import sys
from pathlib import Path

from openai import InternalServerError  # or openai.APIError in older clients
from phi.agent import Agent
from phi.model.openai.like import OpenAILike

# Add datasmith to path if needed
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dataset.verify import load
from datasmith.agents.container_toolbox import PersistentContainer
from datasmith.agents.tools.interpreter_tools import make_docker_file_tool, make_docker_shell_tool
from datasmith.docker.orchestrator import build_repo_sha_image, get_docker_client

MSG_HEADER_RE = re.compile("unexpected tokens remaining in message header")
logger = logging.getLogger(__name__)

# 1) Model: OpenAI-compatible server (safe to share across threads)
model = OpenAILike(
    id="qwen3-coder-30b-a3b-instruct",
    api_key="fake_key",
    base_url="http://0.0.0.0:30001/v1",
)

# 2) Docker client (shared across threads)
docker_client = get_docker_client()


def create_build_agent(task_dir: pathlib.Path) -> tuple[Agent, PersistentContainer]:
    """
    Create a fresh Agent + Docker container tools for a single directory run.

    Returns:
        Tuple of (Agent, PersistentContainer) - container must be stopped when done
    """
    # Load task and context from directory
    task, context = load(task_dir)

    # Build Docker image for this task
    run_task = task.with_tag("run")
    run_id = f"interpreter-{task.sha}"

    logger.info(f"Building image for {task_dir.name}...")
    build_res = build_repo_sha_image(
        client=docker_client,
        docker_ctx=context,
        task=run_task,
        force=False,  # Use cached if available
        run_id=run_id,
    )

    if not build_res.ok:
        raise RuntimeError(f"Docker build failed: {build_res.stderr_tail}")

    # Create persistent container with bind mount
    container = PersistentContainer(
        client=docker_client,
        image=run_task.get_image_name(),
        name=f"interpreter-{task.repo.replace('/', '-')}-{task.sha[:8]}",
        workdir="/workspace/repo",
        run_labels={"datasmith.run": run_id, "datasmith.type": "interpreter"},
        volumes={str(task_dir.absolute()): {"bind": "/agent_workspace", "mode": "rw"}},
    )

    # Start container (will handle name conflicts automatically)
    container.start()

    # Create Docker-aware tools (function-based for phi Agent compatibility)
    shell_tool = make_docker_shell_tool(container)
    file_tool = make_docker_file_tool(container)

    agent = Agent(
        name="build-fixing-bot",
        model=model,
        tools=[shell_tool, file_tool],
        show_tool_calls=True,
        instructions=[
            "You are debugging pytest test failures inside a Docker container.",
            "",
            "CRITICAL RULES:",
            "1. You can ONLY edit existing files in /agent_workspace - you CANNOT create new files",
            "2. Do NOT create random test files like 'simple_test.py' or 'test.sh' - they will be ignored",
            "3. You MUST run verify.py to test your changes: uv run python dataset/verify.py --task <task_path>",
            "4. If you need temporary files, use heredocs inside the shell scripts",
            "",
            "FILES YOU CAN EDIT (in /agent_workspace):",
            "- run_tests.sh: The main pytest execution script",
            "- docker_build_run.sh: Docker build script for the 'run' stage",
            "- Other existing files in /agent_workspace",
            "",
            "WORKFLOW:",
            "1. Read /agent_workspace/run_tests.sh to understand current setup",
            "2. Edit the file using file_operations(operation='edit', path=..., old_content=..., new_content=...)",
            "3. Copy your changes: cp /agent_workspace/run_tests.sh /run_tests.sh",
            "4. TEST with verify.py: run_shell('cd /workspace/repo && uv run python dataset/verify.py --task <path>')",
            "5. Read the verify.py output to see if tests pass",
            "6. If tests fail, repeat from step 2",
            "",
            "Remember: The ONLY way to know if your changes work is to run verify.py!",
        ],
    )

    return agent, container


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
    """Run agent on a single task directory with Docker container tools."""
    print(f"[INFO] Running directory: {task_dir}")
    container = None
    try:
        agent, container = create_build_agent(task_dir)

        # Read error log if it exists
        error_log = ""
        if (task_dir / "test_failure.log").exists():
            error_log = (task_dir / "test_failure.log").read_text()

        msg = f"""
TASK: Fix pytest failures for {task_dir.name}

Task directory: {task_dir.resolve()}
Test script: /agent_workspace/run_tests.sh (bind-mounted from host)

Previous failure (if any):
```
{error_log or "No error log found - this is the first run."}
```

CRITICAL: You MUST run verify.py to test your changes!
verify.py command: uv run python dataset/verify.py --task {task_dir.resolve()}

WORKFLOW (you MUST follow this):
1. Read /agent_workspace/run_tests.sh to understand the test setup
2. Make SMALL edits to fix issues (use file_operations with operation='edit')
3. Copy to container: run_shell('cp /agent_workspace/run_tests.sh /run_tests.sh')
4. RUN VERIFY.PY: run_shell('cd /workspace/repo && timeout 600 uv run python dataset/verify.py --task {task_dir.resolve()}')
5. Read verify.py output - if tests fail, repeat from step 2

RULES:
- You can ONLY edit existing files in /agent_workspace (run_tests.sh, docker_build_run.sh, etc.)
- Do NOT create new files like simple_test.py or test.sh
- Use heredocs inside scripts if you need temporary files
- NEVER skip tests or reduce test coverage
- The ONLY way to verify success is running verify.py

Files you can edit:
- /agent_workspace/run_tests.sh: pytest execution
- /agent_workspace/docker_build_run.sh: system packages, python deps
- /agent_workspace/docker_build_env.sh: environment setup

Changes persist to host automatically via bind mount.
""".strip()

        print("=" * 80)
        print(f"[START] Running task dir: {task_dir}")

        for attempt in range(5):
            try:
                safe_print_response(agent, msg)
                print(f"[DONE ] Task dir: {task_dir}")
                break
            except Exception as e:
                print(f"[ERROR] Attempt {attempt + 1}/5 for {task_dir}: {e}")
                if attempt == 4:
                    raise

    finally:
        # Always cleanup container
        if container:
            try:
                container.stop()
                logger.info(f"Stopped container for {task_dir}")
            except Exception:
                logger.exception(f"Error stopping container for {task_dir}")


if __name__ == "__main__":
    root = pathlib.Path("dataset/formulacode_verified")
    task_dirs = [
        d
        for d in root.rglob("*/*")
        if d.is_dir() and (d / "test_failure.log").exists() and (not (d / "validation_success.json").exists())
    ]
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
