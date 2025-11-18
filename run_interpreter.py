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
            "Files in /agent_workspace are bind-mounted from the host and changes persist.",
            "The main script to modify is /agent_workspace/run_tests.sh",
            "Use run_shell to execute commands and file_operations to read/write files.",
            "After modifying run_tests.sh, copy it to /run_tests.sh before testing.",
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
    container = None
    try:
        agent, container = create_build_agent(task_dir)

        # Read error log if it exists
        error_log = ""
        if (task_dir / "test_failure.log").exists():
            error_log = (task_dir / "test_failure.log").read_text()

        msg = f"""
You are debugging pytest failures for task: {task_dir.name}

The test script is located at /agent_workspace/run_tests.sh (bind-mounted from host).
Changes you make to files in /agent_workspace will persist to the host machine.

Here is the last error message:
```
{error_log or "No error log found - this is the first run."}
```

Your task:
1. Read /agent_workspace/run_tests.sh to understand the current test setup
2. Run the tests to see what fails: bash /agent_workspace/run_tests.sh
3. Analyze the errors and modify /agent_workspace/run_tests.sh to fix them. Do not destroy existing logic. Always run the full test suite.
4. Copy your fixed version to /run_tests.sh so verify.py can use it: cp /agent_workspace/run_tests.sh /run_tests.sh
5. Re-run tests until they pass

You have access to:
- docker_shell: Execute commands inside the container
- docker_file: Read, write, and list files inside the container

Remember: Files in /agent_workspace are bind-mounted and changes persist to the host!
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
