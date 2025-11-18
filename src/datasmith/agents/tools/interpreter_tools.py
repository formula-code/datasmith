"""
Agent tools for interacting with Docker containers during interpreter debugging.

These tools allow agents to execute shell commands and manage files inside
long-running Docker containers, with changes persisting to bind-mounted host directories.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from datasmith.agents.container_toolbox import PersistentContainer

logger = logging.getLogger(__name__)


def make_docker_shell_tool(container: PersistentContainer) -> Callable[[str, int], str]:
    """
    Create a shell command execution function for a Docker container.

    Args:
        container: The PersistentContainer to execute commands in

    Returns:
        A function that executes shell commands inside the container
    """

    def run_shell(command: str, timeout: int = 60) -> str:
        """
        Execute a shell command inside the Docker container.

        Args:
            command: The shell command to execute
            timeout: Timeout in seconds (default: 60)

        Returns:
            Formatted output with exit code, stdout, and stderr
        """
        result = container.exec(command, timeout_s=timeout)

        output_parts = []

        # Format the response for the agent
        if result.rc == 0:
            output_parts.append(f"✓ Command succeeded (exit code: {result.rc})")
        else:
            output_parts.append(f"✗ Command failed (exit code: {result.rc})")

        if result.stdout:
            output_parts.append(f"\n--- STDOUT ---\n{result.stdout.strip()}")

        if result.stderr:
            output_parts.append(f"\n--- STDERR ---\n{result.stderr.strip()}")

        return "\n".join(output_parts)

    run_shell.__name__ = "run_shell"
    return run_shell


def make_docker_file_tool(container: PersistentContainer) -> Callable:
    """
    Create a file operations function for a Docker container.

    This wraps ``DockerFileTool`` to provide a simple callable interface
    while keeping the underlying file logic in a dedicated class.

    Args:
        container: The PersistentContainer to operate on

    Returns:
        A function that performs file operations inside the container
    """

    docker_file_tool = DockerFileTool(container)

    def file_operations(operation: str, path: str, content: str | None = None, max_bytes: int | None = 256000) -> str:
        kwargs: dict[str, Any] = {}
        if max_bytes is not None:
            kwargs["max_bytes"] = max_bytes
        return docker_file_tool(operation, path, content, **kwargs)

    file_operations.__name__ = "file_operations"
    return file_operations


class DockerShellTool:
    """
    Tool for executing shell commands inside a Docker container.

    This wraps PersistentContainer.exec() to provide a shell interface
    for AI agents debugging containerized builds.
    """

    def __init__(self, container: PersistentContainer):
        """
        Initialize with a PersistentContainer instance.

        Args:
            container: The running container to execute commands in
        """
        self.container = container
        self.name = "docker_shell"
        self.__name__ = "docker_shell"
        self.description = "Execute shell commands inside the Docker container"

    def run_shell(self, command: str, timeout: int = 60) -> str:
        """
        Execute a shell command inside the container.

        Args:
            command: The shell command to execute
            timeout: Timeout in seconds (default: 60)

        Returns:
            Formatted output with exit code, stdout, and stderr
        """
        result = self.container.exec(command, timeout_s=timeout)

        output_parts = []

        # Format the response for the agent
        if result.rc == 0:
            output_parts.append(f"✓ Command succeeded (exit code: {result.rc})")
        else:
            output_parts.append(f"✗ Command failed (exit code: {result.rc})")

        if result.stdout:
            output_parts.append(f"\n--- STDOUT ---\n{result.stdout.strip()}")

        if result.stderr:
            output_parts.append(f"\n--- STDERR ---\n{result.stderr.strip()}")

        return "\n".join(output_parts)

    def __call__(self, command: str, timeout: int = 60) -> str:
        """Allow the tool to be called directly."""
        return self.run_shell(command, timeout)


class DockerFileTool:
    """
    Tool for file operations inside a Docker container.

    Provides read, write, and list operations on files inside the container.
    Changes to bind-mounted directories automatically persist to the host.
    """

    def __init__(self, container: PersistentContainer):
        """
        Initialize with a PersistentContainer instance.

        Args:
            container: The running container to operate on
        """
        self.container = container
        self.name = "docker_file"
        self.__name__ = "docker_file"
        self.description = "Read, write, and list files inside the Docker container"

    def read_file(self, path: str, max_bytes: int = 256000) -> str:
        """
        Read a file from inside the container.

        Args:
            path: Absolute path to the file inside the container
            max_bytes: Maximum bytes to read (default: 256KB)

        Returns:
            File contents as string
        """
        try:
            content = self.container.read_file(path, max_bytes=max_bytes)
        except Exception as e:
            return f"Error reading {path}: {e}"
        else:
            if not content:
                return f"File at {path} is empty or could not be read"
            return f"--- Contents of {path} ---\n{content}"

    def write_file(self, path: str, content: str) -> str:
        """
        Write content to a file inside the container.

        If the path is in a bind-mounted directory, changes persist to the host.

        Args:
            path: Absolute path to the file inside the container
            content: Content to write

        Returns:
            Success or error message
        """
        try:
            self.container.write_file(path, content)
            return f"✓ Successfully wrote {len(content)} bytes to {path}"
        except Exception as e:
            logger.error(f"Error writing to {path}: {e}", exc_info=True)
            return f"✗ Error writing to {path}: {e}"

    def list_files(self, directory: str, max_depth: int = 3, max_items: int = 500) -> str:
        """
        List files in a directory inside the container.

        Args:
            directory: Absolute path to directory
            max_depth: Maximum depth to traverse (default: 3)
            max_items: Maximum number of items to return (default: 500)

        Returns:
            List of file paths
        """
        try:
            files = self.container.list_tree(directory, max_depth=max_depth, max_items=max_items)
        except Exception as e:
            return f"Error listing {directory}: {e}"
        else:
            if not files:
                return f"No files found in {directory} (or directory doesn't exist)"

            file_list = "\n".join(f"  - {f}" for f in files[:max_items])
            total_msg = f" (showing {max_items} of {len(files)})" if len(files) > max_items else ""
            return f"--- Files in {directory}{total_msg} ---\n{file_list}"

    def __call__(self, operation: str, path: str, content: str | None = None, **kwargs: Any) -> str:
        """
        Allow the tool to be called with operation dispatch.

        Args:
            operation: One of 'read', 'write', 'list'
            path: File or directory path
            content: Content for write operations
            **kwargs: Additional arguments

        Returns:
            Operation result
        """
        if operation == "read":
            return self.read_file(path, **kwargs)
        elif operation == "write":
            if content is None:
                return "Error: 'content' required for write operation"
            return self.write_file(path, content)
        elif operation == "list":
            return self.list_files(path, **kwargs)
        else:
            return f"Error: Unknown operation '{operation}'. Use 'read', 'write', or 'list'."
