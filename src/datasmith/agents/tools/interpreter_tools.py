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

    def file_operations(
        operation: str,
        path: str,
        old_content: str | None = None,
        new_content: str | None = None,
        max_bytes: int | None = 256000,
    ) -> str:
        """
        Perform file operations inside the Docker container.

        IMPORTANT: You can only EDIT existing files, NOT create new ones.

        Operations:
        - read: Read a file's contents
        - edit: Replace old_content with new_content in an existing file
        - list: List files in a directory
        """
        kwargs: dict[str, Any] = {}
        if max_bytes is not None:
            kwargs["max_bytes"] = max_bytes
        return docker_file_tool(operation, path, old_content=old_content, new_content=new_content, **kwargs)

    # Some tooling stacks (e.g., agent frameworks) inspect __annotations__
    # and assume simple types with a __name__ attribute. Override the
    # runtime annotations to avoid PEP 604 unions causing issues,
    # while keeping source-level hints for static analysis.
    file_operations.__annotations__ = {
        "operation": str,
        "path": str,
        "old_content": str,
        "new_content": str,
        "max_bytes": int,
        "return": str,
    }

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

    def edit_file(self, path: str, old_content: str, new_content: str) -> str:
        """
        Edit an EXISTING file inside the container by replacing old_content with new_content.

        IMPORTANT: You can only EDIT existing files, NOT create new ones.
        Use heredocs in shell scripts if you need temporary files.

        Args:
            path: Absolute path to the EXISTING file inside the container
            old_content: The exact content to replace (must exist in the file)
            new_content: The new content to replace with

        Returns:
            Success or error message
        """
        try:
            # First check if file exists by trying to read it
            try:
                current_content = self.container.read_file(path, max_bytes=10_000_000)  # Read up to 10MB
            except Exception:
                return f"✗ Error: File {path} does not exist. You can only EDIT existing files, not create new ones. Use heredocs in shell scripts for temporary files."

            # Check if old_content exists in the file
            if old_content not in current_content:
                return f"✗ Error: old_content not found in {path}. The content you want to replace doesn't exist. Read the file first to see its current contents."

            # Replace the content (only first occurrence)
            updated_content = current_content.replace(old_content, new_content, 1)
            self.container.write_file(path, updated_content)
            return f"✓ Successfully edited {path} - replaced {len(old_content)} bytes with {len(new_content)} bytes"
        except Exception as e:
            logger.error(f"Error editing {path}: {e}", exc_info=True)
            return f"✗ Error editing {path}: {e}"

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

    def __call__(
        self,
        operation: str,
        path: str,
        content: str | None = None,
        old_content: str | None = None,
        new_content: str | None = None,
        **kwargs: Any,
    ) -> str:
        """
        Allow the tool to be called with operation dispatch.

        Args:
            operation: One of 'read', 'edit', 'list'
            path: File or directory path
            content: Deprecated - use old_content/new_content instead
            old_content: For 'edit' - content to replace
            new_content: For 'edit' - content to replace with
            **kwargs: Additional arguments

        Returns:
            Operation result
        """
        if operation == "read":
            return self.read_file(path, **kwargs)
        elif operation == "edit":
            if old_content is None or new_content is None:
                return "Error: Both 'old_content' and 'new_content' required for edit operation"
            return self.edit_file(path, old_content, new_content)
        elif operation == "list":
            return self.list_files(path, **kwargs)
        elif operation == "write":
            return "Error: 'write' operation is deprecated. Use 'edit' to modify existing files. You cannot create new files - only edit existing ones."
        else:
            return f"Error: Unknown operation '{operation}'. Use 'read', 'edit', or 'list'."
