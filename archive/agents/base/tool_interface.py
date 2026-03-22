"""Tool interface and registry for agent tools."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Protocol


class Tool(Protocol):
    """Protocol defining the interface for agent tools.

    Tools are callable operations that agents can use to interact
    with the environment (e.g., container operations, file operations).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The unique name of this tool."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """A human-readable description of what this tool does."""
        ...

    @abstractmethod
    def execute(self, **kwargs: Any) -> Any:
        """Execute the tool with the given arguments.

        Args:
            **kwargs: Tool-specific arguments.

        Returns:
            The result of executing the tool.

        Raises:
            ToolExecutionError: If tool execution fails.
        """
        ...

    def validate_args(self, **kwargs: Any) -> None:
        """Validate the arguments before execution.

        Args:
            **kwargs: Tool-specific arguments to validate.

        Raises:
            ValueError: If validation fails.
        """
        return None


class ToolRegistry:
    """Registry for managing available tools.

    Provides a central location for registering and retrieving
    tools that agents can use.
    """

    def __init__(self) -> None:
        """Initialize an empty tool registry."""
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        """Register a tool in the registry.

        Args:
            tool: The tool to register.

        Raises:
            ValueError: If a tool with the same name is already registered.
        """
        if tool.name in self._tools:
            raise ValueError(f"Tool '{tool.name}' is already registered")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool | None:
        """Retrieve a tool by name.

        Args:
            name: The name of the tool to retrieve.

        Returns:
            The tool if found, None otherwise.
        """
        return self._tools.get(name)

    def list_tools(self) -> list[str]:
        """List all registered tool names.

        Returns:
            A list of tool names.
        """
        return list(self._tools.keys())

    def __contains__(self, name: str) -> bool:
        """Check if a tool is registered.

        Args:
            name: The tool name to check.

        Returns:
            True if the tool is registered, False otherwise.
        """
        return name in self._tools


class ToolExecutionError(Exception):
    """Exception raised when tool execution fails."""

    pass
