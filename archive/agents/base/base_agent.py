"""Base agent interface for all datasmith agents."""

from __future__ import annotations

import types
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class Agent(ABC, Generic[TInput, TOutput]):
    """Abstract base class for all agents in the datasmith system.

    Agents are responsible for executing specific tasks with defined
    inputs and outputs. They follow a consistent lifecycle:
    1. Validation of inputs
    2. Execution of the core task
    3. Cleanup of resources
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize the agent with optional configuration.

        Args:
            config: Optional configuration dictionary for the agent.
        """
        self.config = config or {}
        self._setup()

    def _setup(self) -> None:
        """Setup method called during initialization.

        Override this method to perform any initialization tasks
        that need to happen after the config is set.
        """
        pass

    @abstractmethod
    def execute(self, input_data: TInput) -> TOutput:
        """Execute the agent's primary task.

        Args:
            input_data: The input data for the agent to process.

        Returns:
            The output result of the agent's execution.

        Raises:
            AgentError: If execution fails.
        """
        pass

    def validate_input(self, input_data: TInput) -> None:
        """Validate input data before execution.

        Args:
            input_data: The input data to validate.

        Raises:
            ValueError: If input validation fails.
        """
        pass

    def cleanup(self) -> None:
        """Cleanup resources after execution.

        Override this method to perform any cleanup tasks that
        need to happen after execution, whether successful or not.
        """
        pass

    def __enter__(self) -> Agent[TInput, TOutput]:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Context manager exit with automatic cleanup."""
        self.cleanup()


class AgentError(Exception):
    """Base exception for agent-related errors."""

    pass


class AgentValidationError(AgentError):
    """Exception raised when agent input validation fails."""

    pass


class AgentExecutionError(AgentError):
    """Exception raised when agent execution fails."""

    pass
