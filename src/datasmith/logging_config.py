"""
Centralized logging configuration for the datasmith package.

This module provides a consistent logging interface across all components
of the datasmith package, with support for different log levels, progress
tracking, and formatted output.
"""

import logging
import sys
from typing import Optional


def configure_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    date_format: str = "%H:%M:%S",
    stream: Optional[object] = None,
) -> logging.Logger:
    """
    Configure logging for the datasmith package.

    Parameters
    ----------
    level : int
        Logging level (default: logging.INFO)
    format_string : str, optional
        Custom format string for log messages
    date_format : str
        Date format for timestamps (default: "%H:%M:%S")
    stream : object, optional
        Stream to write logs to (default: sys.stderr)

    Returns
    -------
    logging.Logger
        Configured logger instance
    """
    if format_string is None:
        format_string = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"

    if stream is None:
        stream = sys.stderr

    # Configure the root logger
    logging.basicConfig(
        level=level,
        format=format_string,
        datefmt=date_format,
        stream=stream,
        force=True,  # Override any existing configuration
    )

    # Get the datasmith logger
    logger = logging.getLogger("datasmith")
    logger.setLevel(level)

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance for the specified name.

    Parameters
    ----------
    name : str, optional
        Logger name. If None, returns the root datasmith logger.

    Returns
    -------
    logging.Logger
        Logger instance
    """
    if name is None:
        return logging.getLogger("datasmith")
    return logging.getLogger(f"datasmith.{name}")


class ProgressLogger:
    """
    A utility class for logging progress updates with consistent formatting.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or get_logger()
        self._last_progress = ""

    def progress(self, message: str, level: int = logging.INFO) -> None:
        """
        Log a progress message.

        Parameters
        ----------
        message : str
            Progress message to log
        level : int
            Logging level (default: logging.INFO)
        """
        self.logger.log(level, message)

    def update_progress(self, message: str, level: int = logging.INFO) -> None:
        """
        Update progress with a message that replaces the previous line.

        Parameters
        ----------
        message : str
            Progress message to log
        level : int
            Logging level (default: logging.INFO)
        """
        # Clear the previous line and write the new message
        if self._last_progress:
            sys.stderr.write("\r\033[K")
        sys.stderr.write(f"\r{message}")
        sys.stderr.flush()
        self._last_progress = message

    def finish_progress(self, final_message: str = "", level: int = logging.INFO) -> None:
        """
        Finish progress tracking and optionally log a final message.

        Parameters
        ----------
        final_message : str
            Final message to log after clearing progress
        level : int
            Logging level for the final message
        """
        if self._last_progress:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()
            self._last_progress = ""

        if final_message:
            self.logger.log(level, final_message)


# Create a default logger instance
default_logger = get_logger()
progress_logger = ProgressLogger(default_logger)
