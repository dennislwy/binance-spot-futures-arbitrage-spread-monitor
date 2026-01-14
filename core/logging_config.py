"""Logging configuration module for the Binance Spot-Futures Arbitrage Monitor.

This module provides centralized logging setup with both file-based and console
output handlers. Logs are automatically rotated daily and retained for 14 days.
"""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logging(log_level: str = "INFO") -> None:
    """Configure application-wide logging with file rotation and console output.
    
    Sets up dual logging handlers:
    - TimedRotatingFileHandler: Writes to log/app.log with daily rotation at midnight
    - StreamHandler: Outputs to console (stdout) for real-time monitoring
    
    The log directory is created automatically if it doesn't exist. Log files older
    than 14 days are automatically deleted. Console output is forced to UTF-8 encoding
    for Windows compatibility.
    
    Args:
        log_level (str): Logging level as string. Valid values are "DEBUG", "INFO",
            "WARNING", "ERROR", "CRITICAL". Defaults to "INFO".
    
    Returns:
        None
    
    Raises:
        OSError: If log directory cannot be created due to permission issues.
        ValueError: If log_level is not a valid logging level string.
    
    Example:
        >>> setup_logging()  # Uses default INFO level
        >>> setup_logging(log_level="DEBUG")  # Enable debug logging
    
    Note:
        This function configures the root logger, affecting all loggers in the
        application unless they explicitly override the configuration.
    """
    # Create log directory structure if it doesn't already exist
    # Uses Path.mkdir() with parents=True for recursive directory creation
    log_file = Path("log/app.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Configure file handler with time-based rotation
    # Rotates at midnight daily, keeping 14 days of backup logs
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),      # Convert Path object to string for compatibility
        when="midnight",              # Rotation occurs at midnight (00:00:00)
        interval=1,                   # Rotate every 1 day (combined with 'when')
        backupCount=14,                # Backup files to keep (14 days retention)
        encoding="utf-8",             # UTF-8 encoding for international characters
    )

    # Set the suffix for rotated log files (e.g., app.log.2026-01-14)
    # This makes it easy to identify logs by date
    file_handler.suffix = "%Y-%m-%d"

    # Configure console handler for stdout output
    # Ensures UTF-8 encoding on Windows where default might be cp1252
    if sys.stdout.encoding != "utf-8":
        import io

        # Wrap stdout with UTF-8 TextIOWrapper to handle Unicode characters
        # The 'replace' error handler replaces unencodable characters with '?'
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding="utf-8",
            errors="replace"
        )

    # Create console handler that writes to the (possibly wrapped) stdout
    console_handler = logging.StreamHandler(sys.stdout)

    # Configure the root logger with both handlers
    # Format includes: timestamp, logger name, function name, level, and message
    # Example: "2026-01-14 10:30:45 [         __main__][      monitor_symbol][ INFO] Starting monitor..."
    logging.basicConfig(
        level=log_level,              # Minimum severity level to log
        format="%(asctime)s [%(name)20.20s][%(funcName)20.20s][%(levelname)5.5s] %(message)s",
        handlers=[file_handler, console_handler],  # Both file and console output
    )