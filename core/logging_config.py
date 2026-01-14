import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def setup_logging(log_level: str = "INFO") -> None:
    # Create log directory if it doesn't exist
    log_file = Path("log/app.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create a timed rotating file handler that rotates daily at midnight
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",  # Rotate at midnight
        interval=1,  # Rotate every 1 day
        backupCount=7,  # Keep 7 backup files (7 days of logs)
        encoding="utf-8",  # Use UTF-8 encoding for log files
    )

    # Set the suffix for rotated files (adds date to filename)
    file_handler.suffix = "%Y-%m-%d"

    # Create console handler for stdout output with UTF-8 encoding
    # Reconfigure stdout to use UTF-8 for Windows compatibility
    if sys.stdout.encoding != "utf-8":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    console_handler = logging.StreamHandler(sys.stdout)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)20.20s][%(funcName)20.20s][%(levelname)5.5s] %(message)s",
        handlers=[file_handler, console_handler],
    )