# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Logging utilities for CTA operations tools.

This module provides enhanced logging functionality with support for rotating log files,
automatic compression, and group ownership management. It is designed to handle high-volume
logging scenarios with automatic file rotation and compression to save disk space.

Key Features:
    - Rotating log files that compress automatically
    - Group ownership management for log files
    - Timestamp-based rotating file names
    - Configurable file size limits and retention policies
    - Integration with Python's standard logging module

Classes:
    LoggingConfiguration: Configuration dataclass for logging settings
    RotatingCompressingGroupFileHandler: Custom file handler with rotation and compression

Functions:
    configure_logging: Configure the root logger with handlers

Reference:
    https://docs.python.org/3/howto/logging-cookbook.html
"""

import gzip
import logging
import logging.handlers
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from rich.console import Console
    from rich.logging import RichHandler
except ImportError:
    RichHandler = None  # type: ignore


@dataclass
class LoggingConfiguration:
    """Configuration dataclass for logging setup.

    This dataclass holds all configuration parameters needed to set up logging
    with rotating, compressing file handlers. It supports both stderr output
    and file-based logging with automatic rotation and compression.

    Attributes:
        file_path: Optional path to the log file. If None, only stderr logging
                  is configured. Defaults to None.
        max_log_file_size: Maximum log file size in bytes before rotation occurs.
                          Defaults to 1 billion bytes (1 GB).
        keep_file_count: Number of rotated (and compressed) log files to retain.
                        Older files are deleted. Defaults to 5.
        date_format: Format string for timestamps in log messages. Used for both
                    message timestamps and rotated file names. Defaults to
                    "%Y-%m-%d %H:%M:%S".
        level: Logging level for the root logger. Valid values: "DEBUG", "INFO",
              "WARNING", "ERROR", "CRITICAL". Defaults to "WARNING".
    """

    file_path: Optional[str] = None
    max_log_file_size: int = 1_000_000_000
    keep_file_count: int = 5
    date_format: str = "%Y-%m-%d %H:%M:%S"
    level: str = "WARNING"


class RotatingCompressingGroupFileHandler(logging.handlers.RotatingFileHandler):
    """
    A rotating file handler that compresses rotated logs and sets group ownership.

    This handler extends `RotatingFileHandler` with the following features:
    - Rotated log files are compressed using gzip.
    - The ownership of new and rotated log files is set to a specified UNIX group.
    - Rotated log files are named with a timestamp.
    """

    def _create_log_file(self) -> None:
        """
        Create the log file and its parent directories with the correct permissions.

        The parent directory must already exist. If the log file does not exist, it is
        created with mode 0o664, and ownership is set to the current user with the
        parent directory's group.

        Raises:
            FileNotFoundError: If the parent directory does not exist.
        """

        if not self.file_path.parent.exists():
            raise FileNotFoundError(f"Parent directory {self.file_path.parent} does not exist")

        if not self.file_path.exists():
            self.file_path.touch()
            self.file_path.chmod(0o664)
            shutil.chown(path=self.file_path, user=self.file_path.owner(), group=self.file_path.parent.group())

    def __init__(
        self,
        file_path: Path,
        date_fmt: str,
        mode="a",
        max_bytes=0,
        backup_count=0,
        encoding="utf-8",
        delay=False,
    ):
        """
        Initialize the rotating, compressing file handler.

        Args:
            file_path: The path to the log file. Parent directory must exist.
            date_fmt: The format string for timestamps in rotated log file names.
            mode: The file open mode (default: "a" for append).
            maxBytes: The maximum log file size in bytes before rotation (default: 0, no limit).
            backupCount: The number of backup rotated logs to keep (default: 0).
            encoding: The file encoding (default: "utf-8").
            delay: If True, file opening is deferred until the first log message (default: False).
        """

        self.file_path = file_path
        self.date_fmt = date_fmt
        self._create_log_file()
        super().__init__(
            file_path, mode=mode, maxBytes=max_bytes, backupCount=backup_count, encoding=encoding, delay=delay
        )

    def my_namer(self) -> str:
        """
        Generate the name for a rotated log file.

        The name is created by appending a timestamp and the `.gz` extension to
        the base log file name.

        Returns:
            str: The destination path for the rotated log file with appended
                 timestamp and `.gz` suffix.
        """
        timestamp = datetime.now().strftime(self.date_fmt)
        return f"{self.file_path}-{timestamp}.gz"

    def my_rotator(self, src: str, dst: str) -> None:
        """
        Compress the source log file to the destination path and set ownership.

        The source file is read in binary mode and compressed using gzip to the
        destination file. The destination file ownership is set to the parent
        directory's group, and the source file is deleted after compression.

        Args:
            src: The path to the source log file to rotate.
            dst: The path to the destination compressed log file (.gz).
        """
        with open(src, "rb") as source_file:
            with gzip.GzipFile(dst, "wb") as dst_file:
                shutil.copyfileobj(source_file, dst_file)
        shutil.chown(path=dst, group=self.file_path.parent.group())
        os.remove(src)

    def doRollover(self):  # Name decided by logging lib
        """
        Performs the log file rotation, compression, and cleanup.

        This method is called by the logging system when the current log file
        reaches its maximum size. It handles the rotation, compresses the old
        log file, manages the number of backup files, and ensures the new log
        file has the correct permissions and ownership.
        """
        if self.stream:
            self.stream.close()
            self.stream = None  # type: ignore

        log_archive_list = sorted(Path(".").glob("*.gz"))
        if self.backupCount > 0:
            # Delete undesired log files
            n_logs_to_delete = len(log_archive_list) - self.backupCount
            for i in range(n_logs_to_delete):
                if os.path.exists(log_archive_list[i]):
                    os.remove(log_archive_list[i])

            dst_file_name = self.my_namer()
            self.my_rotator(self.file_path.as_posix(), dst_file_name)

        self.file_path.touch(mode=0o664, exist_ok=True)
        shutil.chown(path=self.file_path, group=self.file_path.parent.group())


def configure_logging(logging_conf: LoggingConfiguration) -> None:
    """
    Configures the root logger based on the provided configuration.

    This function sets up two handlers for the root logger:
    1. A `StreamHandler` that logs to stderr (used if no file provided)
    2. A `RotatingCompressingGroupFileHandler` that logs to a file with
       rotation, compression, and group ownership.

    It clears any existing handlers on the root logger before adding the new ones.

    Args:
        logging_conf: A `LoggingConfiguration` object containing all the
                      necessary settings for logging.
    """

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging_conf.level)

    # configure formatters
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(module)s:%(lineno)d %(funcName)s() - %(message)s",
        datefmt=logging_conf.date_format,
    )

    # if we don't have a log file, log to stderr
    if RichHandler is None:
        stderr_handler = logging.StreamHandler()
        stderr_handler.setFormatter(formatter)
        root_logger.addHandler(stderr_handler)
    else:
        stderr_console = Console(stderr=True)
        root_logger.addHandler(
            RichHandler(
                console=stderr_console,
                rich_tracebacks=True,
                markup=False,
                show_path=True,
                omit_repeated_times=False,
                log_time_format=logging_conf.date_format,
            )
        )

    if logging_conf.file_path:
        file_handler = RotatingCompressingGroupFileHandler(
            file_path=Path(logging_conf.file_path),
            date_fmt=logging_conf.date_format.replace(" ", "_"),
            max_bytes=logging_conf.max_log_file_size,
            backup_count=logging_conf.keep_file_count,
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
