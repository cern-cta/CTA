# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tape file Dao protocol.

This module defines operations for managing tape files, which are archived files stored on tape.
Tape files can be accessed via multiple identifiers (archive ID, disk file ID, or volume ID) and
support retrieval, listing, and deletion operations with streaming support for large result sets.
"""

from collections.abc import Iterator
from typing import Protocol, overload

from cta.sdk.models import TapeFile


class TapeFileDao(Protocol):
    """Data access object protocol for tape file management.

    Provides operations for accessing archived files on tape, including retrieval of all
    copies of a file, listing files on specific tapes or from file lists, and deletion
    of archived files. Supports both standard list and streaming list operations.
    """

    @overload
    def get_all_copies(self, disk_file_id: str, disk_instance_name: str, /) -> tuple[TapeFile, ...]: ...

    @overload
    def get_all_copies(self, archive_id: int, /) -> tuple[TapeFile, ...]: ...

    def get_all_copies(self, *args, **kwargs) -> tuple[TapeFile, ...]:
        """Retrieve all tape copies of a file.

        This method supports two calling conventions:
        1. By archive ID: get_all_copies(archive_id)
        2. By disk file ID and disk instance: get_all_copies(disk_file_id, disk_instance_name)

        Args:
            archive_id: The unique archive identifier (overload 1).
            disk_file_id: The disk file identifier (overload 2).
            disk_instance_name: The disk instance name (overload 2).

        Returns:
            tuple[TapeFile, ...]: A tuple of all TapeFile copies of the archived file.
        """
        ...

    @overload
    def get(self, archive_id: int, /, *, vid: str) -> TapeFile: ...

    @overload
    def get(self, disk_file_id: str, disk_instance_name: str, /, *, vid: str) -> TapeFile: ...

    def get(self, *args, **kwargs) -> TapeFile:
        """Retrieve a specific tape file copy.

        This method supports two calling conventions:
        1. By archive ID and VID: get(archive_id, vid=vid)
        2. By disk file ID, disk instance, and VID: get(disk_file_id, disk_instance_name, vid=vid)

        Args:
            archive_id: The unique archive identifier (overload 1).
            disk_file_id: The disk file identifier (overload 2).
            disk_instance_name: The disk instance name (overload 2).
            vid: The tape volume ID (VID).

        Returns:
            TapeFile: The tape file object containing tape and position information.
        """
        ...

    @overload
    def list(self, *, vid: str) -> Iterator[TapeFile]: ...

    @overload
    def list(self, *, fids_file: str, disk_instance_name: str) -> Iterator[TapeFile]: ...

    def list(self, **kwargs) -> Iterator[TapeFile]:
        """List tape files using various filter criteria.

        This method supports two calling conventions:
        1. By VID: list(vid=vid)
        2. By file list: list(fids_file=file_path, disk_instance_name=instance_name)

        Args:
            vid: Filter by tape volume ID (overload 1).
            fids_file: Path to a file containing file IDs (overload 2).
            disk_instance_name: The disk instance name (overload 2).

        Yields:
            TapeFile: Tape file objects matching the criteria.
        """
        ...

    @overload
    def delete(self, archive_id: int, /, *, vid: str, reason: str) -> None: ...

    @overload
    def delete(self, disk_file_id: str, disk_instance_name: str, /, *, vid: str, reason: str) -> None: ...

    def delete(self, *args, **kwargs) -> None:
        """Delete a tape file.

        This method supports two calling conventions:
        1. By archive ID: delete(archive_id, vid=vid, reason=reason)
        2. By disk file ID: delete(disk_file_id, disk_instance_name, vid=vid, reason=reason)

        Args:
            archive_id: The unique archive identifier (overload 1).
            disk_file_id: The disk file identifier (overload 2).
            disk_instance_name: The disk instance name (overload 2).
            vid: The tape volume ID containing the file.
            reason: The reason for deleting the file.

        Returns:
            None
        """
        ...
