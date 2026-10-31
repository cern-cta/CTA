# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Recycle bin tape file Dao protocol.

This module defines operations for managing recycle bin tape files, which are files that have
been deleted and placed in a recycle bin for recovery. Supports querying deleted files by
various criteria including volume, archive ID, virtual organisation, and deletion time ranges.
"""

from collections.abc import Iterator
from datetime import datetime
from typing import Optional, Protocol, Union, overload

from cta.sdk.models import RecycleBinTapeFile


class RecycleBinTapeFileDao(Protocol):
    """Data access object protocol for recycle bin tape file management.

    Provides operations for accessing deleted tape files in the recycle bin, including
    retrieval of all copies, filtering by virtual organisation and deletion time, and
    streaming support for large result sets.
    """

    @overload
    def get_all_copies(self, disk_file_id: str, disk_instance_name: str, /) -> tuple[RecycleBinTapeFile, ...]: ...

    @overload
    def get_all_copies(self, archive_id: int, /) -> tuple[RecycleBinTapeFile, ...]: ...

    def get_all_copies(self, *args, **kwargs) -> tuple[RecycleBinTapeFile, ...]:
        """Retrieve all recycle bin copies of a deleted file.

        This method supports two calling conventions:
        1. By archive ID: get_all_copies(archive_id)
        2. By disk file ID and disk instance: get_all_copies(disk_file_id, disk_instance_name)

        Args:
            archive_id: The unique archive identifier (overload 1).
            disk_file_id: The disk file identifier (overload 2).
            disk_instance_name: The disk instance name (overload 2).

        Returns:
            tuple[RecycleBinTapeFile, ...]: A tuple of all recycle bin copies of the deleted file.
        """
        ...

    @overload
    def get(self, archive_id: int, /, *, vid: str) -> RecycleBinTapeFile: ...

    @overload
    def get(self, disk_file_id: str, disk_instance_name: str, /, *, vid: str) -> RecycleBinTapeFile: ...

    def get(self, *args, **kwargs) -> RecycleBinTapeFile:
        """Retrieve a specific recycle bin file copy.

        This method supports two calling conventions:
        1. By archive ID and VID: get(archive_id, vid=vid)
        2. By disk file ID, disk instance, and VID: get(disk_file_id, disk_instance_name, vid=vid)

        Args:
            archive_id: The unique archive identifier (overload 1).
            disk_file_id: The disk file identifier (overload 2).
            disk_instance_name: The disk instance name (overload 2).
            vid: The tape volume ID (VID).

        Returns:
            RecycleBinTapeFile: The recycle bin tape file object.
        """
        ...

    @overload
    def list(
        self,
        *,
        vid: str,
        virtual_organisation_name: Optional[str] = None,
        min_deletion_time: Union[datetime, int, None] = None,
        max_deletion_time: Union[datetime, int, None] = None,
    ) -> Iterator[RecycleBinTapeFile]: ...

    @overload
    def list(
        self,
        *,
        fids_file: str,
        disk_instance_name: str,
        virtual_organisation_name: Optional[str] = None,
        min_deletion_time: Union[datetime, int, None] = None,
        max_deletion_time: Union[datetime, int, None] = None,
    ) -> Iterator[RecycleBinTapeFile]: ...

    @overload
    def list(
        self,
        *,
        virtual_organisation_name: str,
        min_deletion_time: Union[datetime, int, None] = None,
        max_deletion_time: Union[datetime, int, None] = None,
    ) -> Iterator[RecycleBinTapeFile]: ...

    def list(self, **kwargs) -> Iterator[RecycleBinTapeFile]:
        """List recycle bin files using various filter criteria.

        This method supports three calling conventions:
        1. By VID: list(vid=vid, virtual_organisation_name=name, min_deletion_time=t1, max_deletion_time=t2)
        2. By file list: list(fids_file=path, disk_instance_name=name, virtual_organisation_name=name, ...)
        3. By virtual organisation: list(virtual_organisation_name=name, min_deletion_time=t1, max_deletion_time=t2)

        Args:
            vid: Filter by tape volume ID (overload 1).
            fids_file: Path to a file containing file IDs (overload 2).
            disk_instance_name: The disk instance name (overload 2).
            virtual_organisation_name: Filter by virtual organisation name (optional, overloads 1 & 2;
                                       required for overload 3).
            min_deletion_time: Minimum deletion timestamp (datetime or epoch seconds) (optional).
            max_deletion_time: Maximum deletion timestamp (datetime or epoch seconds) (optional).

        Yields:
            RecycleBinTapeFile: Recycle bin file objects matching the criteria.
        """
        ...
