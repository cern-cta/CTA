# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Disk system Dao protocol.

This module defines operations for managing disk systems, which define archival intake
policies for specific disk instance spaces. Disk systems control file ingestion from disk
to tape based on file patterns, retention targets, and polling intervals.
"""

from datetime import timedelta
from typing import Iterator, Optional, Protocol, Union

from cta.sdk.models import DiskSystem


class DiskSystemDao(Protocol):
    """Data access object protocol for disk system management.

    Provides CRUD operations for disk systems that define archival policies including
    file pattern matching, minimum free space requirements, and polling intervals.
    """

    def get(self, name: str, /) -> DiskSystem:
        """Retrieve a specific disk system by name.

        Args:
            name: The disk system name.

        Returns:
            DiskSystem: The disk system object containing policy configuration.
        """
        ...

    def list(self) -> Iterator[DiskSystem]:
        """List all disk systems.

        Yields:
            DiskSystem: A disk system in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a disk system.

        Args:
            name: The name of the disk system to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        disk_instance_name: str,
        disk_instance_space_name: str,
        file_regex: str,
        desired_min_free_space: int = 1_000_000_000_000,
        sleep_time: Union[int, timedelta] = 3600,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new disk system.

        Args:
            name: The name of the new disk system.
            disk_instance_name: The disk instance this system manages.
            disk_instance_space_name: The disk instance space to monitor.
            file_regex: Regular expression pattern for files to be archived.
            desired_min_free_space: Minimum free space target in bytes (default: 1 TB).
            sleep_time: Polling interval for checking free space (seconds or timedelta,
                       default: 3600 seconds / 1 hour).
            comment: Optional description of the disk system
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        name: str,
        /,
        *,
        file_regex: Optional[str] = None,
        desired_min_free_space: Optional[int] = None,
        sleep_time: Union[int, timedelta, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing disk system.

        Args:
            name: The name of the disk system to update.
            file_regex: The new file pattern regex (optional).
            desired_min_free_space: The new minimum free space target (optional).
            sleep_time: The new polling interval in seconds or timedelta (optional).
            comment: The new disk system comment (optional).

        Returns:
            None
        """
        ...
