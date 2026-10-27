# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Disk instance space Dao protocol.

This module defines operations for managing disk instance spaces, which represent
logical storage areas within a disk instance with independent free space tracking
and refresh intervals for monitoring available capacity.
"""

from datetime import timedelta
from typing import Iterator, Optional, Protocol, Union

from cta.sdk.models import DiskInstanceSpace


class DiskInstanceSpaceDao(Protocol):
    """Data access object protocol for disk instance space management.

    Provides CRUD operations for disk instance spaces that represent logical storage
    areas with independent free space monitoring and refresh configuration.
    """

    def get(self, name: str, disk_instance_name: str, /) -> DiskInstanceSpace:
        """Retrieve a specific disk instance space.

        Args:
            name: The disk instance space name.
            disk_instance_name: The parent disk instance name.

        Returns:
            DiskInstanceSpace: The disk instance space object containing configuration details.
        """
        ...

    def list(self) -> Iterator[DiskInstanceSpace]:
        """List all disk instance spaces.

        Yields:
            DiskInstanceSpace: A disk instance space in the system.
        """
        ...

    def delete(self, name: str, disk_instance_name: str, /) -> None:
        """Delete a disk instance space.

        Args:
            name: The name of the disk instance space to delete.
            disk_instance_name: The parent disk instance name.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        free_space_url: str,
        refresh_interval: Union[int, timedelta] = 10,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new disk instance space.

        Args:
            name: The name of the new disk instance space.
            disk_instance_name: The parent disk instance.
            free_space_url: The URL or endpoint to query for free space information.
            refresh_interval: Interval for refreshing free space data (seconds or timedelta,
                            default: 10 seconds).
            comment: Optional description of the disk instance space
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        free_space_url: Optional[str] = None,
        refresh_interval: Union[int, timedelta, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing disk instance space.

        Args:
            name: The name of the disk instance space to update.
            disk_instance_name: The parent disk instance name.
            free_space_url: The new free space URL (optional).
            refresh_interval: The new refresh interval in seconds or timedelta (optional).
            comment: The new disk instance space comment (optional).

        Returns:
            None
        """
        ...
