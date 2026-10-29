# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Disk instance Dao protocol.

This module defines operations for managing disk instances, which are logical disk storage
systems used as buffer space for staging files to and from tape. Disk instances represent
physical or virtual storage infrastructure endpoints.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import DiskInstance


class DiskInstanceDao(Protocol):
    """Data access object protocol for disk instance management.

    Provides CRUD operations for disk instances that serve as staging areas for
    archival and retrieval operations.
    """

    def get(self, name: str, /) -> DiskInstance:
        """Retrieve a specific disk instance by name.

        Args:
            name: The disk instance name.

        Returns:
            DiskInstance: The disk instance object containing configuration details.
        """
        ...

    def list(self) -> Iterator[DiskInstance]:
        """List all disk instances.

        Yields:
            DiskInstance: A disk instance in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a disk instance.

        Args:
            name: The name of the disk instance to delete.

        Returns:
            None
        """
        ...

    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        """Create a new disk instance.

        Args:
            name: The name of the new disk instance.
            comment: Optional description of the disk instance
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        """Update an existing disk instance.

        Args:
            name: The name of the disk instance to update.
            comment: The new disk instance comment (optional).

        Returns:
            None
        """
        ...
