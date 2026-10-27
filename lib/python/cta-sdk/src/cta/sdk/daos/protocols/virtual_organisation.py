# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Virtual organisation Dao protocol.

This module defines operations for managing virtual organisations, which are logical
containers for CTA resources including storage classes, tape pools, and tape media.
Virtual organisations control resource isolation and operational policies.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import VirtualOrganisation


class VirtualOrganisationDao(Protocol):
    """Data access object protocol for virtual organisation management.

    Provides CRUD operations for virtual organisations that serve as logical containers
    for archival resources and control tape drive allocation for archive and retrieve operations.
    """

    def get(self, name: str, /) -> VirtualOrganisation:
        """Retrieve a specific virtual organisation by name.

        Args:
            name: The virtual organisation name.

        Returns:
            VirtualOrganisation: The virtual organisation object containing configuration details.
        """
        ...

    def list(self) -> Iterator[VirtualOrganisation]:
        """List all virtual organisations.

        Yields:
            VirtualOrganisation: A virtual organisation in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a virtual organisation.

        Args:
            name: The name of the virtual organisation to delete.

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
        is_repack_vo: bool = False,
        max_read_drives: int = 1,
        max_write_drives: int = 1,
        max_file_size: int = 1_000_000_000_000,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new virtual organisation.

        Args:
            name: The name of the new virtual organisation.
            disk_instance_name: The disk instance to associate with this virtual organisation.
            is_repack_vo: Whether this is a repack virtual organisation (default: False).
            max_read_drives: Maximum number of drives for retrieve operations (default: 1).
            max_write_drives: Maximum number of drives for archive operations (default: 1).
            max_file_size: Maximum single file size allowed in bytes (default: 1 TB).
            comment: Optional description of the virtual organisation
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
        disk_instance_name: Optional[str] = None,
        is_repack_vo: Optional[bool] = None,
        max_read_drives: Optional[int] = None,
        max_write_drives: Optional[int] = None,
        max_file_size: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing virtual organisation.

        Args:
            name: The name of the virtual organisation to update.
            disk_instance_name: The new disk instance association (optional).
            is_repack_vo: Whether this should be a repack organisation (optional).
            max_read_drives: The new maximum retrieve drives (optional).
            max_write_drives: The new maximum archive drives (optional).
            max_file_size: The new maximum file size in bytes (optional).
            comment: The new virtual organisation comment (optional).

        Returns:
            None
        """
        ...
