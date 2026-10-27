# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Storage class Dao protocol.

This module defines operations for managing storage classes, which define archival policies
including replica counts and virtual organisation assignments. Storage classes determine
how many copies of archived data are maintained on tape.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import StorageClass


class StorageClassDao(Protocol):
    """Data access object protocol for storage class management.

    Provides CRUD operations for storage classes that define archival policies including
    the number of tape replicas to maintain and virtual organisation assignment.
    """

    def get(self, name: str, /) -> StorageClass:
        """Retrieve a specific storage class by name.

        Args:
            name: The storage class name.

        Returns:
            StorageClass: The storage class object containing policy configuration.
        """
        ...

    def list(self) -> Iterator[StorageClass]:
        """List all storage classes.

        Yields:
            StorageClass: A storage class in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a storage class.

        Args:
            name: The name of the storage class to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        virtual_organisation_name: str,
        replica_count: int = 1,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new storage class.

        Args:
            name: The name of the new storage class.
            virtual_organisation_name: The virtual organisation this storage class belongs to.
            replica_count: The number of tape replicas to maintain (default: 1).
            comment: Optional description of the storage class
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
        virtual_organisation_name: Optional[str] = None,
        replica_count: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing storage class.

        Args:
            name: The name of the storage class to update.
            virtual_organisation_name: The new virtual organisation assignment (optional).
            replica_count: The new replica count (optional).
            comment: The new storage class comment (optional).

        Returns:
            None
        """
        ...
