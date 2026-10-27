# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Archive route Dao protocol.

This module defines operations for managing archive routes, which determine how files
from specific storage classes are routed to tape pools during archival operations.
Archive routes control which tape pool receives archived data and the priority order
for multiple routes to the same storage class.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import ArchiveRoute, ArchiveRouteType


class ArchiveRouteDao(Protocol):
    """Data access object protocol for archive route management.

    Provides CRUD operations for archive routes that define how files in storage classes
    are mapped to tape pools for archival. Routes are uniquely identified by storage class
    name, route copy_number, and route type (unique or ordered).
    """

    def get(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> ArchiveRoute:
        """Retrieve a specific archive route.

        Args:
            storage_class_name: The storage class associated with the route.
            copy_number: The route copy_number (order within the storage class).
            type: The route type (unique or ordered).

        Returns:
            ArchiveRoute: The archive route object containing route configuration details.
        """
        ...

    def list(self) -> Iterator[ArchiveRoute]:
        """List all archive routes.

        Yields:
            ArchiveRoute: An archive route in the system.
        """
        ...

    def delete(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> None:
        """Delete an archive route.

        Args:
            storage_class_name: The storage class associated with the route.
            copy_number: The route copy_number to delete.
            type: The route type of the route to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        storage_class_name: str,
        copy_number: int,
        type: ArchiveRouteType,
        /,
        *,
        tape_pool_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new archive route.

        Args:
            storage_class_name: The storage class this route applies to.
            copy_number: The route copy_number (determines priority within the storage class).
            type: The route type (unique or ordered).
            tape_pool_name: The tape pool to route archived files to.
            comment: Optional description of the route (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        storage_class_name: str,
        copy_number: int,
        type: ArchiveRouteType,
        /,
        *,
        tape_pool_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing archive route.

        Args:
            storage_class_name: The storage class of the route to update.
            copy_number: The route copy_number to update.
            type: The route type to update.
            tape_pool_name: The new tape pool name (optional).
            comment: The new route comment (optional).

        Returns:
            None
        """
        ...
