# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Admin user Dao protocol.

This module defines operations for managing admin users within CTA. It exposes
the protocol interface that DAO implementations must follow to create, read,
update and delete `AdminUser` objects.
"""

from collections.abc import Iterator
from typing import Optional, Protocol

from cta.sdk.models import AdminUser


class AdminUserDao(Protocol):
    """Data access object protocol for admin user management.

    Provides CRUD operations for `AdminUser` objects used to represent administrative
    accounts and associated metadata (e.g., comments).
    """

    def get(self, name: str, /) -> AdminUser:
        """Retrieve an admin user by name.

        Args:
            name: The name of the admin user to retrieve.

        Returns:
            AdminUser: The admin user matching the provided name.
        """
        ...

    def list(self) -> Iterator[AdminUser]:
        """List all admin users.

        Yields:
            AdminUser: An admin user in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete an admin user.

        Args:
            name: The name of the admin user to delete.

        Returns:
            None
        """
        ...

    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        """Create a new admin user.

        Args:
            name: The name of the admin user to create.
            comment: Optional comment for the admin user (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        """Update an existing admin user.

        Args:
            name: The name of the admin user to update.
            comment: Optional new comment for the admin user.

        Returns:
            None
        """
        ...
