# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""User mount rule Dao protocol.

This module defines operations for managing user mount rules, which determine how tapes
are mounted for individual users on specific disk instances. User mount rules apply
policies to control tape access behavior on a per-user basis.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import UserMountRule


class UserMountRuleDao(Protocol):
    """Data access object protocol for user mount rule management.

    Provides CRUD operations for user mount rules that control tape mounting
    decisions for individual users on disk instances.
    """

    def get(self, user_name: str, disk_instance_name: str, /) -> UserMountRule:
        """Retrieve a specific user mount rule.

        Args:
            user_name: The user this rule applies to.
            disk_instance_name: The disk instance this rule applies to.

        Returns:
            UserMountRule: The user mount rule object.
        """
        ...

    def list(self) -> Iterator[UserMountRule]:
        """List all user mount rules.

        Yields:
            UserMountRule: User mount rule objects in the system.
        """
        ...

    def delete(self, user_name: str, disk_instance_name: str, /) -> None:
        """Delete a user mount rule.

        Args:
            user_name: The user of the rule to delete.
            disk_instance_name: The disk instance of the rule to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        user_name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new user mount rule.

        Args:
            user_name: The user this rule applies to.
            disk_instance_name: The disk instance this rule applies to.
            mount_policy_name: The mount policy to apply to the user.
            comment: Optional description of the user mount rule
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        user_name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing user mount rule.

        Args:
            user_name: The user of the rule to update.
            disk_instance_name: The disk instance of the rule to update.
            mount_policy_name: The new mount policy name (optional).
            comment: The new user mount rule comment (optional).

        Returns:
            None
        """
        ...
