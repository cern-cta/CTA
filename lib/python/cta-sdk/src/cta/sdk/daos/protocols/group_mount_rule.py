# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Group mount rule Dao protocol.

This module defines operations for managing group mount rules, which determine how tapes
are mounted for user groups on specific disk instances. Group mount rules apply policies
to all members of a user group for consistent tape access behavior.
"""

from collections.abc import Iterator
from typing import Optional, Protocol

from cta.sdk.models import GroupMountRule


class GroupMountRuleDao(Protocol):
    """Data access object protocol for group mount rule management.

    Provides CRUD operations for group mount rules that control tape mounting
    decisions for user groups on disk instances.
    """

    def get(self, group_name: str, disk_instance_name: str, /) -> GroupMountRule:
        """Retrieve a specific group mount rule.

        Args:
            group_name: The user group this rule applies to.
            disk_instance_name: The disk instance this rule applies to.

        Returns:
            GroupMountRule: The group mount rule object.
        """
        ...

    def list(self) -> Iterator[GroupMountRule]:
        """List all group mount rules.

        Yields:
            GroupMountRule: A group mount rule in the system.
        """
        ...

    def delete(self, group_name: str, disk_instance_name: str, /) -> None:
        """Delete a group mount rule.

        Args:
            group_name: The user group of the rule to delete.
            disk_instance_name: The disk instance of the rule to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        group_name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new group mount rule.

        Args:
            group_name: The user group this rule applies to.
            disk_instance_name: The disk instance this rule applies to.
            mount_policy_name: The mount policy to apply to the group.
            comment: Optional description of the group mount rule
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        group_name: str,
        disk_instance_name: str,
        /,
        *,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing group mount rule.

        Args:
            group_name: The user group of the rule to update.
            disk_instance_name: The disk instance of the rule to update.
            mount_policy_name: The new mount policy name (optional).
            comment: The new group mount rule comment (optional).

        Returns:
            None
        """
        ...
