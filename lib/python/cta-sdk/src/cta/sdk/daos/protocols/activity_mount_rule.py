# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Activity mount rule Dao protocol.

This module defines operations for managing activity mount rules, which determine how tapes
are mounted for users based on disk activity patterns. Activity mount rules enable automatic
tape mounting decisions based on user access patterns and activity levels.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import ActivityMountRule


class ActivityMountRuleDao(Protocol):
    """Data access object protocol for activity mount rule management.

    Provides CRUD operations for activity mount rules that control tape mounting
    decisions based on user activity and disk instance patterns.
    """

    def get(self, user_name: str, disk_instance_name: str, regex: str, /) -> ActivityMountRule:
        """Retrieve a specific activity mount rule.

        Args:
            user_name: The user this rule applies to.
            disk_instance_name: The disk instance this rule applies to.
            regex: The activity pattern regex for this rule.

        Returns:
            ActivityMountRule: The activity mount rule object.
        """
        ...

    def list(self) -> Iterator[ActivityMountRule]:
        """List all activity mount rules.

        Yields:
            ActivityMountRule: An activity mount rule in the system.
        """
        ...

    def delete(self, user_name: str, disk_instance_name: str, regex: str, /) -> None:
        """Delete an activity mount rule.

        Args:
            user_name: The user of the rule to delete.
            disk_instance_name: The disk instance of the rule to delete.
            regex: The activity pattern regex of the rule to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        user_name: str,
        disk_instance_name: str,
        regex: str,
        /,
        *,
        mount_policy_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new activity mount rule.

        Args:
            user_name: The user this rule applies to.
            disk_instance_name: The disk instance this rule applies to.
            regex: The activity pattern regex to match.
            mount_policy_name: The mount policy to apply for matching activity.
            comment: Optional description of the activity mount rule
                    (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        user_name: str,
        disk_instance_name: str,
        regex: str,
        /,
        mount_policy_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing activity mount rule.

        Args:
            user_name: The user of the rule to update.
            disk_instance_name: The disk instance of the rule to update.
            regex: The activity pattern regex of the rule to update.
            mount_policy_name: The new mount policy name (optional).
            comment: The new activity mount rule comment (optional).

        Returns:
            None
        """
        ...
