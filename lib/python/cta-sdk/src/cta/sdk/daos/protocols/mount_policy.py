# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Mount policy Dao protocol.

This module defines operations for managing mount policies, which control tape mounting
behavior through configurable priorities and minimum age requirements. Mount policies
determine when tapes should be mounted for archive and retrieve operations.
"""

from datetime import timedelta
from typing import Iterator, Optional, Protocol, Union

from cta.sdk.models import MountPolicy


class MountPolicyDao(Protocol):
    """Data access object protocol for mount policy management.

    Provides CRUD operations for mount policies that control tape mounting behavior
    including priority levels and minimum age requirements for archive and retrieve operations.
    """

    def get(self, name: str, /) -> MountPolicy:
        """Retrieve a specific mount policy by name.

        Args:
            name: The mount policy name.

        Returns:
            MountPolicy: The mount policy object containing configuration details.
        """
        ...

    def list(self) -> Iterator[MountPolicy]:
        """List all mount policies.

        Yields:
            MountPolicy: A mount policy in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a mount policy.

        Args:
            name: The name of the mount policy to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        archive_priority: int = 250,
        min_archive_age: Union[timedelta, int] = 14_400,
        retrieve_priority: int = 150,
        min_retrieve_age: Union[timedelta, int] = 14_400,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new mount policy.

        Args:
            name: The name of the new mount policy.
            archive_priority: Priority level for archive operations (default: 250).
            min_archive_age: Minimum age of files before archiving (seconds or timedelta,
                           default: 14400 seconds / 4 hours).
            retrieve_priority: Priority level for retrieve operations (default: 150).
            min_retrieve_age: Minimum age of archived data before retrieval allowed (seconds or timedelta,
                            default: 14400 seconds / 4 hours).
            comment: Optional description of the mount policy
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
        archive_priority: Optional[int] = None,
        min_archive_age: Union[timedelta, int, None] = None,
        retrieve_priority: Optional[int] = None,
        min_retrieve_age: Union[timedelta, int, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing mount policy.

        Args:
            name: The name of the mount policy to update.
            archive_priority: The new archive priority (optional).
            min_archive_age: The new minimum archive age in seconds or timedelta (optional).
            retrieve_priority: The new retrieve priority (optional).
            min_retrieve_age: The new minimum retrieve age in seconds or timedelta (optional).
            comment: The new mount policy comment (optional).

        Returns:
            None
        """
        ...
