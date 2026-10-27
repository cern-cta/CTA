# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tape pool Dao protocol.

This module defines operations for managing tape pools, which are collections of tapes
organized by virtual organisation and used as destinations for archival operations.
Tape pools support encryption, supply pool relationships, and writable tape management.
"""

from typing import Iterable, Iterator, Optional, Protocol

from cta.sdk.models import TapePool


class TapePoolDao(Protocol):
    """Data access object protocol for tape pool management.

    Provides CRUD operations for tape pools that group tapes for archival purposes.
    Supports encryption configuration, supply pool relationships, and writable tape
    count management.
    """

    def get(self, name: str, /) -> TapePool:
        """Retrieve a specific tape pool by name.

        Args:
            name: The tape pool name.

        Returns:
            TapePool: The tape pool object containing configuration details.
        """
        ...

    def list(
        self,
        /,
        *,
        virtual_organisation_name: Optional[str] = None,
        encrypted: Optional[bool] = None,
    ) -> Iterator[TapePool]:
        """List tape pools, optionally filtered by virtual organisation and encryption status.

        Args:
            virtual_organisation_name: Filter by virtual organisation name (optional).
            encrypted: Filter by encryption status (True for encrypted, False for
                      unencrypted, None for all pools). Default: None.

        Yields:
            TapePool: Tape pool objects matching the criteria.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a tape pool.

        Args:
            name: The name of the tape pool to delete.

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
        min_writable_tape_count: int = 1,
        encryption_key_name: Optional[str] = None,
        supply_tape_pool_names: Optional[Iterable[str]] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new tape pool.

        Args:
            name: The name of the new tape pool.
            virtual_organisation_name: The virtual organisation this pool belongs to.
            min_writable_tape_count: Minimum number of writable tapes to maintain
                                    (default: 1).
            encryption_key_name: The name of the encryption key for the pool (optional).
            supply_tape_pool_names: List of supply tape pool names to replenish this pool
                                   (optional).
            comment: Optional description of the tape pool
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
        min_writable_tape_count: Optional[int] = None,
        supply_tape_pool_names: Optional[Iterable[str]] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing tape pool.

        Args:
            name: The name of the tape pool to update.
            virtual_organisation_name: The new virtual organisation (optional).
            min_writable_tape_count: The new minimum writable tape count (optional).
            supply_tape_pool_names: The new list of supply tape pool names (optional).
            comment: The new tape pool comment (optional).

        Returns:
            None
        """
        ...

    def remove_encryption(self, name: str, /) -> None:
        """Remove encryption configuration from a tape pool.

        Args:
            name: The name of the tape pool to modify.

        Returns:
            None
        """
        ...

    def set_encryption_key_name(self, name: str, /, *, encryption_key_name: str) -> None:
        """Set the encryption key for a tape pool.

        Args:
            name: The name of the tape pool to modify.
            encryption_key_name: The encryption key name to assign.

        Returns:
            None
        """
        ...
