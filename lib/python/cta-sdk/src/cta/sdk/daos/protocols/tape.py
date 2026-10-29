# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tape Dao protocol.

This module defines operations for managing tapes in CTA, including
creating, updating, deleting, and querying tapes, as well as reclaiming
space and changing tape state.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import Tape, TapeState, VerificationStatus


class TapeDao(Protocol):
    """Data access object protocol for tape management.

    Provides operations for creating and managing physical tapes, including
    querying by various attributes such as media type, vendor, logical library,
    tape pool, and state. Supports reclaiming space, changing tape state, and
    updating tape metadata.
    """

    def get(self, vid: str, /) -> Tape:
        """Retrieve a tape by volume ID.

        Args:
            vid: The volume identifier (VID) of the tape to retrieve.

        Returns:
            Tape: The tape object matching the given VID.
        """
        ...

    def list(
        self,
        /,
        *,
        media_type_name: Optional[str] = None,
        vendor: Optional[str] = None,
        logical_library_name: Optional[str] = None,
        tape_pool_name: Optional[str] = None,
        virtual_organisation_name: Optional[str] = None,
        full: Optional[bool] = None,
        fxid_file: Optional[str] = None,
        state: Optional[TapeState] = None,
        purchase_order: Optional[str] = None,
        physical_library_name: Optional[str] = None,
        missing_file_copies: bool = False,
    ) -> Iterator[Tape]:
        """List tapes with optional filtering.

        Really heavy without any filter, you should avoid if not necessary.

        Args:
            media_type_name: Filter by media type name (optional).
            vendor: Filter by vendor name (optional).
            logical_library_name: Filter by logical library name (optional).
            tape_pool_name: Filter by tape pool name (optional).
            virtual_organisation_name: Filter by virtual organisation name (optional).
            full: Filter by whether the tape is full (optional).
            fxid_file: Path to a file containing a list of FXIDs to filter by (optional).
            state: Filter by tape state (optional).
            purchase_order: Filter by purchase order reference (optional).
            physical_library_name: Filter by physical library name (optional).
            missing_file_copies: If True, return only tapes with missing file copies
                                 (default: False).

        Yields:
            Tape: Tape objects matching the given filters.
        """
        ...

    def delete(self, vid: str, /) -> None:
        """Delete a tape by volume ID.

        Args:
            vid: The volume identifier (VID) of the tape to delete.

        Returns:
            None
        """
        ...

    def reclaim(self, vid: str, /) -> None:
        """Reclaim space on a tape by removing files that no longer have valid archive entries.

        Args:
            vid: The volume identifier (VID) of the tape to reclaim.

        Returns:
            None
        """
        ...

    def create(
        self,
        vid: str,
        /,
        *,
        media_type_name: str,
        vendor: str,
        logical_library_name: str,
        tape_pool_name: str,
        full: bool = False,
        purchase_order: Optional[str] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new tape.

        Args:
            vid: The volume identifier (VID) for the new tape.
            media_type_name: The name of the media type for the tape.
            vendor: The vendor of the tape.
            logical_library_name: The name of the logical library to assign the tape to.
            tape_pool_name: The name of the tape pool to assign the tape to.
            full: Whether the tape is already full (default: False).
            purchase_order: An optional purchase order reference.
            comment: An optional comment for the tape
                     (default: "From python cta-sdk").

        Returns:
            None
        """
        ...

    def update(
        self,
        vid: str,
        /,
        *,
        media_type_name: Optional[str] = None,
        vendor: Optional[str] = None,
        logical_library_name: Optional[str] = None,
        tape_pool_name: Optional[str] = None,
        verification_status: Optional[VerificationStatus] = None,
        full: Optional[bool] = None,
        purchase_order: Optional[str] = None,
        dirty: Optional[bool] = None,
        encryption_key_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update a tape's metadata.

        Args:
            vid: The volume identifier (VID) of the tape to update.
            media_type_name: New media type name (optional).
            vendor: New vendor name (optional).
            logical_library_name: New logical library name (optional).
            tape_pool_name: New tape pool name (optional).
            verification_status: New verification status (optional).
            full: Whether the tape is full (optional).
            purchase_order: New purchase order reference (optional).
            dirty: Whether the tape is marked as dirty (optional).
            comment: New comment for the tape (optional).

        Returns:
            None
        """
        ...

    def change_state(self, vid: str, /, *, state: TapeState, reason: str) -> None:
        """Change the state of a tape.

        Args:
            vid: The volume identifier (VID) of the tape.
            state: The new state to assign to the tape.
            reason: The reason for the state change.

        Returns:
            None
        """
        ...
