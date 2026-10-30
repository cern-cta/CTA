# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Drive Dao protocol.

This module defines operations for managing tape drives in CTA, including
retrieving drive state, listing drives, updating metadata, and controlling
drive status (up/down).
"""

from collections.abc import Iterator
from typing import Optional, Protocol

from cta.sdk.models import Drive


class DriveDao(Protocol):
    """Data access object protocol for tape drive management.

    Provides operations for querying and managing tape drives, including
    retrieving individual drives, listing all drives, updating drive comments,
    and controlling drive operational state.
    """

    def get(self, name: str, /) -> Drive:
        """Retrieve a tape drive by name.

        Args:
            name: The name of the tape drive to retrieve.

        Returns:
            Drive: The drive object matching the given name.
        """
        ...

    def list(self, /, *, all: bool = False) -> Iterator[Drive]:
        """List tape drives.

        Args:
            all: If True, include drives from all logical libraries.
                 If False, only return drives visible to the current instance
                 (default: False).

        Yields:
            Drive: A tape drive in the system.
        """
        ...

    def delete(self, name: str, /, *, force: bool = False) -> None:
        """Delete a tape drive.

        Args:
            name: The name of the tape drive to delete.
            force: If True, force deletion even if the drive is in use
                   (default: False).

        Returns:
            None
        """
        ...

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        """Update a tape drive's metadata.

        Args:
            name: The name of the tape drive to update.
            comment: A comment to associate with the drive (optional).

        Returns:
            None
        """
        ...

    def down(self, name: str, /, *, reason: str, force: bool = False) -> None:
        """Set a tape drive to the down state.

        Args:
            name: The name of the tape drive to bring down.
            reason: The reason for bringing the drive down.
            force: If True, force the drive down even if it is currently
                   processing a tape (default: False).

        Returns:
            None
        """
        ...

    def up(self, name: str, /, *, reason: Optional[str] = None) -> None:
        """Set a tape drive to the up state.

        Args:
            name: The name of the tape drive to bring up.
            reason: An optional reason for bringing the drive up.

        Returns:
            None
        """
        ...
