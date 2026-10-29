# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Physical library Dao protocol.

This module defines operations for managing physical libraries, which are actual library
hardware installations that house tape cartridges and drives. Physical libraries contain
physical storage slots for cartridges and drive slots for tape drives.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import PhysicalLibrary, PhysicalLibraryState


class PhysicalLibraryDao(Protocol):
    """Data access object protocol for physical library management.

    Provides CRUD operations for physical library hardware definitions, including
    manufacturer information, slot counts, monitoring URLs, and operational status.
    Physical libraries can be enabled or disabled to control access.
    """

    def get(self, name: str, /) -> PhysicalLibrary:
        """Retrieve a specific physical library by name.

        Args:
            name: The physical library name.

        Returns:
            PhysicalLibrary: The physical library object containing hardware specifications.
        """
        ...

    def list(self, *, state: Optional[PhysicalLibraryState] = None) -> Iterator[PhysicalLibrary]:
        """List physical libraries, optionally filtered by disabled status.

        Args:
            disabled: Filter by disabled status (True for disabled, False for enabled,
                     None for all libraries). Default: None.

        Yields:
            PhysicalLibrary: A physical library matching the criteria.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a physical library.

        Args:
            name: The name of the physical library to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        manufacturer: str,
        model: str,
        physical_cartridge_slots: int,
        physical_drive_slots: int,
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        type: Optional[str] = None,
        location: Optional[str] = None,
        available_cartridge_slots: Optional[int] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new physical library.

        Args:
            name: The name of the new physical library.
            manufacturer: The library hardware manufacturer.
            model: The library hardware model number.
            physical_cartridge_slots: The total number of physical cartridge storage slots.
            physical_drive_slots: The total number of physical drive slots.
            gui_url: URL to the library management GUI (optional).
            webcam_url: URL to the library webcam feed (optional).
            type: The library type classification (optional).
            location: The physical location of the library (optional).
            available_cartridge_slots: The number of available cartridge slots (optional).
            comment: Optional description of the physical library
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
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        location: Optional[str] = None,
        physical_cartridge_slots: Optional[int] = None,
        available_cartridge_slots: Optional[int] = None,
        physical_drive_slots: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing physical library.

        Args:
            name: The name of the physical library to update.
            gui_url: The new GUI URL (optional).
            webcam_url: The new webcam URL (optional).
            location: The new physical location (optional).
            physical_cartridge_slots: The new total cartridge slot count (optional).
            available_cartridge_slots: The new available cartridge slot count (optional).
            physical_drive_slots: The new physical drive slot count (optional).
            comment: The new physical library comment (optional).

        Returns:
            None
        """
        ...

    def disable(self, name: str, /, *, reason: str) -> None:
        """Disable a physical library.

        Args:
            name: The name of the physical library to disable.
            reason: The reason for disabling the physical library.

        Returns:
            None
        """
        ...

    def enable(self, name: str, /) -> None:
        """Enable a physical library.

        Args:
            name: The name of the physical library to enable.

        Returns:
            None
        """
        ...
