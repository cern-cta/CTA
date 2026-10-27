# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Media type Dao protocol.

This module defines operations for managing media types, which represent physical tape
cartridge specifications including capacity, density codes, and cartridge model details.
Media types define the technical characteristics of tape media that can be used in the system.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import MediaType


class MediaTypeDao(Protocol):
    """Data access object protocol for media type management.

    Provides CRUD operations for media types that define tape cartridge characteristics,
    including capacity, density codes, and physical specifications.
    """

    def get(self, name: str, /) -> MediaType:
        """Retrieve a specific media type by name.

        Args:
            name: The media type name.

        Returns:
            MediaType: The media type object containing cartridge specifications.
        """
        ...

    def list(self) -> Iterator[MediaType]:
        """List all media types.

        Yields:
            MediaType: A media type defined in the system.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a media type.

        Args:
            name: The name of the media type to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        cartridge_type: str,
        capacity: int,
        primary_density_code: int = 0,
        secondary_density_code: int = 0,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new media type.

        Args:
            name: The name of the new media type.
            cartridge_type: The cartridge model or type identifier.
            capacity: The storage capacity in bytes.
            primary_density_code: The primary density code (default: 0).
            secondary_density_code: The secondary density code (default: 0).
            comment: Optional description of the media type
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
        cartridge_type: Optional[str] = None,
        primary_density_code: Optional[int] = None,
        secondary_density_code: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing media type.

        Args:
            name: The name of the media type to update.
            cartridge_type: The new cartridge type (optional).
            primary_density_code: The new primary density code (optional).
            secondary_density_code: The new secondary density code (optional).
            comment: The new media type comment (optional).

        Returns:
            None
        """
        ...
