# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Logical library Dao protocol.

This module defines operations for managing logical libraries, which are virtual groupings
of tapes that map to a physical library. Logical libraries organize tape media and control
logical access patterns independent of physical arrangements.
"""

from typing import Iterator, Optional, Protocol

from cta.sdk.models import LogicalLibrary, LogicalLibraryState


class LogicalLibraryDao(Protocol):
    """Data access object protocol for logical library management.

    Provides CRUD operations for logical libraries that organize and group tapes
    within a physical library structure. Logical libraries can be enabled or disabled
    to control access to their tapes.
    """

    def get(self, name: str, /) -> LogicalLibrary:
        """Retrieve a specific logical library by name.

        Args:
            name: The logical library name.

        Returns:
            LogicalLibrary: The logical library object containing configuration details.
        """
        ...

    def list(self, *, state: Optional[LogicalLibraryState] = None) -> Iterator[LogicalLibrary]:
        """List logical libraries, optionally filtered by disabled status.

        Args:
            state: Filter by state status (True for state, False for enabled,
                     None for all libraries). Default: None.

        Yields:
            LogicalLibrary: A logical library matching the criteria.
        """
        ...

    def delete(self, name: str, /) -> None:
        """Delete a logical library.

        Args:
            name: The name of the logical library to delete.

        Returns:
            None
        """
        ...

    def create(
        self,
        name: str,
        /,
        *,
        physical_library_name: str,
        state: LogicalLibraryState = LogicalLibraryState.ENABLED,
        comment: str = "From python cta-sdk",
    ) -> None:
        """Create a new logical library.

        Args:
            name: The name of the new logical library.
            physical_library_name: The physical library this logical library maps to.
            disabled: Whether the logical library is disabled (default: False).
            comment: Optional description of the logical library
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
        physical_library_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        """Update an existing logical library.

        Args:
            name: The name of the logical library to update.
            physical_library_name: The new physical library mapping (optional).
            comment: The new logical library comment (optional).

        Returns:
            None
        """
        ...

    def disable(self, name: str, /, *, reason: str) -> None:
        """Disable a logical library.

        Args:
            name: The name of the logical library to disable.
            reason: The reason for disabling the logical library.

        Returns:
            None
        """
        ...

    def enable(self, name: str, /) -> None:
        """Enable a logical library.

        Args:
            name: The name of the logical library to enable.

        Returns:
            None
        """
        ...
