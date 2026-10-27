# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Provider protocols for library controllers.

Two protocols are exposed:

* [LibraryControllerProvider][ctaops.sdk.controller_providers.protocols.library.LibraryControllerProvider]
  — yields minimal
  [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController]
  instances (ordinal-only addressing, no logical-name resolution).
* [ManagedLibraryControllerProvider][ctaops.sdk.controller_providers.protocols.library.ManagedLibraryControllerProvider]
  — yields richer
  [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController]
  instances (logical-name resolution, lookup by attached drive name,
  context-managed `mount(...)`, `label_tape(...)`).

The `get_*` methods raise
[ResourceNotFoundError][ctaops.sdk.exceptions.ResourceNotFoundError] when
no match is visible; the `find_*` variants return `None` instead.
"""

from typing import Optional, Protocol

from ctaops.sdk.controllers.protocols.library import LibraryController, ManagedLibraryController


class LibraryControllerProvider(Protocol):
    """Locate
    [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController]
    instances by SCSI serial or logical name."""

    def get_library_controller(
        self,
        serial_number: str,
    ) -> LibraryController:
        """Return the library controller matching `serial_number`.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...

    def find_library_controller(
        self,
        serial_number: str,
    ) -> Optional[LibraryController]:
        """Return the library controller matching `serial_number`, or `None`."""
        ...

    def get_library_controller_by_name(
        self,
        name: str,
    ) -> LibraryController:
        """Return the library controller matching the logical ctaops `name`.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...

    def find_library_controller_by_name(
        self,
        name: str,
    ) -> Optional[LibraryController]:
        """Return the library controller matching `name`, or `None`."""
        ...


class ManagedLibraryControllerProvider(Protocol):
    """Locate
    [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController]
    instances by SCSI serial or logical name."""

    def get_library_controller(
        self,
        serial_number: str,
    ) -> ManagedLibraryController:
        """Return the managed library controller matching `serial_number`.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...

    def find_library_controller(
        self,
        serial_number: str,
    ) -> Optional[ManagedLibraryController]:
        """Return the managed library controller matching `serial_number`, or `None`."""
        ...

    def get_library_controller_by_name(
        self,
        name: str,
    ) -> ManagedLibraryController:
        """Return the managed library controller matching `name`.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...

    def find_library_controller_by_name(
        self,
        name: str,
    ) -> Optional[ManagedLibraryController]:
        """Return the managed library controller matching `name`, or `None`."""
        ...

    def get_library_controller_from_drive_name(
        self,
        drive_name: str,
    ) -> ManagedLibraryController:
        """Return the managed library controller of the library that hosts drive `drive_name`.

        `drive_name` is the logical ctaops name (`CTAOPS_NAME`) of the
        drive — the parent library is resolved through the local drive
        repository.

        Raises:
            ResourceNotFoundError: If no drive with that name is visible, or
                if its parent library cannot be located.
        """
        ...

    def find_library_controller_from_drive_name(
        self,
        drive_name: str,
    ) -> Optional[ManagedLibraryController]:
        """Return the managed library controller hosting drive `drive_name`, or `None`."""
        ...
