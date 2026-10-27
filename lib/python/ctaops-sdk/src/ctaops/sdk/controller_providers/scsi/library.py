# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Providers for SCSI-backed library controllers.

A controller provider is the recommended entry point for callers: it
enumerates every locally-visible medium changer and produces controller
instances that share a single pyudev `Context`. Two providers are
exposed:

* [ScsiLibraryControllerProvider][ctaops.sdk.controller_providers.scsi.library.ScsiLibraryControllerProvider]
  — yields minimal
  [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController]
  instances (ordinal-only addressing, no logical-name resolution).
* [ScsiManagedLibraryControllerProvider][ctaops.sdk.controller_providers.scsi.library.ScsiManagedLibraryControllerProvider]
  — yields fully-wired
  [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController]
  instances. Each controller resolves drive/library metadata through
  local SCSI/udev repositories sharing the same context.
"""

import logging
import re
from typing import Optional

from ctaops.sdk.controller_providers.protocols.library import (
    LibraryControllerProvider,
    ManagedLibraryControllerProvider,
)
from ctaops.sdk.controllers.scsi.library import ScsiLibraryController, ScsiManagedLibraryController
from ctaops.sdk.exceptions import ResourceNotFoundError
from ctaops.sdk.repositories.local.drive import LocalLocatedNamedDriveRepository
from pyudev import Context, Device

LOGGER = logging.getLogger("ctaops.sdk")


class ScsiLibraryControllerProvider(LibraryControllerProvider):
    """Build minimal SCSI-backed library controllers for local changers.

    Each call to `get_library_controller(...)` / `find_library_controller(...)`
    returns a
    [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController]
    bound to the matching `sgN` medium-changer device. Controllers
    produced by the same provider share the provider's pyudev `Context`,
    so a single udev snapshot is consistent across all libraries enumerated.

    Note:
        This provider yields ordinal-only controllers. For day-to-day
        operations prefer
        [ScsiManagedLibraryControllerProvider][ctaops.sdk.controller_providers.scsi.library.ScsiManagedLibraryControllerProvider],
        which yields managed controllers exposing logical-name resolution,
        context-managed `mount(...)`, and `label_tape(...)`.

    Args:
        context: Optional pyudev `Context`. When omitted, a fresh context
            is created.

    Example:
        ```python
        from ctaops.sdk import ScsiLibraryControllerProvider

        provider = ScsiLibraryControllerProvider()
        library = provider.get_library_controller_by_name("T0-LIB0")
        print(library.serial_number)
        ```
    """

    def __init__(
        self,
        context: Optional[Context] = None,
        library_name_key: str = "CTAOPS_NAME",
    ):
        LOGGER.debug("Initializing ScsiLibraryControllerProvider")
        self._context = context if context is not None else Context()
        self._library_name_key = library_name_key

    def _list_media_changer_sg_devices(self) -> set[Device]:
        """Return the set of medium-changer `sgN` devices visible locally."""
        return {
            device
            for device in self._context.list_devices(subsystem="scsi_generic", SCSI_TYPE="medium changer")
            if re.match(r"^sg[0-9]+$", device.sys_name)
        }

    def find_library_controller(
        self,
        serial_number: str,
    ) -> Optional[ScsiLibraryController]:
        LOGGER.debug("Searching for library controller with serial=%s", serial_number)
        return next(
            (
                ScsiLibraryController(sg_device)
                for sg_device in self._list_media_changer_sg_devices()
                if serial_number == sg_device.properties.get("ID_SCSI_SERIAL")
            ),
            None,
        )

    def get_library_controller(
        self,
        serial_number: str,
    ) -> ScsiLibraryController:
        if (library_controller := self.find_library_controller(serial_number)) is None:
            raise ResourceNotFoundError(f"Unable to find media changer for {serial_number} locally")
        LOGGER.debug("Resolved library controller for serial=%s", serial_number)
        return library_controller

    def find_library_controller_by_name(
        self,
        name: str,
    ) -> Optional[ScsiLibraryController]:
        LOGGER.debug("Searching for library controller with name=%s", name)
        return next(
            (
                ScsiLibraryController(sg_device)
                for sg_device in self._list_media_changer_sg_devices()
                if name == sg_device.properties.get(self._library_name_key)
            ),
            None,
        )

    def get_library_controller_by_name(
        self,
        name: str,
    ) -> ScsiLibraryController:
        if (library_controller := self.find_library_controller_by_name(name)) is None:
            raise ResourceNotFoundError(f"Unable to find media changer for {name} locally")
        LOGGER.debug("Resolved library controller for name=%s", name)
        return library_controller


class ScsiManagedLibraryControllerProvider(ScsiLibraryControllerProvider, ManagedLibraryControllerProvider):
    """Build managed SCSI-backed library controllers for local changers.

    Each call to `get_library_controller(...)` / `find_library_controller(...)`
    returns a
    [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController]
    — the controller-of-choice for day-to-day operations because it adds
    on top of
    [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController]:

    * logical-name resolution (drives can be addressed by either ordinal
      or `CTAOPS_NAME`),
    * a richer `mount(...)` returning a context-managed
      [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController],
    * the `label_tape(...)` operation backed by `cta-tape-label`.

    Every controller produced by this provider shares the same pyudev
    `Context` so a single udev snapshot is consistent across all libraries
    and the drives they expose.

    Args:
        context: Optional pyudev `Context`. When omitted, a fresh context
            is created.

    Example:
        ```python
        from ctaops.sdk import ScsiManagedLibraryControllerProvider

        provider = ScsiManagedLibraryControllerProvider()
        library = provider.get_library_controller_by_name("T0-LIB0")
        with library.mount("T0-LIB0-DRV0", "X12345") as drive:
            for index, checksum in drive.write_files(file_size=1 << 30, n=5):
                print(index, checksum)
        ```
    """

    def __init__(
        self,
        context: Optional[Context] = None,
        library_name_key: str = "CTAOPS_NAME",
        drive_name_key: str = "CTAOPS_NAME",
        drive_library_address_key: str = "CTAOPS_LIBRARY_ADDRESS",
        drive_library_ordinal_key: str = "CTAOPS_LIBRARY_ORDINAL",
    ):
        super().__init__(context, library_name_key)
        LOGGER.debug("Initializing ScsiManagedLibraryControllerProvider")
        self._drive_repo = LocalLocatedNamedDriveRepository(
            self._context, drive_name_key, drive_library_address_key, drive_library_ordinal_key
        )

    def _list_media_changer_sg_devices(self) -> set[Device]:
        """Return the set of medium-changer `sgN` devices visible locally."""
        return {
            device
            for device in self._context.list_devices(subsystem="scsi_generic", SCSI_TYPE="medium changer")
            if re.match(r"^sg[0-9]+$", device.sys_name)
        }

    def find_library_controller(
        self,
        serial_number: str,
    ) -> Optional[ScsiManagedLibraryController]:
        LOGGER.debug("Searching for library controller with serial=%s", serial_number)
        return next(
            (
                ScsiManagedLibraryController(sg_device)
                for sg_device in self._list_media_changer_sg_devices()
                if serial_number == sg_device.properties.get("ID_SCSI_SERIAL")
            ),
            None,
        )

    def get_library_controller(
        self,
        serial_number: str,
    ) -> ScsiManagedLibraryController:
        if (library_controller := self.find_library_controller(serial_number)) is None:
            raise ResourceNotFoundError(f"Unable to find media changer for {serial_number} locally")
        LOGGER.debug("Resolved library controller for serial=%s", serial_number)
        return library_controller

    def find_library_controller_by_name(
        self,
        name: str,
    ) -> Optional[ScsiManagedLibraryController]:
        LOGGER.debug("Searching for library controller with name=%s", name)
        return next(
            (
                ScsiManagedLibraryController(sg_device)
                for sg_device in self._list_media_changer_sg_devices()
                if name == sg_device.properties.get("CTAOPS_NAME")
            ),
            None,
        )

    def get_library_controller_by_name(
        self,
        name: str,
    ) -> ScsiManagedLibraryController:
        if (library_controller := self.find_library_controller_by_name(name)) is None:
            raise ResourceNotFoundError(f"Unable to find media changer for {name} locally")
        LOGGER.debug("Resolved library controller for name=%s", name)
        return library_controller

    def get_library_controller_from_drive_name(
        self,
        drive_name: str,
    ) -> ScsiManagedLibraryController:
        """Return the managed library controller matching `drive_name`.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        return self.get_library_controller(
            self._drive_repo.get_drive_by_name(drive_name).location.library_serial_number
        )

    def find_library_controller_from_drive_name(
        self,
        drive_name: str,
    ) -> Optional[ScsiManagedLibraryController]:
        """Return the managed library controller matching `drive_name`, or `None`."""
        LOGGER.debug("Searching for library controller from drive name=%s", drive_name)
        try:
            return self.get_library_controller_from_drive_name(drive_name)
        except ResourceNotFoundError:
            return None
