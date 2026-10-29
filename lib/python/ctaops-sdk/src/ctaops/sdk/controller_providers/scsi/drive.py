# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Provider for SCSI-backed drive controllers.

A controller provider is the recommended entry point for callers that
operate on a drive without going through the library: it enumerates every
locally-visible tape drive and returns
[ScsiDriveController][ctaops.sdk.controllers.scsi.drive.ScsiDriveController]
instances sharing a single pyudev `Context`.
"""

import logging
import re
from typing import Optional

from ctaops.sdk.controller_providers.protocols.drive import DriveControllerProvider
from ctaops.sdk.controllers.scsi.drive import ScsiDriveController
from ctaops.sdk.exceptions import ResourceNotFoundError
from pyudev import Context, Device
from typing_extensions import override

LOGGER = logging.getLogger("ctaops.sdk")


class ScsiDriveControllerProvider(DriveControllerProvider):
    """
    Build [ScsiDriveController][ctaops.sdk.controllers.scsi.drive.ScsiDriveController] instances for local tape drives.

    Implements the
    [DriveControllerProvider][ctaops.sdk.controller_providers.protocols.drive.DriveControllerProvider]
    protocol; instances can be addressed by SCSI serial number or by
    ctaops logical name (`CTAOPS_NAME` udev property).

    Args:
        context: Optional pyudev `Context`. When omitted, a fresh context
            is created.

    Example:
        ```python
        from ctaops.sdk import ScsiDriveControllerProvider

        provider = ScsiDriveControllerProvider()
        drive = provider.get_drive_controller_by_name("drive-0")
        print(drive.get_drive_temperature())
        ```
    """

    def __init__(self, context: Optional[Context] = None, drive_name_key: str = "CTAOPS_NAME"):
        LOGGER.debug("Initializing ScsiDriveControllerProvider")
        self._context = Context() if context is None else context
        self._drive_name_key = drive_name_key

    def _list_drive_nst_devices(self) -> set[Device]:
        """Return the set of `nstN` `scsi_tape` devices visible locally."""
        return {
            device
            for device in self._context.list_devices(subsystem="scsi_tape")
            if re.match(r"^nst[0-9]+$", device.sys_name)
        }

    @override
    def find_drive_controller(
        self,
        serial_number: str,
    ) -> Optional[ScsiDriveController]:
        LOGGER.debug("Searching for drive controller with serial=%s", serial_number)
        return next(
            (
                ScsiDriveController(nst_device, self._context)
                for nst_device in self._list_drive_nst_devices()
                if serial_number == nst_device.properties.get("ID_SCSI_SERIAL")
            ),
            None,
        )

    @override
    def get_drive_controller(self, serial_number: str) -> ScsiDriveController:
        """Return the controller for the drive with the given SCSI serial.

        Args:
            serial_number: SCSI serial number of the drive.

        Raises:
            ResourceNotFoundError: If no matching drive is visible locally.
        """
        if (drive_controller := self.find_drive_controller(serial_number)) is None:
            raise ResourceNotFoundError(f"Unable to find nst device for {serial_number} locally")
        LOGGER.debug("Resolved drive controller for serial=%s", serial_number)
        return drive_controller

    @override
    def find_drive_controller_by_name(
        self,
        name: str,
    ) -> Optional[ScsiDriveController]:
        LOGGER.debug("Searching for drive controller with name=%s", name)
        return next(
            (
                ScsiDriveController(nst_device, self._context)
                for nst_device in self._list_drive_nst_devices()
                if name == nst_device.properties.get(self._drive_name_key)
            ),
            None,
        )

    @override
    def get_drive_controller_by_name(
        self,
        name: str,
    ) -> ScsiDriveController:
        if (drive_controller := self.find_drive_controller_by_name(name)) is None:
            raise ResourceNotFoundError(f"Unable to find nst device for {name} locally")
        LOGGER.debug("Resolved drive controller for name=%s", name)
        return drive_controller
