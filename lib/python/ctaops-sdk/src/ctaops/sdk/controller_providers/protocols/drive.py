# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Provider protocol for drive controllers.

A provider enumerates locally-visible tape drives and returns
[DriveController][ctaops.sdk.controllers.protocols.drive.DriveController]
instances for them, either by SCSI serial number or by logical ctaops
name. The `get_*` methods raise
[ResourceNotFoundError][ctaops.sdk.exceptions.ResourceNotFoundError] when
no match is visible; the `find_*` variants return `None` instead so
callers can branch on existence without exception handling.
"""

from typing import Optional, Protocol

from ctaops.sdk.controllers.protocols.drive import DriveController


class DriveControllerProvider(Protocol):
    """Locate controllers by SCSI serial or logical name.

    Return [DriveController][ctaops.sdk.controllers.protocols.drive.DriveController] instances.
    """

    def get_drive_controller(
        self,
        serial_number: str,
    ) -> DriveController:
        """Return the drive controller matching `serial_number`.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def find_drive_controller(
        self,
        serial_number: str,
    ) -> Optional[DriveController]:
        """Return the drive controller matching `serial_number`, or `None`."""
        ...

    def get_drive_controller_by_name(
        self,
        name: str,
    ) -> DriveController:
        """Return the drive controller matching the logical ctaops `name`.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def find_drive_controller_by_name(
        self,
        name: str,
    ) -> Optional[DriveController]:
        """Return the drive controller matching `name`, or `None`."""
        ...
