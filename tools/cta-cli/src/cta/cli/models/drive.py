# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Enriched versions of CTA SDK models with tape server metadata.

This module extends the base CTA SDK models for drive and library objects by
adding optional serial number and connection state attributes.
"""

from dataclasses import dataclass
from typing import Optional

from cta.common import copy_dataclass
from cta.common.prometheus import Labels
from cta.sdk.models import Drive
from cta.tapeserver.sdk.repositories.protocols.drive import DriveLibraryLocation, LocatedNamedDrive


@dataclass
class EnrichedDrive(Drive):
    """Drive model enriched with tape-server-specific metadata.

    Attributes:
        serial_number: Optional[str]
            Serial number of the drive, if available.
        is_connected: bool
            Whether the drive is currently connected.
    """

    local_serial_number: Optional[str]
    location: Optional[DriveLibraryLocation]

    @classmethod
    def merge(cls, cta_drive: Drive, local_drive: Optional[LocatedNamedDrive] = None) -> "EnrichedDrive":
        """Create an enriched drive from a base drive model.

        Args:
            cta_drive: The base drive model to enrich.
            local_drive:Optional local drive.

        Returns:
            EnrichedDrive: An enriched drive instance.
        """
        if local_drive is None:
            return cls(
                **copy_dataclass(cta_drive),
                local_serial_number=None,
                location=None,
            )

        if local_drive.name != cta_drive.name:
            raise ValueError("Mismatch between local_drive.name and cta_drive.name")

        return cls(
            **copy_dataclass(cta_drive),
            local_serial_number=local_drive.serial_number,
            location=local_drive.location,
        )


@dataclass(frozen=True)
class DriveSensors:
    """Sensor readings of a local tape drive.

    Attributes:
        name: Logical name of the drive.
        serial_number: SCSI serial number of the drive.
        temperature_celsius: Internal temperature, None if the drive does not report it.
        humidity_percent: Internal relative humidity, None if the drive does not report it.
    """

    name: str
    serial_number: str
    temperature_celsius: Optional[int]
    humidity_percent: Optional[int]

    def labels(self) -> Labels:
        return {"drive": self.name, "serial_number": self.serial_number}
