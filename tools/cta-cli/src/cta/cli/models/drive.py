# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Enriched versions of CTA SDK models with tape server metadata.

This module extends the base CTA SDK models for drive and library objects by
adding optional serial number and connection state attributes.
"""

from dataclasses import dataclass
from typing import Optional

from cta.sdk.models import Drive
from ctaops.utils import copy_dataclass


@dataclass
class EnrichedDrive(Drive):
    """Drive model enriched with tape-server-specific metadata.

    Attributes:
        serial_number: Optional[str]
            Serial number of the drive, if available.
        is_connected: bool
            Whether the drive is currently connected.
    """

    serial_number: Optional[str]
    is_connected: bool

    @classmethod
    def from_drive(
        cls, drive: Drive, serial_number: Optional[str] = None, is_connected: bool = False
    ) -> "EnrichedDrive":
        """Create an enriched drive from a base drive model.

        Args:
            drive: The base drive model to enrich.
            serial_number: Optional serial number for the enriched drive.
            is_connected: Connection state for the enriched drive.

        Returns:
            EnrichedDrive: An enriched drive instance.
        """
        return cls(
            **copy_dataclass(drive),
            serial_number=serial_number,
            is_connected=is_connected,
        )
