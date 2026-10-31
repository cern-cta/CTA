# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Enriched versions of CTA SDK models with tape server metadata.

This module extends the base CTA SDK models for drive and library objects by
adding optional serial number and connection state attributes.
"""

from dataclasses import dataclass
from typing import Optional

from cta.sdk.models import PhysicalLibrary
from ctaops.utils import copy_dataclass


@dataclass
class EnrichedPhysicalLibrary(PhysicalLibrary):
    """Physical library model enriched with tape-server-specific metadata.

    Attributes:
        serial_number: Optional[str]
            Serial number of the physical library, if available.
        is_connected: bool
            Whether the physical library is currently connected.
    """

    serial_number: Optional[str]
    is_connected: bool

    @classmethod
    def from_physical_library(
        cls, physical_library: PhysicalLibrary, serial_number: Optional[str] = None, is_connected: bool = False
    ) -> "EnrichedPhysicalLibrary":
        """Create an enriched physical library from a base physical library model.

        Args:
            physical_library: The base physical library model to enrich.
            serial_number: Optional serial number for the enriched physical library.
            is_connected: Connection state for the enriched physical library.

        Returns:
            EnrichedPhysicalLibrary: An enriched physical library instance.
        """
        return cls(
            **copy_dataclass(physical_library),
            serial_number=serial_number,
            is_connected=is_connected,
        )
