# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Enriched versions of CTA Drive SDK model with tape server metadata.

This module extends the base CTA SDK models for drive.
"""

from dataclasses import dataclass
from typing import Optional

from cta.sdk.models import LogicalLibrary
from ctaops.utils import copy_dataclass


@dataclass
class EnrichedLogicalLibrary(LogicalLibrary):
    """Logical library model enriched with tape-server-specific metadata.

    Attributes:
        serial_number: Optional[str]
            Serial number of the logical library, if available.
        is_connected: bool
            Whether the logical library is currently connected.
    """

    serial_number: Optional[str]
    is_connected: bool

    @classmethod
    def from_logical_library(
        cls, logical_library: LogicalLibrary, serial_number: Optional[str] = None, is_connected: bool = False
    ) -> "EnrichedLogicalLibrary":
        """Create an enriched logical library from a base logical library model.

        Args:
            logical_library: The base logical library model to enrich.
            serial_number: Optional serial number for the enriched logical library.
            is_connected: Connection state for the enriched logical library.

        Returns:
            EnrichedLogicalLibrary: An enriched logical library instance.
        """
        return cls(
            **copy_dataclass(logical_library),
            serial_number=serial_number,
            is_connected=is_connected,
        )
