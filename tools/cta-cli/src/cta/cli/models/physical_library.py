# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Enriched versions of CTA SDK models with tape server metadata.

This module extends the base CTA SDK models for drive and library objects by
adding optional serial number and connection state attributes.
"""

from dataclasses import dataclass
from typing import Optional

from cta.common import copy_dataclass
from cta.sdk.models import PhysicalLibrary
from cta.tapeserver.sdk.repositories.protocols.library import NamedLibrary


@dataclass
class EnrichedPhysicalLibrary(PhysicalLibrary):
    """Physical library model enriched with tape-server-specific metadata.

    Attributes:
        serial_number: Optional[str]
            Serial number of the physical library, if available.
    """

    local_serial_number: Optional[str]

    @classmethod
    def merge(
        cls, cta_physical_library: PhysicalLibrary, local_library: Optional[NamedLibrary] = None
    ) -> "EnrichedPhysicalLibrary":
        """Create an enriched physical library from a base physical library model.

        Args:
            cta_physical_library: The base cta physical library model to enrich.
            local_library: Optional local library.
        Returns:
            EnrichedPhysicalLibrary: An enriched physical library instance.
        """
        if local_library is None:
            return cls(
                **copy_dataclass(cta_physical_library),
                local_serial_number=None,
            )

        if local_library.name != cta_physical_library.name:
            raise ValueError("Mismatch between local_library.name and cta_physical_library.name")

        return cls(
            **copy_dataclass(cta_physical_library),
            local_serial_number=local_library.serial_number,
        )
