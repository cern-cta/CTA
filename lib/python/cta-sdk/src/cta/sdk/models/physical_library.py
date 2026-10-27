# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from enum import Enum
from typing import Final, Optional

from .metadata import Metadata


class PhysicalLibraryState(Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"


@dataclass(eq=False)
class PhysicalLibrary:
    """Physical tape library.

    Attributes:
        name: Unique name of the physical library.
        manufacturer: The Manufacturer name.
        model: The model of the library.
        type: Cartridge technology.
        gui_url: Graphical user interface web link.
        webcam_url: Webcam URL.
        location: Building location.
        physical_cartridge_slots: Number of slots for cartridges.
        available_cartridge_slots: Number of available slots for cartridges.
        physical_drive_slots: Number of drive slots.
        is_disabled: True if the library is disabled.
        state_reason: Optional reason why the library is disabled.
        metadata: Metadata about creation and updates.
    """

    name: Final[str]
    manufacturer: str
    model: str
    type: str
    gui_url: str
    webcam_url: str
    location: str
    physical_cartridge_slots: int
    available_cartridge_slots: int
    physical_drive_slots: int
    state: PhysicalLibraryState
    state_reason: Optional[str]
    metadata: Metadata

    def __str__(self) -> str:
        return f"PhysicalLibrary({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, PhysicalLibrary):
            return False
        return self.name == other.name
