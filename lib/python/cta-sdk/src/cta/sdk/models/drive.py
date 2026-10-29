# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Annotated, Final, NewType, Optional

from cta.sdk.units import FreeText

from .tape_session import TapeSession

DriveConfig = NewType("DriveConfig", dict[str, str])


class DriveOperatingState(Enum):
    """Operating State of drive

    Attributes:
        UP: Drive available for CTA operation
        DOWN: Drive not available for CTA operation
    """

    UP = "up"
    DOWN = "down"


@dataclass
class DriveMetadata:
    """Drive metadata information.

    Attributes:
        last_refresh_age: Time since last update of the drive table by corresponding cta-taped process
        cta_version: Version of cta-taped
        scheduler_namespace: Name of the scheduler backend used to separate repack and user workflow
        comment: Drive comment
    """

    last_refresh_age: timedelta
    cta_version: str
    scheduler_namespace: str
    comment: Annotated[str, FreeText]


@dataclass(eq=False)
class Drive:
    """Represent a drive object within CTA.

    Attributes:
        name: Drive name
        logical_library_name: Logical library name
        host_name: Tape server hostname
        desired_state: Desired drive operating state
        state: Drive operating state
        state_reason: Reason for drive being down if applicable
        device_path: Device path of the drive on tape server (Almost useless thanks to naming convention)
        physical_library_name: Physical library name
        physical_library_slot: Drive slot within its library
        tape_session: Current tape tape_session (Empty means NO MOUNT)
        state_change_age: Duration since last drive state change
        config: Dictionnary of the drive loaded configuration
        metadata: Drive metadata information
    """

    name: Final[str]
    logical_library_name: str
    host_name: str
    desired_state: DriveOperatingState
    state: DriveOperatingState
    state_reason: Annotated[str, FreeText]
    device_path: str
    physical_library_name: str
    physical_library_slot: str
    tape_session: Optional[TapeSession]
    state_change_age: timedelta
    config: DriveConfig
    metadata: DriveMetadata

    def __str__(self) -> str:
        return f"Drive({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Drive):
            return False
        return self.name == other.name
