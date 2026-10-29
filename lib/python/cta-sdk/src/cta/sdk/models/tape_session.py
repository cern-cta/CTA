# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Annotated, Final

from cta.sdk.units import Byte, BytePerSecond

from .mount_type import MountType


class TapeSessionState(Enum):
    """Tape tape_session possible states

    Attributes:
        STARTING:
        MOUNTING:
        TRANSFERRING: Tranferring data
        CLEANING_UP:
        UNLOADING: The tape is being ejected out off the drive.
        UNMOUNTING: The tape is being unmounted waiting for the robot to be picked up.
        DRAINING_TO_DISK:
    """

    def _generate_next_value_(name: str, start, count, last_values):  # type: ignore[override]
        return name.lower().replace("_", "-")

    STARTING = auto()
    MOUNTING = auto()
    TRANSFERRING = auto()
    CLEANING_UP = auto()
    UNLOADING = auto()
    UNMOUNTING = auto()
    DRAINING_TO_DISK = auto()


@dataclass(eq=False)
class TapeSession:
    """Session information of a drive process.

    Attributes:
        id: Unique tape_session identifier.
        vid: Tape volume identifier.
        tape_pool_name: Name of the tape pool used.
        mount_type: Mount type.
        file_transferred_count: Number of files transferred during the current tape_session.
        byte_transferred_count: Number of bytes transferred during the current tape_session.
        virtual_organisation_name: Name of the virtual organisation.
        state: State of the current tape_session.
        speed: Speed of the tape_session in Mb/s.
        duration: Duration of the tape_session.
    """

    id: Final[int]
    vid: str
    tape_pool_name: str
    mount_type: MountType
    file_transferred_count: int
    byte_transferred_count: Annotated[int, Byte]
    virtual_organisation_name: str
    state: TapeSessionState
    speed: Annotated[float, BytePerSecond] = field(init=False)
    duration: timedelta  # remove from it and put under Drive directly: state_change_age (keep DURATION in output)

    def __post_init__(self):
        self.speed = self.byte_transferred_count / max(self.duration.total_seconds(), 1)

    def __str__(self) -> str:
        return f"Session({self.id})"

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other):
        if not isinstance(other, TapeSession):
            return False
        return self.id == other.id
