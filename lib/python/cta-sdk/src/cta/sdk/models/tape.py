# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Annotated, Any, Final, Optional

from cta.sdk.units import Byte, Percentage
from typing_extensions import override

from .metadata import Metadata


@dataclass
class VerificationStatus:
    """Tape verification status information.

    Attributes:
        date: Date of verification
        status: Verification status
        file_submitted_count: Number of files submitted for verification
        file_verified_count: Number of files verified
        file_failed_count: Number of files that failed verification
    """

    date: datetime
    status: str
    file_submitted_count: int
    file_verified_count: int
    file_failed_count: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VerificationStatus":
        return cls(
            date=data[""],
            status=data[""],
            file_submitted_count=data[""],
            file_verified_count=data[""],
            file_failed_count=data[""],
        )


@dataclass
class TapeMetadata(Metadata):
    """Tape metadata information.

    Attributes:
        label_time: Time when tape was labeled
        label_drive_name: Drive name that performed the label operation
        last_write_time: Time of last write operation
        last_write_drive_name: Drive name that performed last write
        last_read_time: Time of last read operation
        last_read_drive_name: Drive name that performed last read
        update_state_time: Time when state was updated
        update_state_info: Information about state update
        write_mount_count: Number of write mounts.
        read_mount_count: Number of read mounts.
    """

    label_time: Optional[datetime]
    label_drive_name: Optional[str]
    last_write_time: Optional[datetime]
    last_write_drive_name: Optional[str]
    last_read_time: Optional[datetime]
    last_read_drive_name: Optional[str]
    update_state_time: datetime
    update_state_info: str
    write_mount_count: int
    read_mount_count: int


class TapeState(Enum):
    """
    Tape tape_session possible states.

    Attributes:
        ACTIVE: The tape is in good condition to allow a user to read data from it or to write data to it.
        DISABLED: The tape cannot be mounted, but can still have retrieve requests queued to it.
        BROKEN: A tape can stay in this state for long and it is very likely that it is its very last state.
        EXPORTED: A tape can stay in this state for long. It means that the tape was removed from the tape library.
        REPACKING: A tape should be moved to REPACKING state before the operator submits a repack request. Otherwise,
            the repack request won't be accepted.
        REPACKING_DISABLED: The state REPACKING_DISABLED is similar to DISABLED, but for repacking tapes (we can't move
            out of repacking states while a repack is ongoing).
    """

    # Enum supplies the member name, not self, to this Python 3.9 hook.
    def _generate_next_value_(name: str, start, count, last_values):  # type: ignore[override]  # noqa: N805
        return name.lower().replace("_", "-")

    ACTIVE = auto()
    DISABLED = auto()
    BROKEN = auto()
    EXPORTED = auto()
    REPACKING = auto()
    REPACKING_DISABLED = auto()


@dataclass
class TapeStatistics:
    """Statistics about a tape's usage.

    Attributes:
        physical_usage: In bytes.
        logical_usage: In bytes.
        is_dirty: True if the tape is marked dirty.
        last_fseq: Last file sequence position.
        is_full: True when the tape is full.
        file_count: Number of files on the tape.
    """

    physical_usage: Annotated[int, Byte]
    logical_usage: Annotated[int, Byte]
    is_dirty: bool
    last_fseq: int
    is_full: bool
    file_count: int


@dataclass(eq=False)
class Tape:
    """Represents a physical tape within CTA.

    Attributes:
        vid: Volume identifier.
        media_type_name: Media type name.
        vendor: Vendor name.
        purchase_order: Purchase order number.
        logical_library_name: Logical library name.
        physical_library_name: Physical library name.
        tape_pool_name: Tape pool name.
        virtual_organisation_name: Virtual organisation name.
        encryption_key_name: None if the tape is not encrypted.
        capacity: Capacity in bytes.
        format: Format set during a label operation.
        state: Current state of the tape.
        state_reason: Reason for the tape's state.
        verification_status: Optional verification status.
        usage_ratio: Usage Ratio of the tape (0..1).
        statistics: Tape usage statistics.
        metadata: Tape metadata.
    """

    vid: Final[str]
    media_type_name: str
    vendor: str
    purchase_order: str
    logical_library_name: str
    physical_library_name: str
    tape_pool_name: str
    virtual_organisation_name: str
    encryption_key_name: Optional[str]
    capacity: Annotated[int, Byte]
    format: str
    state: TapeState
    state_reason: str
    verification_status: Optional[VerificationStatus]
    usage_ratio: Annotated[float, Percentage] = field(init=False)

    statistics: TapeStatistics
    metadata: TapeMetadata

    @override
    def __str__(self) -> str:
        return f"Tape({self.vid})"

    @override
    def __hash__(self) -> int:
        return hash(self.vid)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, Tape):
            return False
        return self.vid == other.vid

    def __post_init__(self):
        self.usage_ratio = self.statistics.logical_usage / (
            max(self.statistics.physical_usage, 1)
            if self.statistics.is_full
            else max(self.statistics.physical_usage, self.capacity)
        )

    @property
    def is_empty(self) -> bool:
        """Checks if a tape volume is empty."""
        return (
            not self.statistics.is_full
            and self.statistics.physical_usage == 0
            and self.statistics.last_fseq == 0
            and self.statistics.file_count == 0
        )
