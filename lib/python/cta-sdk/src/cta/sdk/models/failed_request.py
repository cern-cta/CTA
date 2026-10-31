# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from typing import Annotated, Final

from cta.sdk.units import Byte
from typing_extensions import override

from .mount_type import MountType
from .tape_file import TapeFile


@dataclass(eq=False)
class FailedRequest:
    """Represent a failed request within CTA.

    Attributes:
        id: Failed request unique identifier
        mount_type: UserRequest type
        requester_name: Name of the requester
        requester_group: Group of the requester
        tape_file: Associated tape file
        tape_pool_name: Name of the tape pool
        total_retries: Total number of retries attempted
        logs: UserRequest logs
    """

    id: Final[str]
    mount_type: MountType
    requester_name: str
    requester_group: str
    tape_file: TapeFile
    tape_pool_name: str
    total_retries: int
    logs: str

    @override
    def __str__(self):
        return f"FailedRequest({self.id})"

    @override
    def __hash__(self) -> int:
        return hash(self.id)

    @override
    def __eq__(self, other: object):
        if not isinstance(other, FailedRequest):
            return False
        return self.id == other.id


@dataclass
class FailedRequestSummary:
    """Summary information for failed requests.

    Attributes:
        mount_type: UserRequest type
        total_file_count: Total count of failed files
        total_byte_count: Total byte count of failed files
        scheduler_backend_name: Name of the scheduler backend
    """

    mount_type: MountType
    total_file_count: int
    total_byte_count: Annotated[int, Byte]
    scheduler_backend_name: str
