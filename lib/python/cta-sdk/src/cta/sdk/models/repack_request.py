# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Annotated, Any, Final, Optional

from cta.sdk.units import Byte, Percentage


class RepackState(Enum):
    """Possible Repack UserRequest states

    Attributes:
        COMPLETE: All the Retrieve and Archive subrequest are completed have been reported as successful.
        STARTING: A job was taken by a tape drive
        RUNNING: The first Retrieve or Archive subrequest has been reported as successful or failed.
        FAILED: All the Retrieve and Archive subrequests are completed but at least one has failed.
        PENDING: UserRequest waiting to be popped by the maintenance daemon.
        TO_EXPAND: UserRequest waiting to be popped by the maintenance daemon.
    """

    def _generate_next_value_(name: str, start, count, last_values):  # type: ignore[override]
        return name.lower().replace("_", "-")

    COMPLETE = auto()
    STARTING = auto()
    RUNNING = auto()
    FAILED = auto()
    PENDING = auto()
    TO_EXPAND = auto()


@dataclass
class RepackStatistics:
    """Transfer statistics on files and bytes.

    Attributes:
        total_file_count: Total number of files to transfer
        total_byte_count: Total number of bytes to transfer
        transferred_file_count: Number of files sucessfully transferred
        transferred_byte_count: Number of bytes sucessfully transferred
        failed_file_count: Number of files that failed to transfer
        failed_byte_count: Number of bytes that failed to transfer
    """

    total_file_count: int
    total_byte_count: Annotated[int, Byte]
    transferred_file_count: int
    transferred_byte_count: Annotated[int, Byte]
    failed_file_count: int
    failed_byte_count: Annotated[int, Byte]

    @property
    def remaining_file_count(self) -> int:
        """Remaining number of files to repack"""
        return self.total_file_count - self.transferred_file_count

    @property
    def remaining_byte_count(self) -> int:
        """Remaining number of bytes to repack"""
        return self.total_byte_count - self.transferred_byte_count


@dataclass
class RepackStartInfo:
    """Starting static information.

    Attributes:
        file_count: Number of files on tape before repack started.
        byte_count: Number of bytes on tape before repack started.
        creation_user_name: Admin user which created the resource.
        creation_host_name: Server hostname from which resource was created.
        creation_time: Date of the resource creation.
        is_all_files_selected_at_start: True if all files were selected at start.
    """

    file_count: int
    byte_count: Annotated[int, Byte]
    creation_user_name: str
    creation_host_name: str
    creation_time: datetime
    is_all_files_selected_at_start: bool


@dataclass(eq=False)
class RepackRequest:
    """RepackRequest a group of UserRequests of vo REPACK to repack a tape.

    Attributes:
        vid: Tape volume identifier.
        tape_pool_name: Tape pool name of the vid.
        state: Current state of the repack request.
        failed_file_count: Number of files on which repack failed.
        disk_buffer_url: Disk buffer URL to use.
        duration: Repack duration since request creation.
        finished_time: Time when the request was marked as finished or failed.
        destination_infos: Destination information for the repack.
        last_expanded_fseq: Last expanded file sequence number on the tape.
        start_info: Starting static information.
        retrieve_statistics: Statistics for the retrieve phase.
        archive_statistics: Statistics for the archive phase.
    """

    vid: Final[str]
    tape_pool_name: str
    state: RepackState
    failed_file_count: int
    disk_buffer_url: str
    duration: timedelta
    finished_time: Optional[datetime]
    destination_infos: list[dict[str, Any]]
    last_expanded_fseq: int
    start_info: RepackStartInfo
    retrieve_statistics: RepackStatistics
    archive_statistics: RepackStatistics

    def __str__(self) -> str:
        return f"RepackRequest({self.vid})"

    def __hash__(self) -> int:
        return hash(self.vid)

    def __eq__(self, other):
        if not isinstance(other, RepackRequest):
            return False
        return self.vid == other.vid

    @property
    def progress(self) -> Annotated[float, Percentage]:
        """Progression of the repack (Percentage between 0 and 1)"""
        if self.retrieve_statistics.total_byte_count == 0 or self.archive_statistics.total_byte_count == 0:
            return 0

        return (
            self.retrieve_statistics.transferred_byte_count / self.retrieve_statistics.total_byte_count / 2
            + self.archive_statistics.transferred_byte_count / self.archive_statistics.total_byte_count / 2
        )
