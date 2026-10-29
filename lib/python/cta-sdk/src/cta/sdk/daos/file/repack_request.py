# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Iterator, Literal, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import RepackRequestDao
from cta.sdk.models import (
    RepackRequest,
    RepackStartInfo,
    RepackState,
    RepackStatistics,
    Tape,
)


def _no_statistics() -> RepackStatistics:
    return RepackStatistics(
        total_file_count=0,
        total_byte_count=0,
        transferred_file_count=0,
        transferred_byte_count=0,
        failed_file_count=0,
        failed_byte_count=0,
    )


class RepackRequestDaoFile(RepackRequestDao):
    resource_type = RepackRequest

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(
        self,
        vid: str,
        /,
        *,
        mount_policy_name: str,
        no_recall: bool = False,
        buffer_url: Optional[str] = None,
        max_files_to_select: Optional[int] = None,
        custom_operation: Optional[Literal["MOVE", "ADDCOPIES"]] = None,
    ) -> None:
        # `RepackRequest` has no field for mount_policy_name, no_recall, max_files_to_select or
        # custom_operation, so those only steer the request that the real scheduler would run;
        # here they are accepted for signature compatibility but not persisted.
        tape = self._handler.get(Tape, vid)
        creation_metadata = self._handler.gen_creation_metadata()

        repack_request = RepackRequest(
            vid,
            tape_pool_name=tape.tape_pool_name,
            state=RepackState.PENDING,
            failed_file_count=0,
            disk_buffer_url=buffer_url or "",
            duration=timedelta(),
            finished_time=None,
            destination_infos=[],
            last_expanded_fseq=0,
            start_info=RepackStartInfo(
                file_count=tape.statistics.file_count,
                byte_count=tape.statistics.logical_usage,
                creation_user_name=creation_metadata["creation_user_name"],
                creation_host_name=creation_metadata["creation_host_name"],
                creation_time=creation_metadata["creation_time"],
                is_all_files_selected_at_start=max_files_to_select is None,
            ),
            retrieve_statistics=RepackStatistics(
                total_file_count=tape.statistics.file_count,
                total_byte_count=tape.statistics.logical_usage,
                transferred_file_count=0,
                transferred_byte_count=0,
                failed_file_count=0,
                failed_byte_count=0,
            ),
            archive_statistics=RepackStatistics(
                total_file_count=tape.statistics.file_count,
                total_byte_count=tape.statistics.logical_usage,
                transferred_file_count=0,
                transferred_byte_count=0,
                failed_file_count=0,
                failed_byte_count=0,
            ),
        )
        self._handler.create(repack_request)

    def get(self, vid: str, /) -> RepackRequest:
        return self._handler.get(self.resource_type, vid)

    def delete(self, vid: str, /) -> None:
        self._handler.delete(self.resource_type, vid)

    def list(self) -> Iterator[RepackRequest]:
        yield from self._handler.iter(self.resource_type)
