# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

from cta.sdk.daos.cli._wrapper import FLAG, CtaCliWrapper
from cta.sdk.daos.protocols import RepackRequestDao
from cta.sdk.models import (
    RepackRequest,
    RepackStartInfo,
    RepackState,
    RepackStatistics,
)
from typing_extensions import override


class RepackRequestDaoCli(RepackRequestDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "repack"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> RepackRequest:
        return RepackRequest(
            raw_data["vid"],
            tape_pool_name=raw_data["tapepool"],
            state=RepackState["TO_EXPAND" if raw_data["status"] == "ToExpand" else raw_data["status"].upper()],
            failed_file_count=int(raw_data["totalFailedFiles"]),
            disk_buffer_url=raw_data["repackBufferUrl"],
            duration=timedelta(seconds=int(raw_data["repackTime"])),
            finished_time=(
                datetime.fromtimestamp(int(time), tz=timezone.utc)
                if ((time := raw_data.get("repackFinishedTime")) and time != "0")
                else None
            ),
            destination_infos=raw_data["destinationInfos"],
            last_expanded_fseq=int(raw_data["lastExpandedFseq"]),
            start_info=RepackStartInfo(
                file_count=int(raw_data["totalFilesOnTapeAtStart"]),
                byte_count=int(raw_data["totalBytesOnTapeAtStart"]),
                creation_user_name=raw_data["creationLog"]["username"],
                creation_host_name=raw_data["creationLog"]["host"],
                creation_time=datetime.fromtimestamp(int(raw_data["creationLog"]["time"]), tz=timezone.utc),
                is_all_files_selected_at_start=raw_data["allFilesSelectedAtStart"],
            ),
            archive_statistics=RepackStatistics(
                total_file_count=int(raw_data["totalFilesToArchive"]),
                total_byte_count=int(raw_data["totalBytesToArchive"]),
                transferred_file_count=int(raw_data["archivedFiles"]),
                transferred_byte_count=int(raw_data["archivedBytes"]),
                failed_file_count=int(raw_data["failedToArchiveFiles"]),
                failed_byte_count=int(raw_data["failedToArchiveBytes"]),
            ),
            retrieve_statistics=RepackStatistics(
                total_file_count=int(raw_data["totalFilesToRetrieve"]),
                total_byte_count=int(raw_data["totalBytesToRetrieve"]),
                transferred_file_count=int(raw_data["retrievedFiles"]),
                transferred_byte_count=int(raw_data["retrievedBytes"]),
                failed_file_count=int(raw_data["failedToRetrieveFiles"]),
                failed_byte_count=int(raw_data["failedToRetrieveBytes"]),
            ),
        )

    @override
    def get(self, vid: str, /) -> RepackRequest:
        return self.raw_data_to_resource(self._wrapper.get(self.resource_type, {"vid": vid}))

    @override
    def list(self) -> Iterator[RepackRequest]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, vid: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"vid": vid})

    @override
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
        data = {"mountpolicy": mount_policy_name}
        data |= {} if buffer_url is None else {"bufferurl": buffer_url}
        data |= {} if max_files_to_select is None else {"maxfilestoselect": max_files_to_select}
        data |= {"no-recall": FLAG} if no_recall else {}
        data |= {} if custom_operation is None else {"just" + custom_operation.lower(): FLAG}

        self._wrapper.create(self.resource_type, {"vid": vid}, data=data)
