# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import builtins
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import FLAG, CtaCliWrapper
from cta.sdk.daos.protocols import FailedRequestDao
from cta.sdk.models import Checksum, DiskFile, FailedRequest, FailedRequestSummary, MountType, TapeFile
from typing_extensions import override


class FailedRequestDaoCli(FailedRequestDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "failedrequest"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> FailedRequest:
        return FailedRequest(
            id=raw_data["objectId"],
            mount_type=(MountType.RETRIEVE if raw_data["requestType"].startswith("RETRIEVE") else MountType.ARCHIVE),
            requester_name=raw_data["requester"]["username"],
            requester_group=raw_data["requester"]["groupname"],
            tape_file=TapeFile(
                int(raw_data["af"]["archiveId"]),
                storage_class_name=raw_data["af"]["storageClass"],
                creation_time=datetime.fromtimestamp(int(raw_data["af"]["creationTime"]), tz=timezone.utc),
                size=int(raw_data["af"]["size"]),
                checksum=Checksum(
                    algo_name="UNKNOWN",
                    value="UNKNOWN",
                ),
                disk_file=DiskFile(
                    id=raw_data["af"].get("diskId", ""),
                    disk_instance_name=raw_data["af"]["diskInstance"],
                    uid=-1,
                    gid=-1,
                    path=raw_data["af"]["df"]["path"],
                ),
                vid=str(raw_data.get("tf", {}).get("vid")),
                block_id=int(raw_data.get("tf", {}).get("blockId", 0)),
                fseq=int(raw_data.get("tf", {}).get("fSeq", 0)),
            ),
            tape_pool_name=raw_data["tapepool"],
            total_retries=int(raw_data["totalretries"]),
            logs="\n".join(raw_data["failurelogs"]),
        )

    @override
    def list(
        self,
        *,
        type: Optional[MountType] = None,
        tape_pool_name: Optional[str] = None,
        vid: Optional[str] = None,
        logs: bool = False,
    ) -> Iterator[FailedRequest]:
        filters = {}
        filters |= {} if type is None else {"just" + type.name.lower(): FLAG}
        filters |= {} if tape_pool_name is None else {"tapepool": tape_pool_name}
        filters |= {} if vid is None else {"vid": vid}
        filters |= {"log": FLAG} if logs else {}

        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.iter(self.resource_type, filters=filters)
        )

    @override
    def delete(self, id: str) -> None:
        self._wrapper.delete(self.resource_type, {"objectid": id})

    @override
    def summary(self) -> builtins.list[FailedRequestSummary]:
        return [
            FailedRequestSummary(
                mount_type=(
                    MountType.RETRIEVE if raw_item["requestType"].startswith("RETRIEVE") else MountType.ARCHIVE
                ),
                total_file_count=int(raw_item["totalFiles"]),
                total_byte_count=int(raw_item["totalSize"]),
                scheduler_backend_name=raw_item["schedulerBackendName"],
            )
            for raw_item in self._wrapper.list(self.resource_type, {"summary": FLAG})
            if raw_item["requestType"] != "TOTAL"
        ]
