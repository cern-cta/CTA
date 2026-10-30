# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Union

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import RecycleBinTapeFileDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import Checksum, DiskFile, RecycleBinTapeFile
from typing_extensions import override


class RecycleBinTapeFileDaoCli(RecycleBinTapeFileDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "recycletf"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> RecycleBinTapeFile:
        return RecycleBinTapeFile(
            int(raw_data["archiveFileId"]),
            storage_class_name=raw_data["storageClass"],
            creation_time=datetime.fromtimestamp(int(raw_data["archiveFileCreationTime"]), tz=timezone.utc),
            size=int(raw_data["sizeInBytes"]),
            checksum=Checksum(algo_name=raw_data["checksum"][0]["type"], value=raw_data["checksum"][0]["value"]),
            disk_file=DiskFile(
                id=raw_data["diskFileId"],
                disk_instance_name=raw_data["diskInstance"],
                uid=int(raw_data["diskFileUid"]),
                gid=int(raw_data["diskFileGid"]),
                path=raw_data["diskFilePath"],
            ),
            vid=str(raw_data["vid"]),
            block_id=int(raw_data["blockId"]),
            fseq=int(raw_data["fseq"]),
            deletion_time=datetime.fromtimestamp(int(raw_data["recycleLogTime"]), tz=timezone.utc),
            deletion_reason=raw_data["reasonLog"],
            virtual_organisation_name=raw_data["virtualOrganization"],
        )

    @override
    def get_all_copies(self, *args: Union[int, str]) -> tuple[RecycleBinTapeFile, ...]:
        resource_id = {"id": args[0]} if len(args) == 1 else {"diskfileid": args[0], "instance": args[1]}
        tape_files = tuple(
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type, resource_id)
        )
        if not tape_files:
            raise ResourceNotFoundError(self.resource_type, resource_id)
        return tape_files

    @override
    def get(self, *args: Union[int, str], vid: str) -> RecycleBinTapeFile:
        resource_id = (
            {"id": args[0], "vid": vid} if len(args) == 1 else {"diskfileid": args[0], "instance": args[1], "vid": vid}
        )

        return self.raw_data_to_resource(self._wrapper.get(self.resource_type, resource_id))

    @override
    def list(self, **kwargs: object) -> Iterator[RecycleBinTapeFile]:
        filters = {}
        filters |= {} if kwargs.get("vid") is None else {"vid": kwargs["vid"]}
        filters |= (
            {"fxidfile": kwargs["fids_file"], "instance": kwargs["disk_instance_name"]}
            if "fids_file" in kwargs and "disk_instance_name" in kwargs
            else {}
        )
        filters |= (
            {}
            if kwargs.get("virtual_organisation_name") is None
            else {"virtualorganisation": kwargs["virtual_organisation_name"]}
        )
        filters |= (
            {}
            if kwargs.get("min_deletion_time") is None
            else {"logunixtimemin": int(r.timestamp()) if isinstance(r := kwargs["min_deletion_time"], datetime) else r}
        )
        filters |= (
            {}
            if kwargs.get("max_deletion_time") is None
            else {"logunixtimemax": int(r.timestamp()) if isinstance(r := kwargs["max_deletion_time"], datetime) else r}
        )

        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.iter(self.resource_type, filters=filters)
        )
