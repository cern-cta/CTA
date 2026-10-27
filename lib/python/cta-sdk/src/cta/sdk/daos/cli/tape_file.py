# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timezone
from typing import Iterator

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import TapeFileDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import Checksum, DiskFile, TapeFile


class TapeFileDaoCli(TapeFileDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "tapefile"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> TapeFile:
        return TapeFile(
            int(raw_data["af"]["archiveId"]),
            storage_class_name=raw_data["af"]["storageClass"],
            creation_time=datetime.fromtimestamp(int(raw_data["af"]["creationTime"]), tz=timezone.utc),
            size=int(raw_data["af"]["size"]),
            checksum=Checksum(
                algo_name=raw_data["af"]["checksum"][0]["type"], value=raw_data["af"]["checksum"][0]["value"]
            ),
            disk_file=DiskFile(
                id=raw_data["df"]["diskId"],
                disk_instance_name=raw_data["df"]["diskInstance"],
                uid=raw_data["df"]["ownerId"]["uid"],
                gid=raw_data["df"]["ownerId"]["gid"],
                path=raw_data["df"]["path"],
            ),
            vid=str(raw_data["tf"]["vid"]),
            block_id=int(raw_data["tf"]["blockId"]),
            fseq=int(raw_data["tf"]["fSeq"]),
        )

    def get_all_copies(self, *args) -> tuple[TapeFile, ...]:
        id = {"id": args[0]} if len(args) == 1 else {"diskfileid": args[0], "instance": args[1]}
        tape_files = tuple(
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type, id)
        )
        if not tape_files:
            raise ResourceNotFoundError(self.resource_type, id)
        return tape_files

    def get(self, *args, vid: str) -> TapeFile:
        id = {"id": args[0], "vid": vid} if len(args) == 1 else {"diskfileid": args[0], "instance": args[1], "vid": vid}

        return self.raw_data_to_resource(self._wrapper.get(self.resource_type, id))

    def list(self, **kwargs) -> Iterator[TapeFile]:
        filters = {}
        filters |= {"vid": kwargs["vid"]} if "vid" in kwargs else {}
        filters |= (
            {"fxidfile": kwargs["fids_file"], "instance": kwargs["disk_instance_name"]} if "fids_file" in kwargs else {}
        )

        yield from (
            self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.iter(self.resource_type, filters=filters)
        )

    def delete(self, *args, vid: str, reason: str) -> None:
        id = {"id": args[0]} if len(args) == 1 else {"diskfileid": args[0], "intance": args[1]}
        id |= {"vid": vid, "reason": reason}
        self._wrapper.delete("tapefile", id)
