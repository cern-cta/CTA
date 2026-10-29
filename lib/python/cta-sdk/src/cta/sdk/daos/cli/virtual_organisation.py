# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import VirtualOrganisationDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import Metadata, VirtualOrganisation
from typing_extensions import override


class VirtualOrganisationDaoCli(VirtualOrganisationDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "virtualorganization"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> VirtualOrganisation:
        return VirtualOrganisation(
            raw_data["name"],
            disk_instance_name=raw_data["diskinstance"],
            is_repack_vo=raw_data["isRepackVo"],
            max_read_drives=int(raw_data["readMaxDrives"]),
            max_write_drives=int(raw_data["writeMaxDrives"]),
            max_file_size=int(raw_data["maxFileSize"]),
            metadata=Metadata(
                raw_data["creationLog"]["username"],
                raw_data["creationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["creationLog"]["time"]), tz=timezone.utc),
                raw_data["lastModificationLog"]["username"],
                raw_data["lastModificationLog"]["host"],
                datetime.fromtimestamp(int(raw_data["lastModificationLog"]["time"]), tz=timezone.utc),
                raw_data["comment"],
            ),
        )

    @override
    def get(self, name: str, /) -> VirtualOrganisation:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_vo = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_vo)

    @override
    def list(self) -> Iterator[VirtualOrganisation]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"virtualorganisation": name})

    @override
    def create(
        self,
        name: str,
        /,
        *,
        disk_instance_name: str,
        is_repack_vo: bool = False,
        max_read_drives: int = 1,
        max_write_drives: int = 1,
        max_file_size: int = 1_000_000_000_000,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "diskinstance": disk_instance_name,
            "isrepackvo": str(is_repack_vo).lower(),
            "readmaxdrives": max_read_drives,
            "writemaxdrives": max_write_drives,
            "maxfilesize": max_file_size,
            "comment": comment,
        }
        self._wrapper.create(
            self.resource_type,
            {
                "virtualorganisation": name,
            },
            data=data,
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        disk_instance_name: Optional[str] = None,
        is_repack_vo: Optional[bool] = None,
        max_read_drives: Optional[int] = None,
        max_write_drives: Optional[int] = None,
        max_file_size: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if disk_instance_name is None else {"diskinstance": disk_instance_name}
        data |= {} if is_repack_vo is None else {"isrepackvo": str(is_repack_vo).lower()}
        data |= {} if max_read_drives is None else {"readmaxdrives": max_read_drives}
        data |= {} if max_write_drives is None else {"writemaxdrives": max_write_drives}
        data |= {} if max_file_size is None else {"maxfilesize": max_file_size}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "virtualorganisation": name,
            },
            data=data,
        )
