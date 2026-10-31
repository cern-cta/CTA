# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Union

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import DiskInstanceSpaceDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import DiskInstanceSpace, Metadata
from typing_extensions import override


class DiskInstanceSpaceDaoCli(DiskInstanceSpaceDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "diskinstancespace"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> DiskInstanceSpace:
        return DiskInstanceSpace(
            name=raw_data["name"],
            disk_instance_name=raw_data["diskInstance"],
            free_space_url=raw_data["freeSpaceQueryUrl"],
            refresh_interval=timedelta(seconds=int(raw_data["refreshInterval"])),
            free_space=int(raw_data["freeSpace"]),
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
    def get(self, name: str, disk_instance: str, /) -> DiskInstanceSpace:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_dis = next(
                raw_item
                for raw_item in self._wrapper.list(self.resource_type)
                if raw_item["name"] == name and raw_item["diskInstance"] == disk_instance
            )
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name, "diskInstance": disk_instance}) from e
        return self.raw_data_to_resource(raw_dis)

    @override
    def list(self) -> Iterator[DiskInstanceSpace]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, name: str, disk_instance: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name, "diskinstance": disk_instance})

    @override
    def create(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        free_space_url: str,
        refresh_interval: Union[int, timedelta] = 10,
        comment: str = "From python cta-sdk",
    ) -> None:
        self._wrapper.create(
            self.resource_type,
            {"name": name, "diskinstance": disk_instance_name},
            data={
                "freespacequeryurl": free_space_url,
                "refreshinterval": (
                    int(refresh_interval.total_seconds())
                    if isinstance(refresh_interval, timedelta)
                    else refresh_interval
                ),
                "comment": comment,
            },
        )

    @override
    def update(
        self,
        name: str,
        disk_instance_name: str,
        /,
        *,
        free_space_url: Optional[str] = None,
        refresh_interval: Union[int, timedelta, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if free_space_url is None else {"freespacequeryurl": free_space_url}
        data |= (
            {}
            if refresh_interval is None
            else {
                "refreshinterval": (
                    int(refresh_interval.total_seconds())
                    if isinstance(refresh_interval, timedelta)
                    else refresh_interval
                )
            }
        )
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(self.resource_type, {"name": name, "diskinstance": disk_instance_name}, data=data)
