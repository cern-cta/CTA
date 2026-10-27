# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional, Union

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import DiskSystemDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import DiskSystem, Metadata


class DiskSystemDaoCli(DiskSystemDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "disksystem"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> DiskSystem:
        return DiskSystem(
            raw_data["name"],
            disk_instance_name=raw_data["diskInstance"],
            disk_instance_space_name=raw_data["diskInstanceSpace"],
            file_regex=raw_data["fileRegexp"],
            desired_min_free_space=int(raw_data["targetedFreeSpace"]),
            sleep_time=timedelta(seconds=int(raw_data["sleepTime"])),
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

    def get(self, name: str, /) -> DiskSystem:
        """Avoid to use multiple times, there is performance issue prefer list()"""

        try:
            raw_ds = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_ds)

    def list(self) -> Iterator[DiskSystem]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {self.resource_type: name})

    def create(
        self,
        name: str,
        /,
        *,
        disk_instance_name: str,
        disk_instance_space_name: str,
        file_regex: str,
        desired_min_free_space: int = 1_000_000_000_000,
        sleep_time: Union[int, timedelta] = 3600,
        comment: str = "From python cta-sdk",
    ) -> None:
        self._wrapper.create(
            self.resource_type,
            {"disksystem": name},
            data={
                "diskinstance": disk_instance_name,
                "diskinstancespace": disk_instance_space_name,
                "fileregexp": file_regex,
                "targetedfreespace": desired_min_free_space,
                "sleeptime": int(sleep_time.total_seconds()) if isinstance(sleep_time, timedelta) else sleep_time,
                "comment": comment,
            },
        )

    def update(
        self,
        name: str,
        /,
        *,
        file_regex: Optional[str] = None,
        desired_min_free_space: Optional[int] = None,
        sleep_time: Union[int, timedelta, None] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if file_regex is None else {"fileregexp": file_regex}
        data |= {} if desired_min_free_space is None else {"targetedfreespace": desired_min_free_space}
        data |= (
            {}
            if sleep_time is None
            else {"sleeptime": int(sleep_time.total_seconds()) if isinstance(sleep_time, timedelta) else sleep_time}
        )
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(self.resource_type, {"disksystem": name}, data=data)
