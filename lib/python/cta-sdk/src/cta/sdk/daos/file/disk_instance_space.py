# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Iterator, Optional, Union

from cta.sdk.daos.file._handler import CtaFileHandler, as_timedelta
from cta.sdk.daos.protocols import DiskInstanceSpaceDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import DiskInstance, DiskInstanceSpace, DiskSystem


class DiskInstanceSpaceDaoFile(DiskInstanceSpaceDao):
    resource_type = DiskInstanceSpace

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        try:
            self._handler.get(DiskInstance, disk_instance_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        disk_instance_space = DiskInstanceSpace(
            name,
            disk_instance_name,
            free_space_url=free_space_url,
            refresh_interval=as_timedelta(refresh_interval),
            free_space=0,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(disk_instance_space)

    def get(self, name: str, disk_instance_name: str, /) -> DiskInstanceSpace:
        return self._handler.get(self.resource_type, name, disk_instance_name)

    def delete(self, name: str, disk_instance_name: str, /) -> None:
        for ds in self._handler.iter(DiskSystem):
            if ds.disk_instance_space_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {ds} ",
                    2,
                    ds,
                )
        self._handler.delete(self.resource_type, name, disk_instance_name)

    def list(self) -> Iterator[DiskInstanceSpace]:
        yield from self._handler.iter(self.resource_type)

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
        with self._handler.update(self.resource_type, name, disk_instance_name) as disk_instance_space:
            if free_space_url is not None:
                disk_instance_space.free_space_url = free_space_url
            if refresh_interval is not None:
                disk_instance_space.refresh_interval = as_timedelta(refresh_interval)

            disk_instance_space.metadata = self._handler.refresh_metadata(disk_instance_space.metadata, comment=comment)
