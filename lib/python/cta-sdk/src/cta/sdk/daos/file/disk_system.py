# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Iterator, Optional, Union

from cta.sdk.daos.file._handler import CtaFileHandler, as_timedelta
from cta.sdk.daos.protocols import DiskSystemDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import DiskInstanceSpace, DiskSystem


class DiskSystemDaoFile(DiskSystemDao):
    resource_type = DiskSystem

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        try:
            self._handler.get(DiskInstanceSpace, disk_instance_space_name, disk_instance_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        disk_system = DiskSystem(
            name,
            disk_instance_name=disk_instance_name,
            disk_instance_space_name=disk_instance_space_name,
            file_regex=file_regex,
            desired_min_free_space=desired_min_free_space,
            sleep_time=as_timedelta(sleep_time),
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(disk_system)

    def get(self, name: str, /) -> DiskSystem:
        return self._handler.get(self.resource_type, name)

    def delete(self, name: str, /) -> None:
        self._handler.delete(self.resource_type, name)

    def list(self) -> Iterator[DiskSystem]:
        yield from self._handler.iter(self.resource_type)

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
        with self._handler.update(self.resource_type, name) as disk_system:
            if file_regex is not None:
                disk_system.file_regex = file_regex
            if desired_min_free_space is not None:
                disk_system.desired_min_free_space = desired_min_free_space
            if sleep_time is not None:
                disk_system.sleep_time = as_timedelta(sleep_time)

            disk_system.metadata = self._handler.refresh_metadata(disk_system.metadata, comment=comment)
