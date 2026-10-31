# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from itertools import chain
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import VirtualOrganisationDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import DiskInstance, StorageClass, TapePool, VirtualOrganisation
from typing_extensions import override


class VirtualOrganisationDaoFile(VirtualOrganisationDao):
    resource_type = VirtualOrganisation

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        try:
            self._handler.get(DiskInstance, disk_instance_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        virtual_organisation = VirtualOrganisation(
            name,
            disk_instance_name=disk_instance_name,
            is_repack_vo=is_repack_vo,
            max_read_drives=max_read_drives,
            max_write_drives=max_write_drives,
            max_file_size=max_file_size,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(virtual_organisation)

    @override
    def get(self, name: str, /) -> VirtualOrganisation:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for obj in chain(
            self._handler.iter(StorageClass),
            self._handler.iter(TapePool),
        ):
            if obj.virtual_organisation_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {obj} ",
                    2,
                    obj,
                )
        self._handler.delete(self.resource_type, name)

    @override
    def list(self) -> Iterator[VirtualOrganisation]:
        yield from self._handler.iter(self.resource_type)

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
        with self._handler.update(self.resource_type, name) as virtual_organisation:
            if disk_instance_name is not None:
                try:
                    virtual_organisation.disk_instance_name = self._handler.get(DiskInstance, disk_instance_name).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e
            if is_repack_vo is not None:
                virtual_organisation.is_repack_vo = is_repack_vo
            if max_read_drives is not None:
                virtual_organisation.max_read_drives = max_read_drives
            if max_write_drives is not None:
                virtual_organisation.max_write_drives = max_write_drives
            if max_file_size is not None:
                virtual_organisation.max_file_size = max_file_size

            virtual_organisation.metadata = self._handler.refresh_metadata(
                virtual_organisation.metadata, comment=comment
            )
