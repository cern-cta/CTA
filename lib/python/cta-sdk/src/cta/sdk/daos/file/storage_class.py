# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from itertools import chain
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import StorageClassDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import ArchiveRoute, RecycleBinTapeFile, StorageClass, TapeFile, VirtualOrganisation
from typing_extensions import override


class StorageClassDaoFile(StorageClassDao):
    resource_type = StorageClass

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    @override
    def create(
        self,
        name: str,
        /,
        *,
        virtual_organisation_name: str,
        replica_count: int = 1,
        comment: str = "From python cta-sdk",
    ) -> None:
        try:
            self._handler.get(VirtualOrganisation, virtual_organisation_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        storage_class = StorageClass(
            name,
            virtual_organisation_name=virtual_organisation_name,
            replica_count=replica_count,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(storage_class)

    @override
    def get(self, name: str, /) -> StorageClass:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for obj in chain(
            self._handler.iter(TapeFile),
            self._handler.iter(RecycleBinTapeFile),
            self._handler.iter(ArchiveRoute),
        ):
            if obj.storage_class_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {obj} ",
                    2,
                    obj,
                )
        self._handler.delete(self.resource_type, name)

    @override
    def list(self) -> Iterator[StorageClass]:
        yield from self._handler.iter(self.resource_type)

    @override
    def update(
        self,
        name: str,
        /,
        *,
        virtual_organisation_name: Optional[str] = None,
        replica_count: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, name) as storage_class:
            if virtual_organisation_name is not None:
                try:
                    storage_class.virtual_organisation_name = self._handler.get(
                        VirtualOrganisation, virtual_organisation_name
                    ).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e
            if replica_count is not None:
                storage_class.replica_count = replica_count

            storage_class.metadata = self._handler.refresh_metadata(storage_class.metadata, comment=comment)
