# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import ArchiveRouteDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import ArchiveRoute, ArchiveRouteType, StorageClass, TapePool


class ArchiveRouteDaoFile(ArchiveRouteDao):
    resource_type = ArchiveRoute

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(
        self,
        storage_class_name: str,
        copy_number: int,
        type: ArchiveRouteType,
        /,
        *,
        tape_pool_name: str,
        comment: str = "From python cta-sdk",
    ) -> None:
        try:
            self._handler.get(StorageClass, storage_class_name)
            self._handler.get(TapePool, tape_pool_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e

        archive_route = ArchiveRoute(
            storage_class_name,
            copy_number,
            type,
            tape_pool_name=tape_pool_name,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(archive_route)

    def get(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> ArchiveRoute:
        return self._handler.get(self.resource_type, storage_class_name, copy_number, type)

    def delete(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> None:
        self._handler.delete(self.resource_type, storage_class_name, copy_number, type)

    def list(self) -> Iterator[ArchiveRoute]:
        yield from self._handler.iter(self.resource_type)

    def update(
        self,
        storage_class_name: str,
        copy_number: int,
        type: ArchiveRouteType,
        /,
        *,
        tape_pool_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, storage_class_name, copy_number, type) as archive_route:
            if tape_pool_name is not None:
                try:
                    archive_route.tape_pool_name = self._handler.get(TapePool, tape_pool_name).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e
                archive_route.tape_pool_name = tape_pool_name

            archive_route.metadata = self._handler.refresh_metadata(archive_route.metadata, comment=comment)
