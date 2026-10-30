# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import ArchiveRouteDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import ArchiveRoute, ArchiveRouteType, Metadata
from typing_extensions import override


class ArchiveRouteDaoCli(ArchiveRouteDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "archiveroute"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> ArchiveRoute:
        return ArchiveRoute(
            storage_class_name=raw_data["storageClass"],
            copy_number=int(raw_data["copyNumber"]),
            type=ArchiveRouteType[raw_data["archiveRouteType"]],
            tape_pool_name=raw_data["tapepool"],
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
    def get(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> ArchiveRoute:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_ar = next(
                raw_item
                for raw_item in self._wrapper.list(self.resource_type)
                if raw_item["storageClass"] == storage_class_name
                and int(raw_item["copyNumber"]) == copy_number
                and raw_item["archiveRouteType"] == type.name
            )
        except StopIteration as e:
            raise ResourceNotFoundError(
                self.resource_type,
                {
                    "storageClass": storage_class_name,
                    "copyNumber": copy_number,
                    "archiveRouteType": type.name,
                },
            ) from e

        return self.raw_data_to_resource(raw_ar)

    @override
    def list(self) -> Iterator[ArchiveRoute]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, storage_class_name: str, copy_number: int, type: ArchiveRouteType, /) -> None:
        self._wrapper.delete(
            self.resource_type,
            {"storageclass": storage_class_name, "copynb": copy_number, "archiveroutetype": type.name},
        )

    @override
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
        self._wrapper.create(
            self.resource_type,
            {
                "storageclass": storage_class_name,
                "copynb": copy_number,
                "archiveroutetype": type.name,
            },
            data={
                "tapepool": tape_pool_name,
                "comment": comment,
            },
        )

    @override
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
        data = {}
        data |= {} if tape_pool_name is None else {"tapepool": tape_pool_name}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "storageclass": storage_class_name,
                "copynb": copy_number,
                "archiveroutetype": type.name,
            },
            data=data,
        )
