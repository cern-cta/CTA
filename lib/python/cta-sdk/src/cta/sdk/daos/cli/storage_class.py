# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import StorageClassDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import Metadata, StorageClass
from typing_extensions import override


class StorageClassDaoCli(StorageClassDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "storageclass"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> StorageClass:
        return StorageClass(
            raw_data["name"],
            virtual_organisation_name=raw_data["vo"],
            replica_count=int(raw_data["nbCopies"]),
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
    def get(self, name: str, /) -> StorageClass:
        try:
            raw_sc = self._wrapper.get(self.resource_type, {"name": name})
        except BackendError as e:
            # issue in cta-cli no return code=2 and not [] on stdout
            if "This result set is invalid" in e.error:
                raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
            else:
                raise

        return self.raw_data_to_resource(raw_sc)

    @override
    def list(self) -> Iterator[StorageClass]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    @override
    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name})

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
        self._wrapper.create(
            self.resource_type,
            {
                "name": name,
            },
            data={
                "virtualorganisation": virtual_organisation_name,
                "numberofcopies": replica_count,
                "comment": comment,
            },
        )

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
        data = {}
        data |= {} if virtual_organisation_name is None else {"virtualorganisation": virtual_organisation_name}
        data |= {} if replica_count is None else {"numberofcopies": replica_count}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )
