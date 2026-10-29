# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timezone
from typing import Iterator, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import DiskInstanceDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import DiskInstance, Metadata


class DiskInstanceDaoCli(DiskInstanceDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "diskinstance"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> DiskInstance:
        return DiskInstance(
            raw_data["name"],
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

    def get(self, name: str, /) -> DiskInstance:
        """Avoid to use multiple times, there is performance issue prefer list()"""

        try:
            raw_di = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_di)

    def list(self) -> Iterator[DiskInstance]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"name": name})

    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        self._wrapper.create(self.resource_type, {"name": name}, data={"comment": comment})

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        data = {}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(self.resource_type, {"name": name}, data=data)
