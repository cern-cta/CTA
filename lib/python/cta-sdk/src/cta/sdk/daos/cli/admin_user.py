# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import datetime, timezone
from typing import Iterator, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import AdminUserDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import AdminUser, Metadata


class AdminUserDaoCli(AdminUserDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "admin"

    @staticmethod
    def raw_data_to_resource(raw_data: dict) -> AdminUser:
        return AdminUser(
            raw_data["user"],
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

    def get(self, name: str, /) -> AdminUser:
        """Avoid to use multiple times, there is performance issue prefer list()"""
        try:
            raw_ad = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["user"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"user": name}) from e
        return self.raw_data_to_resource(raw_ad)

    def list(self) -> Iterator[AdminUser]:
        yield from (self.raw_data_to_resource(raw_item) for raw_item in self._wrapper.list(self.resource_type))

    def delete(self, name: str, /) -> None:
        self._wrapper.delete(self.resource_type, {"username": name})

    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        self._wrapper.create(self.resource_type, {"username": name}, data={"comment": comment})

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        data = {}
        data |= {} if comment is None else {"comment": comment}
        self._wrapper.update(self.resource_type, {"username": name}, data=data)
