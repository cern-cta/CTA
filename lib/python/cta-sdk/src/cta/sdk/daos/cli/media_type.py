# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Optional

from cta.sdk.daos.cli._wrapper import CtaCliWrapper
from cta.sdk.daos.protocols import MediaTypeDao
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import MediaType, Metadata
from typing_extensions import override


class MediaTypeDaoCli(MediaTypeDao):
    def __init__(self, cli_wrapper: CtaCliWrapper):
        self._wrapper = cli_wrapper

    @property
    def resource_type(self) -> str:
        return "mediatype"

    @staticmethod
    def raw_data_to_resource(raw_data: dict[str, Any]) -> MediaType:
        return MediaType(
            name=raw_data["name"],
            cartridge_type=raw_data["cartridge"],
            capacity=int(raw_data["capacity"]),
            primary_density_code=int(raw_data["primaryDensityCode"]),
            secondary_density_code=int(raw_data["secondaryDensityCode"]),
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
    def get(self, name: str, /) -> MediaType:
        """Avoid to use multiple times, there is performance issue prefer list()."""
        try:
            raw_mt = next(raw_item for raw_item in self._wrapper.list(self.resource_type) if raw_item["name"] == name)
        except StopIteration as e:
            raise ResourceNotFoundError(self.resource_type, {"name": name}) from e
        return self.raw_data_to_resource(raw_mt)

    @override
    def list(self) -> Iterator[MediaType]:
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
        cartridge_type: str,
        capacity: int,
        primary_density_code: int = 0,
        secondary_density_code: int = 0,
        comment: str = "From python cta-sdk",
    ) -> None:
        data = {
            "cartridge": cartridge_type,
            "capacity": capacity,
            "primarydensitycode": primary_density_code,
            "secondarydensitycode": secondary_density_code,
            "comment": comment,
        }

        self._wrapper.create(
            self.resource_type,
            {"name": name},
            data=data,
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        cartridge_type: Optional[str] = None,
        primary_density_code: Optional[int] = None,
        secondary_density_code: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        data = {}
        data |= {} if cartridge_type is None else {"cartridge": cartridge_type}
        data |= {} if primary_density_code is None else {"primarydensitycode": primary_density_code}
        data |= {} if secondary_density_code is None else {"secondarydensitycode": secondary_density_code}
        data |= {} if comment is None else {"comment": comment}

        self._wrapper.update(
            self.resource_type,
            {
                "name": name,
            },
            data=data,
        )
