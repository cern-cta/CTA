# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import MediaTypeDao
from cta.sdk.exceptions import BackendError
from cta.sdk.models import MediaType, Tape
from typing_extensions import override


class MediaTypeDaoFile(MediaTypeDao):
    resource_type = MediaType

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

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
        media_type = MediaType(
            name,
            cartridge_type=cartridge_type,
            capacity=capacity,
            primary_density_code=primary_density_code,
            secondary_density_code=secondary_density_code,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(media_type)

    @override
    def get(self, name: str, /) -> MediaType:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for tape in self._handler.iter(Tape):
            if tape.media_type_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {tape} ",
                    2,
                    tape,
                )
        self._handler.delete(self.resource_type, name)

    @override
    def list(self) -> Iterator[MediaType]:
        yield from self._handler.iter(self.resource_type)

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
        with self._handler.update(self.resource_type, name) as media_type:
            if cartridge_type is not None:
                media_type.cartridge_type = cartridge_type
            if primary_density_code is not None:
                media_type.primary_density_code = primary_density_code
            if secondary_density_code is not None:
                media_type.secondary_density_code = secondary_density_code

            media_type.metadata = self._handler.refresh_metadata(media_type.metadata, comment=comment)
