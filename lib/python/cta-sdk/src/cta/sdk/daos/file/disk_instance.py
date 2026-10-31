# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from itertools import chain
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import DiskInstanceDao
from cta.sdk.exceptions import BackendError
from cta.sdk.models import (
    ActivityMountRule,
    DiskInstance,
    DiskInstanceSpace,
    GroupMountRule,
    TapeFile,
    UserMountRule,
    VirtualOrganisation,
)
from typing_extensions import override


class DiskInstanceDaoFile(DiskInstanceDao):
    resource_type = DiskInstance

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    @override
    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        disk_instance = DiskInstance(
            name,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(disk_instance)

    @override
    def get(self, name: str, /) -> DiskInstance:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for md in chain(
            self._handler.iter(UserMountRule),
            self._handler.iter(GroupMountRule),
            self._handler.iter(ActivityMountRule),
            self._handler.iter(DiskInstanceSpace),
            self._handler.iter(VirtualOrganisation),
        ):
            if md.disk_instance_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {md} ",
                    2,
                    md,
                )
        for tf in self._handler.iter(TapeFile):
            if tf.disk_file.disk_instance_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {tf} ",
                    2,
                    tf,
                )

        self._handler.delete(self.resource_type, name)

    @override
    def list(self) -> Iterator[DiskInstance]:
        yield from self._handler.iter(self.resource_type)

    @override
    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        with self._handler.update(self.resource_type, name) as disk_instance:
            disk_instance.metadata = self._handler.refresh_metadata(disk_instance.metadata, comment=comment)
