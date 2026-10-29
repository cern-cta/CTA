# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import AdminUserDao
from cta.sdk.models import AdminUser


class AdminUserDaoFile(AdminUserDao):
    resource_type = AdminUser

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(self, name: str, /, *, comment: str = "From python cta-sdk") -> None:
        user = AdminUser(
            name,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(user)

    def get(self, name: str, /) -> AdminUser:
        return self._handler.get(self.resource_type, name)

    def delete(self, name: str, /) -> None:
        self._handler.delete(self.resource_type, name)

    def list(self) -> Iterator[AdminUser]:
        yield from self._handler.iter(self.resource_type)

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        with self._handler.update(self.resource_type, name) as user:
            user.metadata = self._handler.refresh_metadata(user.metadata, comment=comment)
