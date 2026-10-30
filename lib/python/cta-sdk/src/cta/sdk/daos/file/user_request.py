# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import UserRequestDao
from cta.sdk.models import MountType, UserRequest
from typing_extensions import override


class UserRequestDaoFile(UserRequestDao):
    """File implementation of the user request Dao.

    Note:
        `UserRequestDao` only exposes `list` (plus the shared static `summary`): user requests are
        queued by the scheduler. Seed them through `CtaFileHandler.create`.
    """

    resource_type = UserRequest

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    @override
    def list(self, *, request_type: Optional[MountType] = None) -> Iterator[UserRequest]:
        yield from (
            user_request
            for user_request in self._handler.iter(self.resource_type)
            if request_type is None or user_request.mount_type == request_type
        )
