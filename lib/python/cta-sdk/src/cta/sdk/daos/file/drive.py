# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import DriveDao
from cta.sdk.models import Drive, DriveOperatingState


class DriveDaoFile(DriveDao):
    """File implementation of the drive Dao.

    Note:
        `DriveDao` has no `create`: real drives register themselves through `cta-taped`.
        Seed drives by building a `Drive` and passing it to `CtaFileHandler.create`.
    """

    resource_type = Drive

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def get(self, name: str, /) -> Drive:
        return self._handler.get(self.resource_type, name)

    def delete(self, name: str, /, *, force: bool = False) -> None:
        # `force` only matters against a live drive holding a tape, which the file backend cannot model.
        self._handler.delete(self.resource_type, name)

    def list(self, /, *, all: bool = False) -> Iterator[Drive]:
        # `all` widens the query beyond the instance the command is run against; every drive in the
        # root directory is already visible here, so the flag makes no difference.
        yield from self._handler.iter(self.resource_type)

    def update(self, name: str, /, *, comment: Optional[str] = None) -> None:
        with self._handler.update(self.resource_type, name) as drive:
            if comment is not None:
                drive.metadata.comment = comment

    def down(self, name: str, /, *, reason: str, force: bool = False) -> None:
        with self._handler.update(self.resource_type, name) as drive:
            drive.desired_state = DriveOperatingState.DOWN
            # No cta-taped to reconcile desired_state into state, so apply it directly.
            drive.state = DriveOperatingState.DOWN
            drive.state_reason = reason

    def up(self, name: str, /, *, reason: Optional[str] = None) -> None:
        with self._handler.update(self.resource_type, name) as drive:
            drive.desired_state = DriveOperatingState.UP
            drive.state = DriveOperatingState.UP
            drive.state_reason = reason if reason is not None else ""
