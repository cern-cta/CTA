# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Iterator, Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import LogicalLibraryDao
from cta.sdk.exceptions import BackendError, ResourceNotFoundError
from cta.sdk.models import LogicalLibrary, LogicalLibraryState, PhysicalLibrary, Tape


class LogicalLibraryDaoFile(LogicalLibraryDao):
    resource_type = LogicalLibrary

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    def create(
        self,
        name: str,
        /,
        *,
        physical_library_name: str,
        state: LogicalLibraryState = LogicalLibraryState.ENABLED,
        comment: str = "From python cta-sdk",
    ) -> None:
        try:
            self._handler.get(PhysicalLibrary, physical_library_name)
        except ResourceNotFoundError as e:
            raise BackendError("Linked resource does not exist", 2, {}) from e
        logical_library = LogicalLibrary(
            name,
            physical_library_name=physical_library_name,
            state=state,
            state_reason=None,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(logical_library)

    def get(self, name: str, /) -> LogicalLibrary:
        return self._handler.get(self.resource_type, name)

    def delete(self, name: str, /) -> None:
        for tape in self._handler.iter(Tape):
            if tape.logical_library_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {tape} ",
                    2,
                    tape,
                )

        self._handler.delete(self.resource_type, name)

    def list(self, *, state: Optional[LogicalLibraryState] = None) -> Iterator[LogicalLibrary]:
        yield from (
            logical_library
            for logical_library in self._handler.iter(self.resource_type)
            if state is None or logical_library.state == state
        )

    def update(
        self,
        name: str,
        /,
        *,
        physical_library_name: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, name) as logical_library:
            if physical_library_name is not None:
                try:
                    logical_library.physical_library_name = self._handler.get(
                        PhysicalLibrary, physical_library_name
                    ).name
                except ResourceNotFoundError as e:
                    raise BackendError("Linked resource does not exist", 2, {}) from e

            logical_library.metadata = self._handler.refresh_metadata(logical_library.metadata, comment=comment)

    def disable(self, name: str, /, *, reason: str) -> None:
        with self._handler.update(self.resource_type, name) as logical_library:
            logical_library.state = LogicalLibraryState.DISABLED
            logical_library.state_reason = reason
            logical_library.metadata = self._handler.refresh_metadata(logical_library.metadata)

    def enable(self, name: str, /) -> None:
        with self._handler.update(self.resource_type, name) as logical_library:
            logical_library.state = LogicalLibraryState.ENABLED
            logical_library.state_reason = None
            logical_library.metadata = self._handler.refresh_metadata(logical_library.metadata)
