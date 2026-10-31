# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterator
from typing import Optional

from cta.sdk.daos.file._handler import CtaFileHandler
from cta.sdk.daos.protocols import PhysicalLibraryDao
from cta.sdk.exceptions import BackendError
from cta.sdk.models import LogicalLibrary, PhysicalLibrary, PhysicalLibraryState
from typing_extensions import override


class PhysicalLibraryDaoFile(PhysicalLibraryDao):
    resource_type = PhysicalLibrary

    def __init__(self, file_handler: CtaFileHandler):
        self._handler = file_handler

    @override
    def create(
        self,
        name: str,
        /,
        *,
        manufacturer: str,
        model: str,
        physical_cartridge_slots: int,
        physical_drive_slots: int,
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        type: Optional[str] = None,
        location: Optional[str] = None,
        available_cartridge_slots: Optional[int] = None,
        comment: str = "From python cta-sdk",
    ) -> None:
        physical_library = PhysicalLibrary(
            name,
            manufacturer=manufacturer,
            model=model,
            type=type or "",
            gui_url=gui_url or "",
            webcam_url=webcam_url or "",
            location=location or "",
            physical_cartridge_slots=physical_cartridge_slots,
            available_cartridge_slots=(
                available_cartridge_slots if available_cartridge_slots is not None else physical_cartridge_slots
            ),
            physical_drive_slots=physical_drive_slots,
            state=PhysicalLibraryState.ENABLED,
            state_reason=None,
            metadata=self._handler.generate_metadata(comment=comment),
        )
        self._handler.create(physical_library)

    @override
    def get(self, name: str, /) -> PhysicalLibrary:
        return self._handler.get(self.resource_type, name)

    @override
    def delete(self, name: str, /) -> None:
        for ll in self._handler.iter(LogicalLibrary):
            if ll.physical_library_name == name:
                raise BackendError(
                    f"You can not delete {self.resource_type}({name}), it is referenced by {ll} ",
                    2,
                    ll,
                )

        self._handler.delete(self.resource_type, name)

    @override
    def list(self, *, state: Optional[PhysicalLibraryState] = None) -> Iterator[PhysicalLibrary]:
        yield from (
            physical_library
            for physical_library in self._handler.iter(self.resource_type)
            if state is None or physical_library.state == state
        )

    @override
    def update(
        self,
        name: str,
        /,
        *,
        gui_url: Optional[str] = None,
        webcam_url: Optional[str] = None,
        location: Optional[str] = None,
        physical_cartridge_slots: Optional[int] = None,
        available_cartridge_slots: Optional[int] = None,
        physical_drive_slots: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> None:
        with self._handler.update(self.resource_type, name) as physical_library:
            if gui_url is not None:
                physical_library.gui_url = gui_url
            if webcam_url is not None:
                physical_library.webcam_url = webcam_url
            if location is not None:
                physical_library.location = location
            if physical_cartridge_slots is not None:
                physical_library.physical_cartridge_slots = physical_cartridge_slots
            if available_cartridge_slots is not None:
                physical_library.available_cartridge_slots = available_cartridge_slots
            if physical_drive_slots is not None:
                physical_library.physical_drive_slots = physical_drive_slots

            physical_library.metadata = self._handler.refresh_metadata(physical_library.metadata, comment=comment)

    @override
    def disable(self, name: str, /, *, reason: str) -> None:
        with self._handler.update(self.resource_type, name) as physical_library:
            physical_library.state = PhysicalLibraryState.DISABLED
            physical_library.state_reason = reason
            physical_library.metadata = self._handler.refresh_metadata(physical_library.metadata)

    @override
    def enable(self, name: str, /) -> None:
        with self._handler.update(self.resource_type, name) as physical_library:
            physical_library.state = PhysicalLibraryState.ENABLED
            physical_library.state_reason = None
            physical_library.metadata = self._handler.refresh_metadata(physical_library.metadata)
