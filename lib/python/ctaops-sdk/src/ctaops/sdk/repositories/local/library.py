# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""udev-backed implementations of the library repository protocols.

See [ctaops.sdk.repositories.protocols.library][ctaops.sdk.repositories.protocols.library]
for the public contracts. The classes in this module enumerate
locally-attached SCSI medium changers via pyudev.
"""

import re
from abc import ABC, abstractmethod
from collections.abc import Container
from logging import getLogger
from typing import Optional

from ctaops.sdk.exceptions import ResourceNotFoundError
from ctaops.sdk.repositories.protocols.library import (
    Library,
    LibraryRepository,
    NamedLibrary,
    NamedLibraryRepository,
)
from pyudev import Context, Device
from typing_extensions import override

LOGGER = getLogger("ctaops.sdk")


class _LocalAbstractLibraryRepository(ABC):
    def __init__(self, context: Optional[Context] = None):
        self._context = Context() if context is None else context

    def _list_media_changer_sg_devices(self) -> set[Device]:
        """Return the set of `sgN` medium-changer devices visible locally."""
        LOGGER.debug("Listing local sgN medium-changer devices")
        return {
            device
            for device in self._context.list_devices(subsystem="scsi_generic", SCSI_TYPE="medium changer")
            if re.match(r"^sg[0-9]+$", device.sys_name)
        }

    @abstractmethod
    def list_libraries(self) -> Container[Library]:
        pass

    @abstractmethod
    def get_library(self, serial_number: str) -> Library:
        pass


class LocalLibraryRepository(_LocalAbstractLibraryRepository, LibraryRepository):
    """udev-backed [LibraryRepository][ctaops.sdk.repositories.protocols.library.LibraryRepository].

    Libraries are discovered via the `scsi_generic` subsystem filtered to
    `SCSI_TYPE=medium changer` (`sgN` nodes). Devices missing
    `ID_SCSI_SERIAL` are skipped — they are considered unmanaged.

    Note:
        Read-only — reflects the state of the local hardware as exposed
        by udev at the moment each method runs.

    Args:
        context: Optional pyudev `Context` to reuse. When omitted, a fresh
            context is created.

    Example:
        ```python
        from ctaops.sdk import LocalLibraryRepository

        repo = LocalLibraryRepository()
        for lib in repo.list_libraries():
            print(lib.serial_number)
        ```
    """

    @override
    def list_libraries(self) -> set[Library]:
        """
        See the base implementation.

        [Reference][ctaops.sdk.repositories.protocols.library.LibraryRepository.list_libraries].

        Implementation: enumerates medium-changer `sgN` devices and emits
        one
        [Library][ctaops.sdk.repositories.protocols.library.Library] per
        device that exposes `ID_SCSI_SERIAL`.
        """
        return {
            Library(device.properties["ID_SCSI_SERIAL"])
            for device in self._list_media_changer_sg_devices()
            if "ID_SCSI_SERIAL" in device.properties
        }

    @override
    def get_library(self, serial_number: str) -> Library:
        """
        See the base implementation.

        [Reference][ctaops.sdk.repositories.protocols.library.LibraryRepository.get_library].
        """
        try:
            return next(library for library in self.list_libraries() if library.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Library with serial number {serial_number} not found locally") from e


class LocalNamedLibraryRepository(_LocalAbstractLibraryRepository, NamedLibraryRepository):
    """udev-backed [NamedLibraryRepository][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository].

    Same discovery rules as
    [LocalLibraryRepository][ctaops.sdk.repositories.local.library.LocalLibraryRepository],
    but additionally requires the `CTAOPS_NAME` udev property on each
    medium changer. Changers without `CTAOPS_NAME` are skipped.
    """

    def __init__(self, context: Optional[Context] = None, library_name_key: str = "CTAOPS_NAME"):
        super().__init__(context)
        self._library_name_key = library_name_key

    @override
    def list_libraries(self) -> set[NamedLibrary]:
        """
        See the base implementation.

        [Reference][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository.list_libraries].
        """
        return {
            NamedLibrary(device.properties["ID_SCSI_SERIAL"], device.properties[self._library_name_key])
            for device in self._list_media_changer_sg_devices()
            if "ID_SCSI_SERIAL" in device.properties and self._library_name_key in device.properties
        }

    @override
    def get_library(self, serial_number: str) -> NamedLibrary:
        """
        See the base implementation.

        [Reference][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository.get_library].
        """
        try:
            return next(library for library in self.list_libraries() if library.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Library with serial number {serial_number} not found locally") from e

    @override
    def get_library_by_name(self, name: str) -> NamedLibrary:
        """
        See the base implementation.

        [Reference][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository.get_library_by_name].
        """
        try:
            return next(library for library in self.list_libraries() if library.name == name)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Library with name {name} not found locally") from e
