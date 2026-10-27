# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""udev-backed implementations of the drive repository protocols.

See [ctaops.sdk.repositories.protocols.drive][ctaops.sdk.repositories.protocols.drive]
for the public contracts. The classes in this module enumerate
locally-attached SCSI tape drives via pyudev and (for the located
variants) walk the SCSI topology to bind each drive to its parent medium
changer.
"""

import re
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Container, Optional

from ctaops.sdk.exceptions import ResourceNotFoundError
from ctaops.sdk.repositories.protocols.drive import (
    Drive,
    DriveLibraryLocation,
    DriveRepository,
    LocatedDrive,
    LocatedDriveRepository,
    LocatedNamedDrive,
    LocatedNamedDriveRepository,
    NamedDrive,
    NamedDriveRepository,
)
from pyudev import Context, Device

LOGGER = getLogger("ctaops.sdk")


class _LocalAbstractDriveRepository(ABC):
    def __init__(self, context: Optional[Context] = None):
        self._context = Context() if context is None else context

    def _list_drive_nst_devices(self) -> set[Device]:
        """Return the set of `nstN` `scsi_tape` devices visible locally."""
        LOGGER.debug("Listing local nstN devices")
        return {
            device
            for device in self._context.list_devices(subsystem="scsi_tape")
            if re.match(r"^nst[0-9]+$", device.sys_name)
        }

    def _find_media_changer_serial_number(
        self,
        nst_device: Device,
    ) -> Optional[str]:
        """Locate the SCSI serial of the changer that is the drive's sibling.

        Walks up to the drive's `scsi_target` parent and looks for a
        sibling `sgN` medium-changer that matches the requested
        filter, if any.
        """
        LOGGER.debug("Finding media changer serial number for device %s", nst_device.sys_name)
        return next(
            (
                device.properties.get("ID_SCSI_SERIAL")
                for device in self._context.list_devices(
                    subsystem="scsi_generic",
                    SCSI_TYPE="medium changer",
                    parent=nst_device.find_parent(subsystem="scsi", device_type="scsi_target"),
                )
                if re.match(r"^sg[0-9]+$", device.sys_name) and "ID_SCSI_SERIAL" in device.properties
            ),
            None,
        )

    @abstractmethod
    def list_drives(self) -> Container[Drive]:
        pass

    @abstractmethod
    def get_drive(self, serial_number: str) -> Drive:
        pass


class LocalDriveRepository(_LocalAbstractDriveRepository, DriveRepository):
    """udev-backed [DriveRepository][ctaops.sdk.repositories.protocols.drive.DriveRepository].

    Drives are discovered via the `scsi_tape` subsystem (`nstN` nodes).
    Devices missing `ID_SCSI_SERIAL` are skipped — they are considered
    unmanaged.

    Args:
        context: Optional pyudev `Context` to reuse. When omitted, a fresh
            context is created.
    """

    def list_drives(self) -> set[Drive]:
        """See [DriveRepository.list_drives][ctaops.sdk.repositories.protocols.drive.DriveRepository.list_drives]."""
        return {
            Drive(
                serial_number=nst_device.properties["ID_SCSI_SERIAL"],
            )
            for nst_device in self._list_drive_nst_devices()
            if "ID_SCSI_SERIAL" in nst_device.properties
        }

    def get_drive(self, serial_number: str) -> Drive:
        """See [DriveRepository.get_drive][ctaops.sdk.repositories.protocols.drive.DriveRepository.get_drive]."""
        try:
            return next(drive for drive in self.list_drives() if drive.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {serial_number} not found locally") from e


class LocalNamedDriveRepository(_LocalAbstractDriveRepository, NamedDriveRepository):
    """udev-backed [NamedDriveRepository][ctaops.sdk.repositories.protocols.drive.NamedDriveRepository].

    Same discovery as
    [LocalDriveRepository][ctaops.sdk.repositories.local.drive.LocalDriveRepository],
    but additionally requires the `CTAOPS_NAME` udev property on each
    `nstN` device. Drives without `CTAOPS_NAME` are skipped.
    """

    def __init__(self, context: Optional[Context] = None, drive_name_key: str = "CTAOPS_NAME"):
        super().__init__(context)
        self._drive_name_key = drive_name_key

    def list_drives(self) -> set[NamedDrive]:
        """See [NamedDriveRepository.list_drives][ctaops.sdk.repositories.protocols.drive.NamedDriveRepository.list_drives]."""
        return {
            NamedDrive(
                serial_number=nst_device.properties["ID_SCSI_SERIAL"],
                name=nst_device.properties[self._drive_name_key],
            )
            for nst_device in self._list_drive_nst_devices()
            if "ID_SCSI_SERIAL" in nst_device.properties and self._drive_name_key in nst_device.properties
        }

    def get_drive(self, serial_number: str) -> NamedDrive:
        """See [NamedDriveRepository.get_drive][ctaops.sdk.repositories.protocols.drive.NamedDriveRepository.get_drive]."""
        try:
            return next(drive for drive in self.list_drives() if drive.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {serial_number} not found locally") from e

    def get_drive_by_name(self, name: str) -> NamedDrive:
        """See [NamedDriveRepository.get_drive_by_name][ctaops.sdk.repositories.protocols.drive.NamedDriveRepository.get_drive_by_name]."""
        try:
            return next(drive for drive in self.list_drives() if drive.name == name)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {name} not found locally") from e


class LocalLocatedDriveRepository(_LocalAbstractDriveRepository, LocatedDriveRepository):
    """udev-backed [LocatedDriveRepository][ctaops.sdk.repositories.protocols.drive.LocatedDriveRepository].

    Each drive is bound to its parent library by walking up to the drive's
    `scsi_target` parent and finding the sibling `sgN` medium changer
    that carries `ID_SCSI_SERIAL`. Drives without a sibling changer, or
    without the `CTAOPS_LIBRARY_ADDRESS` / `CTAOPS_LIBRARY_ORDINAL`
    properties, are skipped.
    """

    def __init__(
        self,
        context: Optional[Context] = None,
        drive_library_address_key: str = "CTAOPS_LIBRARY_ADDRESS",
        drive_library_ordinal_key: str = "CTAOPS_LIBRARY_ORDINAL",
    ):
        super().__init__(context)
        self._drive_library_address_key = drive_library_address_key
        self._drive_library_ordinal_key = drive_library_ordinal_key

    def list_drives(self, lib_serial_number: Optional[str] = None) -> set[LocatedDrive]:
        """See [LocatedDriveRepository.list_drives][ctaops.sdk.repositories.protocols.drive.LocatedDriveRepository.list_drives]."""
        return {
            LocatedDrive(
                serial_number=nst_device.properties["ID_SCSI_SERIAL"],
                location=DriveLibraryLocation(
                    int(nst_device.properties[self._drive_library_address_key]),
                    int(nst_device.properties[self._drive_library_ordinal_key]),
                    mc_sn,
                ),
            )
            for nst_device in self._list_drive_nst_devices()
            if "ID_SCSI_SERIAL" in nst_device.properties
            and self._drive_library_address_key in nst_device.properties
            and self._drive_library_ordinal_key in nst_device.properties
            and (mc_sn := self._find_media_changer_serial_number(nst_device)) is not None
            and (lib_serial_number is None or lib_serial_number == mc_sn)
        }

    def get_drive(self, serial_number: str, lib_serial_number: Optional[str] = None) -> LocatedDrive:
        """See [LocatedDriveRepository.get_drive][ctaops.sdk.repositories.protocols.drive.LocatedDriveRepository.get_drive]."""
        try:
            return next(drive for drive in self.list_drives(lib_serial_number) if drive.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {serial_number} not found locally") from e

    def get_drive_by_ordinal(self, ordinal: int, lib_serial_number: str) -> LocatedDrive:
        """See [LocatedDriveRepository.get_drive_by_ordinal][ctaops.sdk.repositories.protocols.drive.LocatedDriveRepository.get_drive_by_ordinal]."""
        try:
            return next(drive for drive in self.list_drives(lib_serial_number) if drive.location.ordinal == ordinal)
        except StopIteration as e:
            raise ResourceNotFoundError(
                f"Drive with ordinal {ordinal} in library {lib_serial_number} not found locally"
            ) from e


class LocalLocatedNamedDriveRepository(_LocalAbstractDriveRepository, LocatedNamedDriveRepository):
    """udev-backed [LocatedNamedDriveRepository][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository].

    Combines the binding rules of
    [LocalLocatedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedDriveRepository]
    with the name requirement of
    [LocalNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalNamedDriveRepository]:
    a drive must carry `ID_SCSI_SERIAL`, `CTAOPS_NAME`,
    `CTAOPS_LIBRARY_ADDRESS`, `CTAOPS_LIBRARY_ORDINAL`, and have a
    sibling medium-changer with `ID_SCSI_SERIAL`.
    """

    def __init__(
        self,
        context: Optional[Context] = None,
        drive_name_key: str = "CTAOPS_NAME",
        drive_library_address_key: str = "CTAOPS_LIBRARY_ADDRESS",
        drive_library_ordinal_key: str = "CTAOPS_LIBRARY_ORDINAL",
    ):
        super().__init__(context)
        self._drive_name_key = drive_name_key
        self._drive_library_address_key = drive_library_address_key
        self._drive_library_ordinal_key = drive_library_ordinal_key

    def list_drives(self, lib_serial_number: Optional[str] = None) -> set[LocatedNamedDrive]:
        """See [LocatedNamedDriveRepository.list_drives][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository.list_drives]."""
        return {
            LocatedNamedDrive(
                serial_number=nst_device.properties["ID_SCSI_SERIAL"],
                name=nst_device.properties[self._drive_name_key],
                location=DriveLibraryLocation(
                    int(nst_device.properties[self._drive_library_address_key]),
                    int(nst_device.properties[self._drive_library_ordinal_key]),
                    mc_sn,
                ),
            )
            for nst_device in self._list_drive_nst_devices()
            if "ID_SCSI_SERIAL" in nst_device.properties
            and self._drive_library_address_key in nst_device.properties
            and self._drive_library_ordinal_key in nst_device.properties
            and self._drive_name_key in nst_device.properties
            and (mc_sn := self._find_media_changer_serial_number(nst_device)) is not None
            and (lib_serial_number is None or lib_serial_number == mc_sn)
        }

    def get_drive(self, serial_number: str, lib_serial_number: Optional[str] = None) -> LocatedNamedDrive:
        """See [LocatedNamedDriveRepository.get_drive][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository.get_drive]."""
        try:
            return next(drive for drive in self.list_drives(lib_serial_number) if drive.serial_number == serial_number)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {serial_number} not found locally") from e

    def get_drive_by_name(self, name: str, lib_serial_number: Optional[str] = None) -> LocatedNamedDrive:
        """See [LocatedNamedDriveRepository.get_drive_by_name][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository.get_drive_by_name]."""
        try:
            return next(drive for drive in self.list_drives(lib_serial_number) if drive.name == name)
        except StopIteration as e:
            raise ResourceNotFoundError(f"Drive {name} not found locally") from e

    def get_drive_by_ordinal(self, ordinal: int, lib_serial_number: str) -> LocatedNamedDrive:
        """See [LocatedNamedDriveRepository.get_drive_by_ordinal][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository.get_drive_by_ordinal]."""
        try:
            return next(drive for drive in self.list_drives(lib_serial_number) if drive.location.ordinal == ordinal)
        except StopIteration as e:
            raise ResourceNotFoundError(
                f"Drive with ordinal {ordinal} in library {lib_serial_number} not found locally"
            ) from e
