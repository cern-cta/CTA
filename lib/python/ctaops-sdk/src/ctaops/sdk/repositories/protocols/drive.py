# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols and value objects for read-only tape-drive enumeration.

Four progressively richer protocols are exposed:

* [DriveRepository][ctaops.sdk.repositories.protocols.drive.DriveRepository]
  — bare-bones inventory; drives are identified by SCSI serial only.
* [NamedDriveRepository][ctaops.sdk.repositories.protocols.drive.NamedDriveRepository]
  — drives also carry their logical ctaops name.
* [LocatedDriveRepository][ctaops.sdk.repositories.protocols.drive.LocatedDriveRepository]
  — drives also know which library/ordinal they sit in.
* [LocatedNamedDriveRepository][ctaops.sdk.repositories.protocols.drive.LocatedNamedDriveRepository]
  — both name and location.

Concrete udev-backed implementations:
[LocalDriveRepository][ctaops.sdk.repositories.local.drive.LocalDriveRepository],
[LocalNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalNamedDriveRepository],
[LocalLocatedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedDriveRepository],
[LocalLocatedNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedNamedDriveRepository].
"""

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass(frozen=True)
class DriveLibraryLocation:
    """Position of a tape drive inside its parent logical library.

    A drive's location is reported by the library's media changer and is
    what mount/unmount commands address. `address` is the raw SCSI element
    address inside the changer; `ordinal` is the 0-indexed position of the
    drive among its peers in the same library.

    Attributes:
        address: Element address of the drive within the changer (usually a
            SCSI element address).
        ordinal: 0-based index of the drive within its library, derived from
            `address`. This is the form accepted by tools like `cta-smc -D`.
        library_serial_number: Serial number of the parent logical library.

    Raises:
        ValueError: If `library_serial_number` is empty.
    """

    address: int
    ordinal: int
    library_serial_number: str

    def __post_init__(self) -> None:
        if not self.library_serial_number:
            raise ValueError("DriveLibraryLocation.library_serial_number must be a non-empty string")


@dataclass(frozen=True)
class Drive:
    """Identity of a tape drive.

    The base class carries only the SCSI serial number; richer variants
    add the logical ctaops name
    ([NamedDrive][ctaops.sdk.repositories.protocols.drive.NamedDrive]),
    the library binding
    ([LocatedDrive][ctaops.sdk.repositories.protocols.drive.LocatedDrive]),
    or both ([LocatedNamedDrive][ctaops.sdk.repositories.protocols.drive.LocatedNamedDrive]).

    Equality and hashing are based solely on `serial_number`, so the same
    drive seen with or without extra metadata compares equal.

    Attributes:
        serial_number: Serial number of the drive (usually from SCSI).

    Raises:
        ValueError: If `serial_number` is empty.
    """

    serial_number: str

    def __hash__(self) -> int:
        return hash(self.serial_number)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Drive):
            return False
        return self.serial_number == other.serial_number

    def __post_init__(self) -> None:
        if not self.serial_number:
            raise ValueError("Drive.serial_number must be a non-empty string")


@dataclass(frozen=True, eq=False)  # eq = False so the Drive-based equality survives
class NamedDrive(Drive):
    """Drive that also exposes its logical ctaops name.

    Attributes:
        name: Logical drive name, configured externally by the operator
            (typically the `CTAOPS_NAME` udev property).

    Raises:
        ValueError: If `name` is empty.
    """

    name: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.name:
            raise ValueError("NamedDrive.name must be a non-empty string")


@dataclass(frozen=True, eq=False)
class LocatedDrive(Drive):
    """Drive that is bound to a parent library.

    Attributes:
        location: Position of the drive in its parent library.
    """

    location: DriveLibraryLocation

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.location is None:
            raise ValueError("LocatedDrive.location must be a DriveLibraryLocation instance")


@dataclass(frozen=True, eq=False)
class LocatedNamedDrive(LocatedDrive, NamedDrive):
    """Drive carrying both a logical ctaops name and a library binding."""

    pass


class DriveRepository(Protocol):
    """Minimal drive repository: serial-number lookups only."""

    def list_drives(self) -> set[Drive]:
        """Return the set of locally-visible drives."""
        ...

    def get_drive(self, serial_number: str) -> Drive:
        """Return the drive with the given SCSI serial number.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...


class NamedDriveRepository(Protocol):
    """Drive repository whose entries also carry their logical ctaops name."""

    def list_drives(self) -> set[NamedDrive]:
        """Return the set of locally-visible named drives."""
        ...

    def get_drive(self, serial_number: str) -> NamedDrive:
        """Return the named drive with the given SCSI serial number.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def get_drive_by_name(self, name: str) -> NamedDrive:
        """Return the named drive with the given logical name.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...


class LocatedDriveRepository(Protocol):
    """Drive repository whose entries are bound to a parent library."""

    def list_drives(self, lib_serial_number: Optional[str] = None) -> set[LocatedDrive]:
        """Return the set of located drives, optionally scoped to one library.

        Args:
            lib_serial_number: When given, restrict the result to drives bound
                to the library with that serial number.
        """
        ...

    def get_drive(self, serial_number: str, lib_serial_number: Optional[str] = None) -> LocatedDrive:
        """Return the located drive with the given SCSI serial number.

        Args:
            serial_number: SCSI serial of the drive to look up.
            lib_serial_number: Optional library serial to scope the lookup to.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def get_drive_by_ordinal(self, ordinal: int, lib_serial_number: str) -> LocatedDrive:
        """Return the located drive at the given ordinal within `lib_serial_number`.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...


class LocatedNamedDriveRepository(Protocol):
    """Drive repository combining logical-name and library-location addressing."""

    def list_drives(self, lib_serial_number: Optional[str] = None) -> set[LocatedNamedDrive]:
        """Return the set of named+located drives, optionally scoped to one library."""
        ...

    def get_drive(self, serial_number: str, lib_serial_number: Optional[str] = None) -> LocatedNamedDrive:
        """Return the named+located drive with the given SCSI serial number.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def get_drive_by_name(self, name: str, lib_serial_number: Optional[str] = None) -> LocatedNamedDrive:
        """Return the named+located drive with the given logical name.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...

    def get_drive_by_ordinal(self, ordinal: int, lib_serial_number: str) -> LocatedNamedDrive:
        """Return the named+located drive at `ordinal` within `lib_serial_number`.

        Raises:
            ResourceNotFoundError: If no matching drive is visible.
        """
        ...
