# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols and value objects for read-only tape-library enumeration.

Two protocols are exposed:

* [LibraryRepository][ctaops.sdk.repositories.protocols.library.LibraryRepository]
  — bare-bones inventory; libraries are identified by SCSI serial only.
* [NamedLibraryRepository][ctaops.sdk.repositories.protocols.library.NamedLibraryRepository]
  — libraries also carry their logical ctaops name.

Concrete udev-backed implementations:
[LocalLibraryRepository][ctaops.sdk.repositories.local.library.LocalLibraryRepository]
and
[LocalNamedLibraryRepository][ctaops.sdk.repositories.local.library.LocalNamedLibraryRepository].
"""

from dataclasses import dataclass
from typing import Protocol

from typing_extensions import override


@dataclass(frozen=True)
class Library:
    """Identity of a tape library (media changer).

    Attributes:
        serial_number: Serial number of the changer (usually from SCSI).

    Raises:
        ValueError: If `serial_number` is empty.
    """

    serial_number: str

    def __post_init__(self) -> None:
        if not self.serial_number:
            raise ValueError("Library.serial_number must be a non-empty string")

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Library):
            return False
        return self.serial_number == other.serial_number

    @override
    def __hash__(self) -> int:
        return hash(self.serial_number)


@dataclass(frozen=True, eq=False)
class NamedLibrary(Library):
    """Logical library with a non-empty name.

    Attributes:
        name: Logical library name, configured externally by the operator (e.g. via udev).

    Raises:
        ValueError: If `name` is empty.
    """

    name: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.name:
            raise ValueError("NamedLibrary.name must be a non-empty string")


class LibraryRepository(Protocol):
    """Minimal library repository: serial-number lookups only."""

    def list_libraries(self) -> set[Library]:
        """Return the set of locally-visible libraries."""
        ...

    def get_library(self, serial_number: str) -> Library:
        """Return the library with the given SCSI serial number.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...


class NamedLibraryRepository(Protocol):
    """Library repository whose entries also carry their logical ctaops name."""

    def list_libraries(self) -> set[NamedLibrary]:
        """Return the set of locally-visible named libraries."""
        ...

    def get_library(self, serial_number: str) -> NamedLibrary:
        """Return the named library with the given SCSI serial number.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...

    def get_library_by_name(self, name: str) -> NamedLibrary:
        """Return the named library with the given logical name.

        Raises:
            ResourceNotFoundError: If no matching library is visible.
        """
        ...
