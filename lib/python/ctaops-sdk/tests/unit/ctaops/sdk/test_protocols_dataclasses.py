"""Tests for the immutable dataclasses exposed by the repository protocols."""

from typing import Optional

import pytest
from ctaops.sdk.repositories.protocols.drive import (
    Drive,
    DriveLibraryLocation,
    LocatedDrive,
    LocatedNamedDrive,
    NamedDrive,
)
from ctaops.sdk.repositories.protocols.library import Library, NamedLibrary


def test_drive_library_location_is_frozen_and_hashable() -> None:
    loc = DriveLibraryLocation(address=257, ordinal=0, library_serial_number="LIBSN")
    assert loc.address == 257
    assert loc.ordinal == 0
    assert loc.library_serial_number == "LIBSN"
    with pytest.raises(AttributeError):
        loc.address = 99  # type: ignore[misc]
    assert hash(loc) == hash(DriveLibraryLocation(address=257, ordinal=0, library_serial_number="LIBSN"))


def test_drive_library_location_rejects_empty_serial() -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        DriveLibraryLocation(address=1, ordinal=0, library_serial_number="")


def test_drive_equality_keyed_on_serial_number() -> None:
    a = Drive(serial_number="SN1")
    b = Drive(serial_number="SN1")
    c = Drive(serial_number="SN2")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)


@pytest.mark.parametrize("serial", [None, ""])
def test_drive_rejects_empty_serial(serial: Optional[str]) -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        Drive(serial_number=serial)  # type: ignore[arg-type]


def test_named_drive_holds_logical_name() -> None:
    drive = NamedDrive(serial_number="DRSN", name="T0-LIB0-DRV0")
    assert drive.serial_number == "DRSN"
    assert drive.name == "T0-LIB0-DRV0"


@pytest.mark.parametrize("name", [None, ""])
def test_named_drive_rejects_empty_name(name: Optional[str]) -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        NamedDrive(serial_number="SN", name=name)  # type: ignore[arg-type]


def test_located_drive_holds_location() -> None:
    loc = DriveLibraryLocation(address=10, ordinal=1, library_serial_number="LIBSN")
    drive = LocatedDrive(serial_number="DRSN", location=loc)
    assert drive.location.ordinal == 1
    assert drive.location.library_serial_number == "LIBSN"


def test_located_named_drive_combines_name_and_location() -> None:
    loc = DriveLibraryLocation(address=10, ordinal=1, library_serial_number="LIBSN")
    drive = LocatedNamedDrive(serial_number="DRSN", name="T0-LIB0-DRV0", location=loc)
    assert drive.name == "T0-LIB0-DRV0"
    assert drive.location.ordinal == 1


def test_library_equality_keyed_on_serial_number() -> None:
    a = Library(serial_number="SN1")
    b = Library(serial_number="SN1")
    c = Library(serial_number="SN2")
    assert a == b
    assert a != c


@pytest.mark.parametrize("serial", [None, ""])
def test_library_rejects_empty_serial(serial: Optional[str]) -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        Library(serial_number=serial)  # type: ignore[arg-type]


def test_named_library_holds_name() -> None:
    lib = NamedLibrary(serial_number="SN", name="T0-LIB0")
    assert lib.serial_number == "SN"
    assert lib.name == "T0-LIB0"


@pytest.mark.parametrize(("serial", "name"), [(None, "T0"), ("", "T0"), ("SN", None), ("SN", "")])
def test_named_library_rejects_empty_fields(serial: Optional[str], name: Optional[str]) -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        NamedLibrary(serial_number=serial, name=name)  # type: ignore[arg-type]
