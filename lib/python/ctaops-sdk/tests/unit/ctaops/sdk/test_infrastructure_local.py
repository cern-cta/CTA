"""Tests for the udev-backed local repositories.

These tests fabricate fake pyudev devices via the ``device_factory`` /
``make_context`` fixtures (see conftest.py) so we can exercise the
repositories without real hardware.
"""

from collections.abc import Callable
from typing import Optional
from unittest.mock import MagicMock

import pytest
from ctaops.sdk.exceptions import ResourceNotFoundError
from ctaops.sdk.repositories.local.drive import (
    LocalDriveRepository,
    LocalLocatedDriveRepository,
    LocalLocatedNamedDriveRepository,
    LocalNamedDriveRepository,
)
from ctaops.sdk.repositories.local.library import (
    LocalLibraryRepository,
    LocalNamedLibraryRepository,
)


def _library_device(make_device: Callable[..., MagicMock], *, sys_name: str, serial: str, ctaops_name: str = "T0-LIB0"):
    return make_device(
        sys_name=sys_name,
        subsystem="scsi_generic",
        device_node=f"/dev/{sys_name}",
        properties={
            "ID_SCSI_SERIAL": serial,
            "CTAOPS_NAME": ctaops_name,
            "SCSI_TYPE": "medium changer",
        },
    )


def _drive_device(
    make_device: Callable[..., MagicMock],
    *,
    sys_name: str,
    serial: str,
    ctaops_name: str = "T0-LIB0-DRV0",
    address: str = "257",
    ordinal: str = "0",
    parent: Optional[MagicMock] = None,
):
    drive = make_device(
        sys_name=sys_name,
        subsystem="scsi_tape",
        device_node=f"/dev/{sys_name}",
        parent=parent,
        properties={
            "ID_SCSI_SERIAL": serial,
            "CTAOPS_NAME": ctaops_name,
            "CTAOPS_LIBRARY_ADDRESS": address,
            "CTAOPS_LIBRARY_ORDINAL": ordinal,
        },
    )
    if parent is not None:
        drive.find_parent.return_value = parent
    return drive


# --- LocalLibraryRepository / LocalNamedLibraryRepository ------------------- #


def test_list_libraries_returns_one_entry_per_changer(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    lib_a = _library_device(device_factory, sys_name="sg0", serial="LIBA")
    lib_b = _library_device(device_factory, sys_name="sg1", serial="LIBB", ctaops_name="T0-LIB1")
    repo = LocalLibraryRepository(make_context([lib_a, lib_b]))

    serials = {lib.serial_number for lib in repo.list_libraries()}
    assert serials == {"LIBA", "LIBB"}


def test_list_libraries_skips_devices_without_id_scsi_serial(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    """A medium-changer device without ID_SCSI_SERIAL is unmanaged and must not poison the result set."""
    unmanaged = device_factory(
        sys_name="sg0",
        subsystem="scsi_generic",
        device_node="/dev/sg0",
        properties={"SCSI_TYPE": "medium changer"},
    )
    managed = _library_device(device_factory, sys_name="sg1", serial="LIBA")
    repo = LocalLibraryRepository(make_context([unmanaged, managed]))

    assert {lib.serial_number for lib in repo.list_libraries()} == {"LIBA"}


def test_list_libraries_skips_non_sg_devices(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    bogus = _library_device(device_factory, sys_name="sgX", serial="BOGUS")
    real = _library_device(device_factory, sys_name="sg0", serial="LIBA")
    repo = LocalLibraryRepository(make_context([bogus, real]))

    assert {lib.serial_number for lib in repo.list_libraries()} == {"LIBA"}


def test_get_library_by_serial(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    lib = _library_device(device_factory, sys_name="sg0", serial="LIBA")
    repo = LocalLibraryRepository(make_context([lib]))

    found = repo.get_library("LIBA")
    assert found.serial_number == "LIBA"


def test_get_library_missing_raises(make_context: Callable[[list[MagicMock]], MagicMock]):
    repo = LocalLibraryRepository(make_context([]))

    with pytest.raises(ResourceNotFoundError):
        repo.get_library("DOES-NOT-EXIST")


def test_named_library_repo_skips_devices_without_ctaops_name(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    plain_changer = device_factory(
        sys_name="sg0",
        subsystem="scsi_generic",
        device_node="/dev/sg0",
        properties={"ID_SCSI_SERIAL": "LIBA", "SCSI_TYPE": "medium changer"},
    )
    named = _library_device(device_factory, sys_name="sg1", serial="LIBB")
    repo = LocalNamedLibraryRepository(make_context([plain_changer, named]))

    libs = repo.list_libraries()
    assert {lib.serial_number for lib in libs} == {"LIBB"}
    assert next(iter(libs)).name == "T0-LIB0"


def test_named_library_get_by_name(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    lib = _library_device(device_factory, sys_name="sg0", serial="LIBA")
    repo = LocalNamedLibraryRepository(make_context([lib]))

    assert repo.get_library_by_name("T0-LIB0").serial_number == "LIBA"


def test_named_library_get_by_name_missing_raises(make_context: Callable[[list[MagicMock]], MagicMock]):
    repo = LocalNamedLibraryRepository(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        repo.get_library_by_name("nope")


# --- LocalDriveRepository / LocalNamedDriveRepository ----------------------- #


def test_local_drive_repository_lists_drives(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    drive = _drive_device(device_factory, sys_name="nst0", serial="DRA")
    repo = LocalDriveRepository(make_context([drive]))

    assert {d.serial_number for d in repo.list_drives()} == {"DRA"}


def test_local_drive_repository_skips_drives_without_serial(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    no_serial = device_factory(
        sys_name="nst0",
        subsystem="scsi_tape",
        device_node="/dev/nst0",
        properties={},
    )
    repo = LocalDriveRepository(make_context([no_serial]))
    assert repo.list_drives() == set()


def test_local_drive_repository_get_drive_missing_raises(make_context: Callable[[list[MagicMock]], MagicMock]):
    repo = LocalDriveRepository(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        repo.get_drive("DRX")


def test_local_named_drive_repository_lists_named_drives(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    drive = _drive_device(device_factory, sys_name="nst0", serial="DRA", ctaops_name="T0-LIB0-DRV0")
    repo = LocalNamedDriveRepository(make_context([drive]))

    drives = repo.list_drives()
    assert {(d.serial_number, d.name) for d in drives} == {("DRA", "T0-LIB0-DRV0")}


def test_local_named_drive_repository_get_drive_by_name(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    drive = _drive_device(device_factory, sys_name="nst0", serial="DRA", ctaops_name="T0-LIB0-DRV0")
    repo = LocalNamedDriveRepository(make_context([drive]))

    found = repo.get_drive_by_name("T0-LIB0-DRV0")
    assert found.serial_number == "DRA"


def test_local_named_drive_repository_get_drive_by_name_missing_raises(
    make_context: Callable[[list[MagicMock]], MagicMock],
):
    repo = LocalNamedDriveRepository(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        repo.get_drive_by_name("nope")


# --- LocalLocatedDriveRepository / LocalLocatedNamedDriveRepository --------- #


def _wire_drive_under_library(
    device_factory: Callable[..., MagicMock],
    *,
    drive_sys_name: str,
    drive_serial: str,
    drive_name: str = "T0-LIB0-DRV0",
    lib_sys_name: str,
    lib_serial: str,
    lib_name: str = "T0-LIB0",
    address: str = "257",
    ordinal: str = "0",
):
    """Build a (lib, drive) pair sharing the same scsi_target parent."""
    target = device_factory(sys_name=lib_sys_name + "-target", subsystem="scsi")
    target.find_parent.return_value = target
    lib = _library_device(device_factory, sys_name=lib_sys_name, serial=lib_serial, ctaops_name=lib_name)
    lib.parent = target
    drive = _drive_device(
        device_factory,
        sys_name=drive_sys_name,
        serial=drive_serial,
        ctaops_name=drive_name,
        address=address,
        ordinal=ordinal,
        parent=target,
    )
    return target, lib, drive


def test_located_drive_repo_filters_by_library_serial(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    _, lib_a, drive_a = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst0",
        drive_serial="DRA",
        lib_sys_name="sg0",
        lib_serial="LIBA",
    )
    _, lib_b, drive_b = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst1",
        drive_serial="DRB",
        drive_name="T0-LIB1-DRV0",
        lib_sys_name="sg1",
        lib_serial="LIBB",
        lib_name="T0-LIB1",
    )
    repo = LocalLocatedDriveRepository(make_context([lib_a, lib_b, drive_a, drive_b]))

    drives = repo.list_drives(lib_serial_number="LIBA")
    assert {d.serial_number for d in drives} == {"DRA"}


def test_located_drive_repo_without_filter_returns_all(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    _, lib, drive = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst0",
        drive_serial="DRA",
        lib_sys_name="sg0",
        lib_serial="LIBA",
    )
    repo = LocalLocatedDriveRepository(make_context([lib, drive]))

    assert {d.serial_number for d in repo.list_drives()} == {"DRA"}


def test_located_drive_repo_get_by_ordinal(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    _, lib, drive = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst0",
        drive_serial="DRA",
        lib_sys_name="sg0",
        lib_serial="LIBA",
        address="257",
        ordinal="0",
    )
    repo = LocalLocatedDriveRepository(make_context([lib, drive]))

    found = repo.get_drive_by_ordinal(0, "LIBA")
    assert found.serial_number == "DRA"


def test_located_drive_repo_get_by_ordinal_missing_raises(make_context: Callable[[list[MagicMock]], MagicMock]):
    repo = LocalLocatedDriveRepository(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        repo.get_drive_by_ordinal(0, "LIBA")


def test_located_named_drive_repo_yields_full_metadata(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    _, lib, drive = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst0",
        drive_serial="DRA",
        drive_name="T0-LIB0-DRV0",
        lib_sys_name="sg0",
        lib_serial="LIBA",
    )
    repo = LocalLocatedNamedDriveRepository(make_context([lib, drive]))

    drives = list(repo.list_drives())
    assert len(drives) == 1
    assert drives[0].serial_number == "DRA"
    assert drives[0].name == "T0-LIB0-DRV0"
    assert drives[0].location.ordinal == 0
    assert drives[0].location.library_serial_number == "LIBA"


def test_located_named_drive_repo_get_by_name(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    _, lib, drive = _wire_drive_under_library(
        device_factory,
        drive_sys_name="nst0",
        drive_serial="DRA",
        drive_name="T0-LIB0-DRV0",
        lib_sys_name="sg0",
        lib_serial="LIBA",
    )
    repo = LocalLocatedNamedDriveRepository(make_context([lib, drive]))

    found = repo.get_drive_by_name("T0-LIB0-DRV0")
    assert found.serial_number == "DRA"


def test_located_named_drive_repo_skips_drives_without_changer_sibling(
    device_factory: Callable[..., MagicMock], make_context: Callable[[list[MagicMock]], MagicMock]
):
    """A drive that has no sibling medium changer must be skipped."""
    target = device_factory(sys_name="target", subsystem="scsi")
    target.find_parent.return_value = target
    drive = _drive_device(
        device_factory,
        sys_name="nst0",
        serial="DRA",
        parent=target,
    )
    repo = LocalLocatedNamedDriveRepository(make_context([drive]))

    assert repo.list_drives() == set()
