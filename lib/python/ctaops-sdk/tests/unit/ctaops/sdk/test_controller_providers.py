"""Tests for the SCSI-backed controller providers."""

from unittest.mock import MagicMock

import pytest
from ctaops.sdk.controller_providers.scsi.drive import ScsiDriveControllerProvider
from ctaops.sdk.controller_providers.scsi.library import (
    ScsiLibraryControllerProvider,
    ScsiManagedLibraryControllerProvider,
)
from ctaops.sdk.exceptions import ResourceNotFoundError


@pytest.fixture(autouse=True)
def patch_is_char_device(monkeypatch):
    """Treat any /dev/* path the SDK opens as a character device."""
    monkeypatch.setattr("pathlib.Path.is_char_device", lambda _: True)


def _drive_nst(device_factory, *, sys_name="nst0", serial="DRSN", ctaops_name="T0-LIB0-DRV0"):
    parent = MagicMock(name="ScsiTarget")
    nst = device_factory(
        sys_name=sys_name,
        subsystem="scsi_tape",
        device_node=f"/dev/{sys_name}",
        parent=parent,
        properties={"ID_SCSI_SERIAL": serial, "CTAOPS_NAME": ctaops_name},
    )
    sg = device_factory(
        sys_name="sg5",
        subsystem="scsi_generic",
        device_node="/dev/sg5",
        parent=parent,
    )
    st = device_factory(
        sys_name=f"{sys_name[1:]}",
        subsystem="scsi_tape",
        device_node=f"/dev/{sys_name[1:]}",
        parent=parent,
    )
    return nst, sg, st


# --- ScsiDriveControllerProvider ------------------------------------------- #


def test_drive_provider_get_controller_by_serial(device_factory, make_context):
    nst, sg, st = _drive_nst(device_factory)
    provider = ScsiDriveControllerProvider(make_context([nst, sg, st]))
    ctrl = provider.get_drive_controller("DRSN")
    assert ctrl.serial_number == "DRSN"


def test_drive_provider_get_controller_missing_raises(make_context):
    provider = ScsiDriveControllerProvider(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        provider.get_drive_controller("nope")


def test_drive_provider_get_controller_by_name(device_factory, make_context):
    nst, sg, st = _drive_nst(device_factory)
    provider = ScsiDriveControllerProvider(make_context([nst, sg, st]))
    ctrl = provider.get_drive_controller_by_name("T0-LIB0-DRV0")
    assert ctrl.serial_number == "DRSN"


def test_drive_provider_get_controller_by_name_missing_raises(make_context):
    provider = ScsiDriveControllerProvider(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        provider.get_drive_controller_by_name("nope")


def test_drive_provider_find_controller_returns_none(make_context):
    """`find_*` variants must not raise — they return None when no match."""
    provider = ScsiDriveControllerProvider(make_context([]))
    assert provider.find_drive_controller("nope") is None
    assert provider.find_drive_controller_by_name("nope") is None


# --- ScsiLibraryControllerProvider / ScsiManagedLibraryControllerProvider --- #


def _library_sg(device_factory, *, sys_name="sg0", serial="LIBSN", ctaops_name="T0-LIB0"):
    return device_factory(
        sys_name=sys_name,
        subsystem="scsi_generic",
        device_node=f"/dev/{sys_name}",
        properties={
            "ID_SCSI_SERIAL": serial,
            "CTAOPS_NAME": ctaops_name,
            "SCSI_TYPE": "medium changer",
        },
    )


def test_library_provider_get_controller_missing_raises(make_context):
    provider = ScsiLibraryControllerProvider(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        provider.get_library_controller("nope")


def test_library_provider_get_controller_by_serial(device_factory, make_context):
    sg = _library_sg(device_factory)
    provider = ScsiLibraryControllerProvider(make_context([sg]))
    ctrl = provider.get_library_controller("LIBSN")
    assert ctrl.serial_number == "LIBSN"


def test_library_provider_get_controller_by_name(device_factory, make_context):
    sg = _library_sg(device_factory)
    provider = ScsiLibraryControllerProvider(make_context([sg]))
    ctrl = provider.get_library_controller_by_name("T0-LIB0")
    assert ctrl.serial_number == "LIBSN"


def test_library_provider_find_returns_none_when_missing(make_context):
    provider = ScsiLibraryControllerProvider(make_context([]))
    assert provider.find_library_controller("nope") is None
    assert provider.find_library_controller_by_name("nope") is None


def test_managed_provider_produces_managed_controllers(device_factory, make_context):
    """The managed provider must return a controller that exposes the ManagedLibraryController extras."""
    from ctaops.sdk.controllers.scsi.library import ScsiManagedLibraryController

    sg = _library_sg(device_factory)
    provider = ScsiManagedLibraryControllerProvider(make_context([sg]))
    ctrl = provider.get_library_controller("LIBSN")
    assert isinstance(ctrl, ScsiManagedLibraryController)


def test_managed_provider_by_name_missing_raises(make_context):
    provider = ScsiManagedLibraryControllerProvider(make_context([]))
    with pytest.raises(ResourceNotFoundError):
        provider.get_library_controller_by_name("nope")
