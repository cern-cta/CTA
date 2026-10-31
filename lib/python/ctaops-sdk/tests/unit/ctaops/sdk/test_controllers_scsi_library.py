"""Tests for ScsiManagedLibraryController.

The library controller shells out to /usr/bin/cta-smc and /usr/bin/cta-tape-label
for every operation. We monkeypatch subprocess.run to capture invocations and
return canned JSON. Drive/library metadata lookups go through
LocalLocatedNamedDriveRepository / LocalNamedLibraryRepository — both are
patched on the controller instance with simple stubs.
"""

import json
import subprocess
from collections.abc import Callable
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from ctaops.sdk.controllers.scsi.library import ScsiManagedLibraryController
from ctaops.sdk.exceptions import (
    DriveStateError,
    LibraryOperationError,
    ResourceNotFoundError,
    TapeLabelError,
)
from ctaops.sdk.repositories.protocols.drive import DriveLibraryLocation, LocatedNamedDrive


def _make_sg_device(
    device_factory: Callable[..., MagicMock], *, serial: Optional[str] = "LIBSN", ctaops_name: str = "T0-LIB0"
):
    return device_factory(
        sys_name="sg0",
        subsystem="scsi_generic",
        device_node="/dev/sg0",
        properties={
            "ID_SCSI_SERIAL": serial,
            "CTAOPS_NAME": ctaops_name,
            "SCSI_TYPE": "medium changer",
        },
    )


def _stub_drive_repo() -> MagicMock:
    """Return a repository stub containing a single canned drive."""
    repo = MagicMock(name="LocalLocatedNamedDriveRepository")
    canned = LocatedNamedDrive(
        serial_number="DRSN",
        name="T0-LIB0-DRV0",
        location=DriveLibraryLocation(address=257, ordinal=0, library_serial_number="LIBSN"),
    )
    repo.get_drive_by_ordinal.return_value = canned
    repo.get_drive_by_name.return_value = canned
    return repo


def _build_managed_controller(device_factory: Callable[..., MagicMock], *, drive_repo: "MagicMock | None" = None):
    """Build a ScsiManagedLibraryController with internal repos stubbed."""
    sg = _make_sg_device(device_factory)
    with patch(
        "ctaops.sdk.controllers.scsi.library.LocalLocatedNamedDriveRepository",
        return_value=drive_repo or _stub_drive_repo(),
    ):
        return ScsiManagedLibraryController(sg)


def test_init_rejects_non_medium_changer(device_factory: Callable[..., MagicMock]):
    bad = device_factory(
        sys_name="sg0",
        subsystem="scsi_generic",
        properties={"SCSI_TYPE": "disk"},
    )
    with pytest.raises(ValueError, match="not a valid tape library changer device"):
        ScsiManagedLibraryController(bad)


def test_serial_number_returns_id_scsi_serial(device_factory: Callable[..., MagicMock]):
    ctrl = _build_managed_controller(device_factory)
    assert ctrl.serial_number == "LIBSN"


def test_serial_number_missing_raises(device_factory: Callable[..., MagicMock]):
    sg = device_factory(
        sys_name="sg0",
        subsystem="scsi_generic",
        properties={"SCSI_TYPE": "medium changer"},
    )
    ctrl = ScsiManagedLibraryController(sg)
    with pytest.raises(ValueError, match="ID_SCSI_SERIAL"):
        ctrl.serial_number  # noqa: B018


def test_drive_is_free_parses_json(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(cmd: list[str], **kwargs: Any):
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=json.dumps([{"status": "free"}]))

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", fake_run)
    assert ctrl.drive_is_free(0) is True


def test_drive_is_free_busy_returns_false(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout=json.dumps([{"status": "loaded"}]))

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    assert ctrl.drive_is_free(0) is False


def test_drive_is_free_invalid_json_raises_value_error(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout="<not-json>")

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    with pytest.raises(ValueError, match="Invalid response from cta-smc"):
        ctrl.drive_is_free(0)


def test_drive_is_free_subprocess_error_wraps(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def boom(*a: Any, **kw: Any):
        raise subprocess.CalledProcessError(returncode=2, cmd=a[0])

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", boom)
    with pytest.raises(LibraryOperationError):
        ctrl.drive_is_free(0)


def test_import_tapes_invokes_cta_smc_without_shell(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    """Regression: --import must reach cta-smc as a real argv entry, not via shell=True."""
    ctrl = _build_managed_controller(device_factory)
    seen: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any):
        seen["cmd"] = list(cmd)
        seen["shell"] = kwargs.get("shell", False)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", fake_run)
    ctrl.import_tapes()
    assert seen["cmd"] == ["/usr/bin/cta-smc", "-i"]
    assert seen["shell"] is False


def test_import_tapes_subprocess_error_wraps(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def boom(*a: Any, **kw: Any):
        raise subprocess.CalledProcessError(returncode=1, cmd=a[0])

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", boom)
    with pytest.raises(LibraryOperationError):
        ctrl.import_tapes()


def test_export_tape_runs_cta_smc(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)
    seen: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any):
        seen["cmd"] = list(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="")

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", fake_run)
    ctrl.export_tape("ABC123")
    assert seen["cmd"][:2] == ["/usr/bin/cta-smc", "--json"]
    assert "ABC123" in seen["cmd"]


def test_label_tape_with_old_vid_and_force(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)
    seen: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any):
        seen["cmd"] = list(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", fake_run)
    ctrl.label_tape(0, "NEW", old_vid="OLD", force=True)
    cmd = seen["cmd"]
    assert cmd[0] == "/usr/bin/cta-tape-label"
    assert "--vid" in cmd
    assert "NEW" in cmd
    assert "--oldlabel" in cmd
    assert "OLD" in cmd
    assert "--force" in cmd


def test_label_tape_subprocess_error_raises_label_error(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def boom(*a: Any, **kw: Any):
        raise subprocess.CalledProcessError(returncode=1, cmd=a[0])

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", boom)
    with pytest.raises(TapeLabelError):
        ctrl.label_tape(0, "NEW")


def test_get_tape_location_drive(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(
            args=a[0], returncode=0, stdout=json.dumps([{"elementType": "drive", "elementAddress": "256"}])
        )

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    loc = ctrl.get_tape_location("ABC123")
    assert loc.location == "DRIVE"
    assert loc.address == 256
    assert loc.vid == "ABC123"
    assert loc.library_serial_number == "LIBSN"


def test_get_tape_location_home(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(
            args=a[0], returncode=0, stdout=json.dumps([{"elementType": "slot", "elementAddress": "12"}])
        )

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    loc = ctrl.get_tape_location("ABC123")
    assert loc.location == "HOME"


def test_get_tape_location_unknown_for_other_element_type(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(
            args=a[0], returncode=0, stdout=json.dumps([{"elementType": "transport", "elementAddress": "12"}])
        )

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    loc = ctrl.get_tape_location("ABC123")
    assert loc.location == "UNKNOWN"


def test_get_tape_location_missing_raises_resource_not_found(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout="[]")

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    with pytest.raises(ResourceNotFoundError):
        ctrl.get_tape_location("ABC123")


def test_get_max_io_slots(device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout=json.dumps([{"port": {"count": "12"}}]))

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    assert ctrl.get_max_io_slots() == 12


@pytest.mark.parametrize(
    "stdout",
    [
        "{}",  # not a list
        "[]",  # empty list
        "[null]",  # not a dict
        "[{}]",  # missing port
        json.dumps([{"port": []}]),  # port not a dict
        json.dumps([{"port": {}}]),  # missing count
        json.dumps([{"port": {"count": "not-a-number"}}]),  # not int-coercible
        json.dumps([{"port": {"count": None}}]),  # None count
        "<not-json>",  # malformed JSON
    ],
)
def test_get_max_io_slots_invalid_response_raises(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch, stdout: str
):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout=stdout)

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    with pytest.raises(LibraryOperationError) as excinfo:
        ctrl.get_max_io_slots()
    # The wrapped error must include the actual cta-smc payload so an
    # operator can see what the library returned.
    assert repr(stdout) in str(excinfo.value) or "count" in str(excinfo.value)


def test_drive_key_unknown_type_raises_type_error(device_factory: Callable[..., MagicMock]):
    ctrl = _build_managed_controller(device_factory)
    with pytest.raises(TypeError):
        ctrl._get_drive_metadata(1.5)  # type: ignore[arg-type]


def test_unmount_no_op_when_drive_already_free(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout=json.dumps([{"status": "free"}]))

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.library.subprocess.run",
        fake_run,
    )
    ctrl.unmount(0)


def test_mount_raises_drive_state_error_when_drive_busy_with_other_tape(
    device_factory: Callable[..., MagicMock], monkeypatch: pytest.MonkeyPatch
):
    ctrl = _build_managed_controller(device_factory)

    # First call: super().drive_is_free → False (loaded)
    # Second call: _get_drive_vid → returns a *different* vid
    # Third call: drive_is_free → False (still loaded)
    responses = iter(
        [
            json.dumps([{"status": "loaded"}]),
            json.dumps([{"vid": "OTHER"}]),
            json.dumps([{"status": "loaded"}]),
        ]
    )

    def fake_run(*a: Any, **kw: Any):
        return subprocess.CompletedProcess(args=a[0], returncode=0, stdout=next(responses))

    fake_drive_controller = MagicMock()
    fake_drive_controller.has_loaded_tape.return_value = True
    ctrl._drive_controller_provider.get_drive_controller = MagicMock(  # type: ignore[method-assign]
        return_value=fake_drive_controller
    )

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.library.subprocess.run", fake_run)
    with pytest.raises(DriveStateError):
        ctrl.mount(0, "WANTED")
