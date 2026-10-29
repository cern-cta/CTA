"""Tests for ScsiDriveController and ScsiMountedDriveController.

Both classes delegate to subprocess for sg_logs/mt and to direct device-node
file I/O. We monkeypatch subprocess.run, builtins.open, os.urandom, os.sync,
and pathlib.Path.is_char_device to keep tests hermetic.
"""

import errno
import subprocess
from typing import Any
from unittest.mock import MagicMock

import pytest
from ctaops.sdk.controllers.scsi.drive import (
    ScsiDriveController,
    ScsiMountedDriveController,
)
from ctaops.sdk.exceptions import (
    DriveOperationError,
    DriveStateError,
    EODError,
    EOTError,
)


@pytest.fixture(autouse=True)
def patch_is_char_device(monkeypatch):
    """Treat any /dev/* path the SDK opens as a character device."""
    monkeypatch.setattr("pathlib.Path.is_char_device", lambda self: True)


def _build_st_device(device_factory, *, sys_name="nst0", serial="DRSN", ctaops_name="T0-LIB0-DRV0"):
    parent = MagicMock(name="ScsiTarget")
    nst_device = device_factory(
        sys_name=sys_name,
        subsystem="scsi_tape",
        device_node=f"/dev/{sys_name}",
        parent=parent,
        properties={"ID_SCSI_SERIAL": serial, "CTAOPS_NAME": ctaops_name},
    )
    sg_device = device_factory(
        sys_name="sg5",
        subsystem="scsi_generic",
        device_node="/dev/sg5",
        parent=parent,
    )
    st_device = device_factory(
        sys_name="st0",
        subsystem="scsi_tape",
        device_node="/dev/st0",
        parent=parent,
    )
    return nst_device, sg_device, st_device


@pytest.fixture
def fake_context(device_factory, make_context):
    nst_device, sg_device, st_device = _build_st_device(device_factory)
    ctx = make_context([nst_device, sg_device, st_device])
    return ctx, nst_device


def test_init_rejects_non_st_device(device_factory, make_context):
    bad = device_factory(sys_name="sda", subsystem="block")
    with pytest.raises(ValueError):
        ScsiDriveController(bad, context=make_context([]))


def test_init_rejects_st_with_wrong_sys_name(device_factory, make_context):
    bad = device_factory(sys_name="sttX", subsystem="scsi_tape")
    with pytest.raises(ValueError):
        ScsiDriveController(bad, context=make_context([]))


def test_serial_number_returns_id_scsi_serial(fake_context):
    ctx, nst = fake_context
    drv = ScsiDriveController(nst, context=ctx)
    assert drv.serial_number == "DRSN"


def test_serial_number_missing_raises(device_factory, make_context):
    parent = MagicMock(name="ScsiTarget")
    nst = device_factory(
        sys_name="nst0",
        subsystem="scsi_tape",
        parent=parent,
        properties={},
    )
    sg = device_factory(sys_name="sg5", subsystem="scsi_generic", parent=parent)
    st = device_factory(sys_name="st0", subsystem="scsi_tape", parent=parent)
    drv = ScsiDriveController(nst, context=make_context([nst, sg, st]))
    with pytest.raises(ValueError):
        drv.serial_number  # noqa: B018


def test_set_compression_invokes_mt(monkeypatch, fake_context):
    ctx, nst = fake_context
    drv = ScsiDriveController(nst, context=ctx)
    seen: dict[str, Any] = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = list(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.subprocess.run", fake_run)
    drv.set_compression(1)

    assert seen["cmd"][:2] == ["/bin/mt", "-f"]
    assert str(seen["cmd"][2]) == "/dev/st0"
    assert seen["cmd"][3:] == ["compression", "1"]


def test_set_compression_subprocess_error_wrapped(monkeypatch, fake_context):
    ctx, nst = fake_context
    drv = ScsiDriveController(nst, context=ctx)

    def fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0])

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.subprocess.run", fake_run)
    with pytest.raises(DriveOperationError):
        drv.set_compression(1)


def test_has_loaded_tape_true_when_online(monkeypatch, fake_context):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout="DRIVE: ONLINE\n"),
    )
    assert drv.has_loaded_tape() is True


def test_has_loaded_tape_false_when_no_online(monkeypatch, fake_context):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout="DRIVE: OFFLINE\n"),
    )
    assert drv.has_loaded_tape() is False


def test_has_loaded_tape_uses_word_boundaries(monkeypatch, fake_context):
    """Regression: `ONLINE` embedded in a longer token must NOT trigger a
    positive — the previous `"ONLINE" in stdout` check did, the new
    word-boundary matcher does not."""
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout="status: NOT_ONLINE_YET\n"),
    )
    assert drv.has_loaded_tape() is False


def test_unload_skips_when_no_tape(monkeypatch, fake_context):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        if "status" in cmd:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="OFFLINE")
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.subprocess.run", fake_run)
    drv.unload()
    assert len(calls) == 1  # only the status check, no eject


def test_unload_runs_eject_when_tape_loaded(monkeypatch, fake_context):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        if "status" in cmd:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="ONLINE")
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.subprocess.run", fake_run)
    drv.unload()
    assert any("eject" in c for c in calls)


@pytest.mark.parametrize(
    "row",
    [
        # Real-world style: row offset prefix, single spaces between bytes
        "    00 11 22 33 44 55 66 77 88 09 19 aa bb cc dd ee\n",
        # Double-space between bytes 8 and 9 (the format the legacy regex relied on)
        "    00 11 22 33 44 55 66 77 88  09 19 aa bb cc dd ee\n",
        # 4-digit row offset (also legal)
        "    0000 11 22 33 44 55 66 77 88 09 19 aa bb cc dd ee\n",
    ],
)
def test_get_drive_temperature_parses_value(monkeypatch, fake_context, row):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    sg_logs_output = "Log page code=0xd,0x1, DS=0, SPF=1, page_len=0x100\n" + row
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout=sg_logs_output),
    )
    assert drv.get_drive_temperature() == 0x19


def test_get_drive_temperature_short_dump_raises(monkeypatch, fake_context):
    """The hex parser must fail loudly when the dump is shorter than expected."""
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    sg_logs_output = (
        "Log page code=0xd,0x1, DS=0, SPF=1, page_len=0x100\n"
        "    00 11 22 33\n"  # only 4 bytes — not enough for offset 9
    )
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout=sg_logs_output),
    )
    with pytest.raises(DriveOperationError):
        drv.get_drive_temperature()


def test_get_drive_temperature_unsupported_drive(monkeypatch, fake_context):
    ctx, st = fake_context
    drv = ScsiDriveController(st, context=ctx)
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout="some other format\n"),
    )
    with pytest.raises(DriveOperationError):
        drv.get_drive_temperature()


# --- Mounted drive tests --------------------------------------------------- #


def _build_mounted(monkeypatch, device_factory, make_context, vid="VID000"):
    st_device, sg_device, nst_device = _build_st_device(device_factory)
    ctx = make_context([st_device, sg_device, nst_device])
    callback = MagicMock(name="UnmountCallback")
    md = ScsiMountedDriveController(st_device, vid, callback, context=ctx)
    return md, callback


def test_mounted_drive_unmount_runs_once(device_factory, make_context, monkeypatch):
    md, callback = _build_mounted(monkeypatch, device_factory, make_context)

    md.unmount()
    md.unmount()
    assert callback.call_count == 1


def test_mounted_drive_context_manager_invokes_callback(device_factory, make_context, monkeypatch):
    md, callback = _build_mounted(monkeypatch, device_factory, make_context)

    with md:
        pass
    assert callback.call_count == 1


def test_checksum_matches_adler32_of_repeated_block():
    from zlib import adler32

    md_cls = ScsiMountedDriveController
    block = b"\x01\x02\x03\x04" * 64  # 256 bytes
    file_size = 1000
    block_size = 256
    expected = 1
    full = block * (file_size // block_size) + block[: file_size % block_size]
    expected = adler32(full)

    # Use a bound method via a synthetic instance bypassing __init__.
    instance = md_cls.__new__(md_cls)
    actual = instance._checksum(block, file_size, block_size)  # type: ignore[arg-type]
    assert actual == expected


def test_write_files_yields_then_raises_eot_on_enospc(device_factory, make_context, monkeypatch):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.os.urandom", lambda n: b"\x00" * n)

    write_calls = {"count": 0}

    class _BoomDev:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def write(self, data):
            write_calls["count"] += 1
            # Blow up on third file, mid-write
            if write_calls["count"] > 8:
                err = OSError("ENOSPC")
                err.errno = errno.ENOSPC
                raise err

    monkeypatch.setattr("builtins.open", lambda *a, **kw: _BoomDev())

    yielded: list[tuple[int, int]] = []
    with pytest.raises(EOTError) as excinfo:
        for item in md.write_files(file_size=1024, n=10, block_size=256):
            yielded.append(item)

    # We yielded at least one full file before the next file ran out of space.
    assert yielded
    assert excinfo.value.written_files == len(yielded)


def test_write_files_raises_when_already_unmounted(device_factory, make_context, monkeypatch):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)
    md.unmount()

    with pytest.raises(DriveStateError):
        list(md.write_files(file_size=1024, n=1, block_size=256))


def test_read_files_yields_per_file_then_raises_eod(device_factory, make_context, monkeypatch):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)

    sequence = iter(
        [
            b"\x01" * 8,
            b"",  # filemark for file 1
            b"\x02" * 8,
            b"",  # filemark for file 2
            b"",  # filemark, no data => EODError
        ]
    )

    class _Dev:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, _block_size):
            return next(sequence)

    monkeypatch.setattr("builtins.open", lambda *a, **kw: _Dev())

    out: list[tuple[int, int]] = []
    with pytest.raises(EODError):
        for item in md.read_files(n=5, block_size=256):
            out.append(item)

    assert [idx for idx, _ in out] == [1, 2]


def test_read_files_blocks_when_already_unmounted(device_factory, make_context, monkeypatch):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)
    md.unmount()
    with pytest.raises(DriveStateError):
        list(md.read_files(n=1, block_size=256))


def test_get_vol1_returns_decoded_label(device_factory, make_context, monkeypatch):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)
    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.os.sync", lambda: None)

    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0),
    )

    label_bytes = b"VOL1ABC123" + b" " * 70

    class _Dev:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, n):
            return label_bytes[:n]

    monkeypatch.setattr("builtins.open", lambda *a, **kw: _Dev())
    assert md.get_VOL1() == label_bytes.decode("utf-8")


def test_is_tape_write_protected(monkeypatch, device_factory, make_context):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)
    monkeypatch.setattr(
        "ctaops.sdk.controllers.scsi.drive.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=a[0], returncode=0, stdout="WR_PROT"),
    )
    assert md.is_tape_write_protected() is True


def test_set_block_size_runs_mt_setblk(monkeypatch, device_factory, make_context):
    md, _ = _build_mounted(monkeypatch, device_factory, make_context)
    seen: dict[str, Any] = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = list(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("ctaops.sdk.controllers.scsi.drive.subprocess.run", fake_run)
    md.set_block_size(262144)
    assert "setblk" in seen["cmd"]
    assert "262144" in seen["cmd"]
