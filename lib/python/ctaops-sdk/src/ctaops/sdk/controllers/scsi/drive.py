# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""SCSI-backed implementations of the drive controller protocols.

See [ctaops.sdk.controllers.protocols.drive][ctaops.sdk.controllers.protocols.drive] for the public contract.
The classes in this module talk to the kernel through `/dev/sgN` /
`/dev/stN` / `/dev/nstN` (resolved via pyudev) and the `mt` /
`sg_logs` userland tools.
"""

import errno
import logging
import os
import re
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Callable, Optional
from zlib import adler32

from ctaops.sdk.controllers.protocols.drive import DriveController, MountedDriveController
from ctaops.sdk.exceptions import DriveOperationError, DriveStateError, EODError, EOTError
from pyudev import Context, Device
from typing_extensions import override

LOGGER = logging.getLogger("ctaops.sdk")

_IBM_DRIVE_REGEX = "^Log page code=0xd,0x1, DS=0, SPF=1, page_len=0x"
_HEX_ROW_RE = re.compile(r"^\s+(?P<offset>[0-9a-fA-F]{2,4})\s+(?P<bytes>(?:[0-9a-fA-F]{2}\s+){0,15}[0-9a-fA-F]{2})")


class ScsiDriveController(DriveController):
    """SCSI-backed [DriveController][ctaops.sdk.controllers.protocols.drive.DriveController].

    Wraps a single `scsi_tape` device (`/dev/nstN`) plus the matching
    generic device (`/dev/sgN`) and rewinding device (`/dev/stN`)
    discovered through pyudev. Equality and hashing are based on the SCSI
    serial number, so two controllers built from the same drive compare
    equal even if their pyudev `Device` objects differ.

    Args:
        nst_device: The `scsi_tape` udev device for the drive (`nstN`).
        context: Optional pyudev `Context` to reuse. When omitted, a fresh
            context is created — pass an explicit one when multiple
            controllers should share a single udev snapshot.

    Raises:
        ValueError: If `nst_device` is not an `nstN` `scsi_tape` device.
    """

    def __init__(self, nst_device: Device, context: Optional[Context] = None):
        LOGGER.debug("Initializing ScsiDriveController for device %s", nst_device)
        if nst_device.subsystem != "scsi_tape" or not re.match(r"^nst[0-9]+$", nst_device.sys_name):
            raise ValueError(f"Device {nst_device} is not a valid tape drive nst device")

        self._context = Context() if context is None else context
        self._nst_device = nst_device
        self._sg_device = next(
            iter(self._context.list_devices(subsystem="scsi_generic", parent=self._nst_device.parent))
        )
        self._st_device = next(
            device
            for device in iter(self._context.list_devices(subsystem="scsi_tape", parent=self._nst_device.parent))
            if re.match(r"^st[0-9]+$", device.sys_name)
        )

    @override
    def __hash__(self) -> int:
        return hash(self.serial_number)

    @override
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ScsiDriveController):
            return False
        return self.serial_number == value.serial_number

    def _mt_status_has_flag(self, stdout: str, flag: str) -> bool:
        """Return `True` if `flag` appears as a whole token in `mt status`.

        `mt status` lists drive flags by name (`ONLINE`, `WR_PROT`,
        `IM_REP_EN`, …). Substring matching with `flag in stdout` would
        let unrelated text trigger false positives — anchor on word
        boundaries instead.
        """
        return re.search(rf"\b{re.escape(flag)}\b", stdout) is not None

    def _extract_sg_logs_byte(self, stdout: str, offset: int) -> int:
        """Return byte `offset` of the parameter value block from `sg_logs --hex`.

        Each row is parsed structurally — leading hex offset, then up to 16
        2-digit hex tokens — instead of relying on column spacing. This
        avoids the previous regex's dependence on a specific double-space
        placement that real `sg_logs` output does not always produce.

        Raises:
            DriveOperationError: When no content row is found or the dump is
                shorter than `offset + 1` bytes.
        """
        first_match = next((match for line in stdout.splitlines() if (match := _HEX_ROW_RE.match(line))), None)
        if not first_match:
            raise DriveOperationError("sg_logs --hex output contained no recognisable content row")

        data = bytearray(int(token, 16) for token in first_match.group("bytes").split())
        if len(data) > offset:
            return data[offset]

        raise DriveOperationError(f"sg_logs --hex dump is {len(data)} bytes, expected at least {offset + 1}")

    @property
    def _sg_path(self) -> Path:
        device_node = self._sg_device.device_node
        if device_node is None:
            raise DriveOperationError(f"sg device for drive {self._nst_device.sys_name} has no device node")
        path = Path(device_node)
        if not path.is_char_device():
            raise DriveOperationError(
                f"sg device node {device_node} for drive {self._nst_device.sys_name} is not a character device"
            )
        return path

    @property
    def _nst_path(self) -> Path:
        device_node = self._nst_device.device_node
        if device_node is None:
            raise DriveOperationError(f"nst device {self._nst_device.sys_name} has no device node")
        path = Path(device_node)
        if not path.is_char_device():
            raise DriveOperationError(f"nst device node {device_node} is not a character device")
        return path

    @property
    def _st_path(self) -> Path:
        device_node = self._st_device.device_node
        if device_node is None:
            raise DriveOperationError(f"st device {self._st_device.sys_name} has no device node")
        path = Path(device_node)
        if not path.is_char_device():
            raise DriveOperationError(f"st device node {device_node} is not a character device")
        return path

    @property
    @override
    def serial_number(self) -> str:
        """SCSI serial number from the `ID_SCSI_SERIAL` udev property.

        Raises:
            ValueError: If the device has no `ID_SCSI_SERIAL` property.
        """
        serial_number = self._nst_device.properties.get("ID_SCSI_SERIAL")
        if not serial_number:
            raise ValueError(f"Device {self._nst_device} does not have an ID_SCSI_SERIAL property")
        return serial_number

    @override
    def get_drive_temperature(self) -> int:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.DriveController.get_drive_temperature].

        Implementation: queries SCSI log page `0x0D` subpage `0x01`
        parameter `0x0000` via `sg_logs --hex` and decodes byte 9 of the
        first content row.
        """
        LOGGER.debug("sg_logs temperature query on drive %s", self.serial_number)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                [
                    "/usr/bin/sg_logs",
                    "--page",
                    "0x0D,0x01",
                    "--control",
                    "1",
                    "--hex",
                    "--paramp",
                    "0x0000",
                    self._sg_path,
                ],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise DriveOperationError("Failed to get drive temperature from sg_logs") from e

        if not re.search(_IBM_DRIVE_REGEX, cmd_call.stdout.split("\n")[0]):
            raise DriveOperationError(f"Drive {self.serial_number} does not support IBM temperature monitoring")

        return self._extract_sg_logs_byte(cmd_call.stdout, 9)

    @override
    def get_drive_humidity(self) -> int:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.DriveController.get_drive_humidity].

        Implementation: queries SCSI log page `0x0D` subpage `0x01`
        parameter `0x0100` via `sg_logs --hex` and decodes byte 9 of the
        first content row.
        """
        LOGGER.debug("sg_logs humidity query on drive %s", self.serial_number)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                [
                    "/usr/bin/sg_logs",
                    "--page",
                    "0x0D,0x01",
                    "--control",
                    "1",
                    "--hex",
                    "--paramp",
                    "0x0100",
                    self._sg_path,
                ],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise DriveOperationError("Failed to get drive humidity from sg_logs") from e

        if not re.search(_IBM_DRIVE_REGEX, cmd_call.stdout.split("\n")[0]):
            raise DriveOperationError(f"Drive {self.serial_number} does not support IBM humidity monitoring")

        return self._extract_sg_logs_byte(cmd_call.stdout, 9)

    @override
    def has_loaded_tape(self) -> bool:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.DriveController.has_loaded_tape].

        Implementation: parses `mt -f <st> status` output for the
        `ONLINE` flag.
        """
        LOGGER.debug("Checking if drive %s has a tape loaded", self.serial_number)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/bin/mt", "-f", self._st_path, "status"],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise DriveOperationError(f"Unable to get status information with mt tool for drive {self}") from e
        return self._mt_status_has_flag(cmd_call.stdout, "ONLINE")

    @override
    def unload(self) -> None:
        """See [DriveController.unload][ctaops.sdk.controllers.protocols.drive.DriveController.unload].

        Implementation: runs `mt -f <st> eject` only if a tape is currently
        loaded.
        """
        if self.has_loaded_tape():
            LOGGER.info("Ejecting tape from drive %s", self.serial_number)
            try:
                subprocess.run(  # noqa: S603 - fixed tool, argv only.
                    ["/bin/mt", "-f", self._st_path, "eject"],
                    stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                    stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                    text=True,
                    timeout=180,
                    check=True,
                )

            except subprocess.SubprocessError as e:
                raise DriveOperationError(f"Unable to eject tape inside drive {self} with mt tool") from e

    @override
    def set_compression(self, value: int) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.DriveController.set_compression].

        Implementation: runs `mt -f <st> compression <value>`.
        """
        LOGGER.info("Setting compression=%s on drive %s", value, self.serial_number)
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/bin/mt", "-f", self._st_path, "compression", str(value)],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )

        except subprocess.SubprocessError as e:
            raise DriveOperationError(f"Unable to set compression for drive {self} with mt tool") from e

    @override
    def __repr__(self) -> str:
        return f"ScsiDriveController(serial_number={self.serial_number})"


class ScsiMountedDriveController(ScsiDriveController, MountedDriveController):
    """SCSI-backed [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController].

    Always built by
    [ScsiManagedLibraryController.mount][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController.mount];
    not part of the public construction API. The `unmount_callback` passed
    in must return the tape to its home slot — the controller invokes it
    exactly once, on context exit or on the first explicit
    [unmount][ctaops.sdk.controllers.scsi.drive.ScsiMountedDriveController.unmount]
    call.

    Args:
        nst_device: The `scsi_tape` udev device for the drive.
        vid: Volume identifier of the tape that has just been mounted.
        unmount_callback: Zero-arg callable that performs the actual unmount
            via the library.
        context: Optional pyudev `Context`.
    """

    def __init__(
        self,
        nst_device: Device,
        vid: str,
        unmount_callback: Callable[[], None],
        context: Optional[Context] = None,
    ) -> None:
        super().__init__(nst_device, context)
        LOGGER.debug("Initializing ScsiMountedDriveController for device %s with vid=%s", nst_device, vid)
        self._mounted_vid = vid
        self._unmount_callback = unmount_callback
        self._unmounted = False

    @property
    @override
    def mounted_vid(self) -> str:
        """Volume identifier of the tape currently mounted on this drive."""
        return self._mounted_vid

    @override
    def __enter__(self) -> "ScsiMountedDriveController":
        return self

    @override
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[object],
    ) -> Optional[bool]:
        """Unmount the tape on context exit (idempotent with manual unmount)."""
        self.unmount()

    @override
    def unmount(self) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.unmount].
        """
        if not self._unmounted:
            LOGGER.info("Unmounting tape %s from drive %s", self._mounted_vid, self.serial_number)
            self._unmount_callback()  # unload is made by the callback directly
            self._unmounted = True

    def _checksum(self, block: bytes, file_size: int, block_size: int = 256 * 1024) -> int:
        """Return the Adler-32 checksum of `block` repeated to `file_size` bytes.

        Args:
            block: Byte buffer to repeat.
            file_size: Total size in bytes to checksum.
            block_size: Granularity of the repetition (default 256 KiB).
        """
        checksum = 1
        for _ in range(file_size // block_size):
            checksum = adler32(block, checksum)
        if (remaining_size := file_size % block_size) > 0:
            checksum = adler32(block[:remaining_size], checksum)
        return checksum

    @override
    def write_files(
        self,
        file_size: int,
        n: Optional[int],
        block_size: int = 256 * 1024,
    ) -> Generator[tuple[int, int], None, None]:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.write_files].

        Implementation: each file is one `open(nst, "wb")` so the kernel
        emits a filemark when the file is closed. The pseudo-random buffer
        is generated once with [os.urandom][os.urandom]; its checksum is
        computed once and reused for every yielded file.

        Note:
            On `ENOSPC` the generator raises
            [EOTError][ctaops.sdk.exceptions.EOTError] carrying the count
            of *successful* files (not including the one that failed
            mid-write).
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")
        block = os.urandom(block_size)
        checksum = self._checksum(block, file_size, block_size)
        file_index = 1
        LOGGER.info(
            "Starting write_files on drive %s (vid=%s, file_size=%s, n=%s)",
            self.serial_number,
            self._mounted_vid,
            file_size,
            n,
        )
        try:
            while True:
                with open(self._nst_path, "wb", buffering=0) as tape_dev:
                    tape_dev.writelines(block for _ in range(file_size // block_size))

                    if (remaining_size := file_size % block_size) > 0:
                        tape_dev.write(block[:remaining_size])

                yield (file_index, checksum)
                if n is not None and file_index >= n:
                    return
                file_index += 1
        except OSError as e:
            if e.errno == errno.ENOSPC:
                LOGGER.warning("EOT reached on drive %s after %s file(s)", self.serial_number, file_index - 1)
                raise EOTError(checksum=checksum, written_files=file_index - 1) from e
            raise

    @override
    def read_files(
        self,
        n: int,
        block_size: int = 256 * 1024,
    ) -> Generator[tuple[int, int], None, None]:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.read_files].

        Implementation: opens `nst` once and reads until each filemark
        (a zero-byte read). The Adler-32 checksum is accumulated as data is
        read so memory usage is constant regardless of file size.
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")

        LOGGER.info("Starting read_files on drive %s (vid=%s, n=%s)", self.serial_number, self._mounted_vid, n)
        with open(self._nst_path, "rb", buffering=0) as tape_dev:
            for file_index in range(1, n + 1):
                checksum = 1
                blocks_read = 0

                while True:
                    data = tape_dev.read(block_size)
                    if not data:
                        break
                    checksum = adler32(data, checksum)
                    blocks_read += 1

                if blocks_read == 0:
                    LOGGER.warning("EOD reached on drive %s after %s file(s)", self.serial_number, file_index - 1)
                    raise EODError(f"End of tape reached after {file_index} files")
                yield (file_index, checksum)

    def _skip_vol1(self) -> None:
        """Read and discard the 80-byte VOL1 record at the current position."""
        with open(self._nst_path, "rb", buffering=0) as dev:
            dev.read(80)
        os.sync()

    @override
    def rewind(self, skip_vol1: bool = True) -> None:
        """See [MountedDriveController.rewind][ctaops.sdk.controllers.protocols.drive.MountedDriveController.rewind].

        Implementation: `mt -f <st> rewind` followed (when `skip_vol1`
        is `True`) by an 80-byte read on `nst` to step past the VOL1
        record.
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")
        LOGGER.info("Rewinding drive %s (skip_vol1=%s)", self.serial_number, skip_vol1)
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/bin/mt", "-f", self._st_path, "rewind"],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=120,
                check=True,
            )

        except subprocess.SubprocessError as e:
            raise DriveOperationError(f"Unable to rewind the tape in drive {self} with mt tool") from e

        if skip_vol1:
            self._skip_vol1()

    @override
    def get_VOL1(self) -> str:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.get_VOL1].

        Implementation: rewinds the tape (without skipping VOL1) and reads
        the first 80 bytes from `nst` decoded as UTF-8.
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")

        self.rewind(skip_vol1=False)

        LOGGER.debug("Reading VOL1 record from drive %s", self.serial_number)
        with open(self._nst_path, "rb", buffering=0) as dev:
            label = dev.read(80)

        return label.decode("utf-8")

    @override
    def is_tape_write_protected(self) -> bool:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.is_tape_write_protected].

        Implementation: parses `mt -f <st> status` for the `WR_PROT` flag.
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")

        LOGGER.debug("Checking if tape in drive %s is write-protected", self.serial_number)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/bin/mt", "-f", self._st_path, "status"],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise DriveOperationError(f"Unable to get status information with mt tool for drive {self}") from e
        return self._mt_status_has_flag(cmd_call.stdout, "WR_PROT")

    @override
    def set_block_size(self, value: int) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.drive.MountedDriveController.set_block_size].

        Implementation: runs `mt -f <st> setblk <value>`.
        """
        if self._unmounted:
            raise DriveStateError("Drive was unmounted")
        LOGGER.info("Setting block size to %s on drive %s", value, self.serial_number)
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/bin/mt", "-f", self._st_path, "setblk", str(value)],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=20,
                check=True,
            )

        except subprocess.SubprocessError as e:
            raise DriveOperationError(f"Unable to set block size for drive {self} with mt tool") from e

    @classmethod
    def from_drive_controller(
        cls, drive_controller: ScsiDriveController, vid: str, unmount_callback: Callable[[], None]
    ) -> "ScsiMountedDriveController":
        """Build a mounted controller from an existing drive controller.

        Reuses the underlying `nst_device` and pyudev `Context` of the
        source controller so a single udev snapshot covers both.

        Args:
            drive_controller: The source drive controller.
            vid: Volume identifier of the just-mounted tape.
            unmount_callback: Zero-arg callable that returns the tape to
                its home slot.
        """
        return cls(drive_controller._nst_device, vid, unmount_callback, drive_controller._context)

    @override
    def __repr__(self) -> str:
        return f"ScsiMountedDriveController(serial_number={self.serial_number})"
