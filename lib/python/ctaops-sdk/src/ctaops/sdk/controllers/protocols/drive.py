# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols describing how callers drive a tape drive.

Two layers are exposed:

* [DriveController][ctaops.sdk.controllers.protocols.drive.DriveController]
  — operations safe to perform on a drive whether or not a tape is
  currently mounted (status queries, environmental sensors, ejection,
  drive-level configuration).
* [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
  — extends `DriveController` with I/O operations that require a mounted
  tape (read/write, rewind, label inspection, write-protect query). Always
  obtained from a library `mount(...)` call and used as a context manager
  so the tape is unmounted on exit.

Concrete SCSI-backed implementations:
[ScsiDriveController][ctaops.sdk.controllers.scsi.drive.ScsiDriveController]
and
[ScsiMountedDriveController][ctaops.sdk.controllers.scsi.drive.ScsiMountedDriveController].
"""

from collections.abc import Iterator
from typing import Optional, Protocol


class DriveController(Protocol):
    """Hardware-level operations on a tape drive.

    Implementations talk to the drive through `mt` and `sg_logs`. These
    operations do not require a mounted tape — they work whether the drive
    is empty or loaded.

    Example:
        ```python
        from ctaops.sdk import ScsiDriveControllerProvider

        provider = ScsiDriveControllerProvider()
        drive = provider.get_drive_controller_by_name("drive-0")
        if drive.has_loaded_tape():
            drive.unload()
        print(drive.get_drive_temperature())
        ```
    """

    @property
    def serial_number(self) -> str:
        """SCSI serial number of the drive."""
        ...

    def unload(self) -> None:
        """Eject the currently-loaded tape, if any.

        No-op if the drive is already empty.

        Raises:
            DriveOperationError: If the eject command fails.
        """
        ...

    def has_loaded_tape(self) -> bool:
        """Return `True` iff a tape is currently loaded and the drive is online.

        Raises:
            DriveOperationError: If the drive status cannot be queried.
        """
        ...

    def set_compression(self, value: int) -> None:
        """Configure hardware compression on the drive.

        Args:
            value: Compression level. `0` disables compression; non-zero
                values enable it (drive-specific semantics).

        Raises:
            DriveOperationError: If the configuration command fails.
        """
        ...

    def get_drive_temperature(self) -> int:
        """Return the drive's current internal temperature in degrees Celsius.

        Note:
            This relies on IBM-specific log pages. Drives that do not report
            environmental data on the expected SCSI log page raise
            [DriveOperationError][ctaops.sdk.exceptions.DriveOperationError].

        Raises:
            DriveOperationError: If the drive does not support IBM
                environmental monitoring or the response cannot be parsed.
        """
        ...

    def get_drive_humidity(self) -> int:
        """Return the drive's current internal relative humidity (percent).

        Note:
            This relies on IBM-specific log pages. Drives that do not report
            environmental data on the expected SCSI log page raise
            [DriveOperationError][ctaops.sdk.exceptions.DriveOperationError].

        Raises:
            DriveOperationError: If the drive does not support IBM
                environmental monitoring or the response cannot be parsed.
        """
        ...


class MountedDriveController(DriveController, Protocol):
    """A drive with a mounted tape, ready for I/O.

    Instances are context managers: the underlying tape is unmounted (and
    returned to its home slot) automatically when the `with` block exits,
    whether normally or via exception. Always obtained from
    [ManagedLibraryController.mount][ctaops.sdk.controllers.protocols.library.ManagedLibraryController.mount],
    never built directly.

    Warning:
        Once the context exits — or
        [unmount][ctaops.sdk.controllers.protocols.drive.MountedDriveController.unmount]
        has been called — every other method on this object raises
        [DriveStateError][ctaops.sdk.exceptions.DriveStateError]. Do not
        stash a reference past the `with` block.

    Example:
        ```python
        with library.mount("drive-0", "X12345") as drive:
            drive.set_block_size(256 * 1024)
            for file_index, checksum in drive.write_files(file_size=1 << 30, n=10):
                print(file_index, checksum)
        ```
    """

    @property
    def mounted_vid(self) -> str:
        """Volume identifier (barcode) of the tape currently mounted."""
        ...

    def unmount(self) -> None:
        """Manually unmount the tape now instead of waiting for context exit.

        Idempotent — subsequent calls (and the eventual `__exit__`) are
        no-ops. After this returns the controller is unusable; use it only
        when you cannot rely on a `with` block.
        """
        ...

    def rewind(self, skip_vol1: bool = True) -> None:
        """Rewind the tape to the beginning.

        Args:
            skip_vol1: If `True` (default), advance past the 80-byte VOL1
                label record after rewinding so subsequent reads start at the
                first user file.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            DriveOperationError: If the rewind command fails.
        """
        ...

    def get_VOL1(self) -> str:  # noqa: N802 - VOL1 is the tape-label format and existing API.
        """Read and return the 80-byte VOL1 label as UTF-8.

        Rewinds the tape internally before reading; on exit the tape is
        positioned just after the VOL1 record.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            DriveOperationError: If the rewind command fails.
        """
        ...

    def __enter__(self) -> "MountedDriveController": ...
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[object],
    ) -> Optional[bool]: ...

    def write_files(
        self,
        file_size: int,
        n: Optional[int],
        block_size: int = 256 * 1024,
    ) -> Iterator[tuple[int, int]]:
        """Write files of pseudo-random data to the tape.

        Each iteration writes one file of `file_size` bytes (a single
        `open()` per file produces one filemark on the tape) and yields
        `(file_index, checksum)`. The same byte pattern is reused across
        files, so all files share the same Adler-32 checksum.

        Args:
            file_size: Size of each file in bytes.
            n: Number of files to write. `None` means "until end-of-tape".
            block_size: Buffer size used for each `write()` syscall
                (default 256 KiB).

        Yields:
            `(file_index, checksum)` after each successful file. `file_index`
            is 1-based.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            EOTError: When the tape physically runs out of space. The
                exception carries the checksum and the count of files
                written successfully before EOT.
            OSError: For other I/O failures.
        """
        ...

    def read_files(
        self,
        n: int,
        block_size: int = 256 * 1024,
    ) -> Iterator[tuple[int, int]]:
        """Read files from the current tape position and checksum each one.

        Args:
            n: Number of files to read.
            block_size: Buffer size used for each `read()` syscall.

        Yields:
            `(file_index, checksum)` after each successful file.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            EODError: If end-of-data is reached before `n` files have been
                read.
        """
        ...

    def is_tape_write_protected(self) -> bool:
        """Return `True` if the loaded tape's write-protect flag is set.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            DriveOperationError: If the status query fails.
        """
        ...

    def set_block_size(self, value: int) -> None:
        """Configure the drive's fixed block size.

        Args:
            value: Block size in bytes. Use `0` to enable variable-length
                blocks.

        Raises:
            DriveStateError: If the drive has already been unmounted.
            DriveOperationError: If the configuration command fails.
        """
        ...
