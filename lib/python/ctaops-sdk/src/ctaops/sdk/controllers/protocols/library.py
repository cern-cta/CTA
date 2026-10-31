# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Protocols describing how callers drive a tape library (media changer).

Two layers are exposed:

* [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController]
  — minimal contract sufficient to operate a changer addressed by drive
  ordinals only.
* [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController]
  — extends `LibraryController` with ctaops-aware operations: drives can
  be addressed by either ordinal or logical name, `mount` returns a
  [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
  ready for I/O, and CTA-specific operations such as `label_tape` are
  exposed.

Concrete SCSI-backed implementations:
[ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController]
and
[ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController].
"""

from dataclasses import dataclass
from typing import Literal, Optional, Protocol, Union

from ctaops.sdk.controllers.protocols.drive import MountedDriveController
from typing_extensions import override


@dataclass(frozen=True)
class TapeLocation:
    """Where a specific tape currently lives within a library.

    Attributes:
        vid: Volume identifier (barcode) of the tape.
        location: One of `"DRIVE"` (currently mounted in a drive),
            `"HOME"` (resting in a storage slot), or `"UNKNOWN"` (the
            changer reports a location but it does not map to one of the
            above categories).
        address: SCSI element address of the slot or drive holding the tape.
        library_serial_number: Serial number of the library that returned
            this location.
    """

    vid: str
    location: Literal["DRIVE", "HOME", "UNKNOWN"]
    address: int
    library_serial_number: str


DriveOrdinal = int
"""Drive ordinal as understood by changer tools (`cta-smc`, `mtx`, …)."""

DriveName = str
"""Logical drive name, configured externally by the operator (e.g. via udev)."""


class LibraryController(Protocol):
    """Minimal media-changer contract: address drives by ordinal.

    Sufficient for code that already knows the ordinal layout and does not
    need name resolution. For day-to-day operations prefer
    [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController].
    """

    @property
    def serial_number(self) -> str:
        """Serial number of the changer. (usually SCSI serial number)."""
        ...

    def mount(self, drive_key: DriveOrdinal, vid: str) -> object:
        """Move the tape `vid` from its home slot into drive `drive_key`.

        Args:
            drive_key: Drive ordinal as understood by the changer.
            vid: Volume identifier of the tape to mount.

        Raises:
            LibraryOperationError: If the changer command fails.
            DriveStateError: If the drive is not free after the mount
                operation.
        """
        ...

    def unmount(self, drive_key: DriveOrdinal) -> None:
        """Move the tape currently in drive `drive_key` back to its slot.

        No-op if the drive is already free.

        Raises:
            LibraryOperationError: If the changer command fails.
        """
        ...

    def drive_is_free(self, drive_key: DriveOrdinal) -> bool:
        """Return `True` if drive `drive_key` has no tape mounted.

        Raises:
            LibraryOperationError: If the status query fails.
        """
        ...

    def export_tape(self, vid: str) -> None:
        """Move tape `vid` to the import/export mailslot for physical removal.

        Raises:
            LibraryOperationError: If the changer command fails.
        """
        ...

    def import_tapes(self) -> None:
        """Pull every tape currently in the import/export mailslot into storage.

        Raises:
            LibraryOperationError: If the changer command fails.
        """
        ...

    def get_tape_location(self, vid: str) -> TapeLocation:
        """Return the current [TapeLocation][ctaops.sdk.controllers.protocols.library.TapeLocation] of tape `vid`.

        Raises:
            ResourceNotFoundError: If the changer reports no such tape.
            LibraryOperationError: If the location query fails.
        """
        ...

    def get_max_io_slots(self) -> int:
        """Return the number of I/O (mailslot) slots the library exposes.

        Raises:
            LibraryOperationError: If the query fails or the response is
                malformed.
        """
        ...


class ManagedLibraryController(LibraryController, Protocol):
    """ctaops-aware media-changer contract.

    Layered on top of
    [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController]
    to add:

    * a logical `name` resolved from the changer's ctaops udev metadata,
    * acceptance of either drive ordinal *or* drive name (`CTAOPS_NAME`)
      everywhere a `drive_key` is taken,
    * a richer `mount` that returns a
      [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
      context manager wired to the matching drive controller,
    * the `label_tape` operation backed by `cta-tape-label`.

    Example:
        ```python
        from ctaops.sdk import ScsiManagedLibraryControllerProvider

        provider = ScsiManagedLibraryControllerProvider()
        library = provider.get_library_controller_by_name("library-0")
        with library.mount("drive-0", "X12345") as drive:
            for index, checksum in drive.write_files(file_size=1 << 30, n=5):
                ...
        ```
    """

    @property
    def name(self) -> str:
        """Logical library name (`CTAOPS_NAME` udev property)."""
        ...

    @override
    def mount(self, drive_key: Union[DriveOrdinal, DriveName], vid: str) -> MountedDriveController:
        """Mount tape `vid` in `drive_key` and return an I/O context manager.

        If the drive already holds the requested tape and is online, the
        existing mount is reused (no physical mount is performed) and a
        new context manager wrapping it is returned.

        Args:
            drive_key: Either the ordinal (int) or logical name (str) of the
                drive.
            vid: Volume identifier of the tape to mount.

        Returns:
            A [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
            ready to be used as a context manager.

        Raises:
            ResourceNotFoundError: If the drive is not visible locally.
            DriveStateError: If the drive holds a different tape and is not
                free.
            LibraryOperationError: If the changer mount command fails.
        """
        ...

    @override
    def unmount(self, drive_key: Union[DriveOrdinal, DriveName]) -> None:
        """Eject the tape in `drive_key` and return it to its home slot.

        No-op if the drive is already free.

        Args:
            drive_key: Either the ordinal (int) or logical name (str) of the
                drive.

        Raises:
            LibraryOperationError: If the changer command fails.
            DriveOperationError: If ejecting the tape from the drive fails.
        """
        ...

    @override
    def drive_is_free(self, drive_key: Union[DriveOrdinal, DriveName]) -> bool:
        """Return `True` if the named/ordinal-addressed drive is free.

        Args:
            drive_key: Either the ordinal (int) or logical name (str) of the
                drive.

        Raises:
            LibraryOperationError: If the status query fails.
        """
        ...

    def label_tape(
        self,
        drive_key: Union[DriveOrdinal, DriveName],
        vid: str,
        old_vid: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """Write a CTA tape label onto the tape currently in `drive_key`.

        Args:
            drive_key: Either the ordinal (int) or logical name (str) of the
                drive holding the tape.
            vid: New volume identifier to write.
            old_vid: If given, the tape must already carry this label or the
                operation aborts (used as a guard against mislabelling).
            force: Overwrite any existing label without consistency checks.

        Raises:
            TapeLabelError: If `cta-tape-label` reports failure.
        """
        ...
