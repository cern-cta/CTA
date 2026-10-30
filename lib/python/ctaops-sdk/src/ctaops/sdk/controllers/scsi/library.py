# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""SCSI-backed implementations of the library controller protocols.

See [ctaops.sdk.controllers.protocols.library][ctaops.sdk.controllers.protocols.library] for the public contract.
The classes in this module talk to the changer through `cta-smc` and to
`cta-tape-label` for label operations. The managed variant resolves drive
ordinals/names against a
[LocalLocatedNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedNamedDriveRepository]
and a
[LocalNamedLibraryRepository][ctaops.sdk.repositories.local.library.LocalNamedLibraryRepository].
"""

import json
import logging
import subprocess
from typing import Any, Optional, Union

from ctaops.sdk.controller_providers.scsi.drive import ScsiDriveControllerProvider
from ctaops.sdk.controllers.protocols.library import (
    DriveName,
    DriveOrdinal,
    LibraryController,
    ManagedLibraryController,
    TapeLocation,
)
from ctaops.sdk.controllers.scsi.drive import ScsiMountedDriveController
from ctaops.sdk.exceptions import DriveStateError, LibraryOperationError, ResourceNotFoundError, TapeLabelError
from ctaops.sdk.repositories.local.drive import LocalLocatedNamedDriveRepository
from ctaops.sdk.repositories.local.library import LocalNamedLibraryRepository
from ctaops.sdk.repositories.protocols.drive import LocatedNamedDrive
from pyudev import Context, Device
from typing_extensions import override

LOGGER = logging.getLogger("ctaops.sdk")


class ScsiLibraryController(LibraryController):
    """SCSI-backed [LibraryController][ctaops.sdk.controllers.protocols.library.LibraryController].

    Minimal contract: wraps a single `scsi_generic` medium-changer device
    and exposes ordinal-only changer operations through `cta-smc`. Drive
    addressing is by raw ordinal — no logical-name resolution and no
    repository lookups are involved.

    Equality and hashing are based on the SCSI serial number, so two
    controllers built from the same changer compare equal even if their
    pyudev `Device` objects differ.

    Note:
        For day-to-day operations prefer
        [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController],
        which adds logical-name resolution, returns a context-managed
        [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
        from `mount`, and exposes the `label_tape` operation.

    Args:
        sg_device: The medium-changer `scsi_generic` udev device
            (`SCSI_TYPE=medium changer`).
        context: Optional pyudev `Context`. When omitted, a fresh context is
            created.

    Raises:
        ValueError: If `sg_device` is not a medium-changer `scsi_generic`
            device.

    Example:
        ```python
        from ctaops.sdk import ScsiLibraryControllerProvider

        provider = ScsiLibraryControllerProvider()
        library = provider.get_library_controller_by_name("T0-LIB0")
        library.mount(drive_key=0, vid="X12345")
        try:
            ...
        finally:
            library.unmount(0)
        ```
    """

    def __init__(
        self,
        sg_device: Device,
        context: Optional[Context] = None,
    ):
        LOGGER.debug("Initializing ScsiLibraryController for device %s", sg_device)
        if sg_device.subsystem != "scsi_generic" or sg_device.properties.get("SCSI_TYPE") != "medium changer":
            raise ValueError(f"Device {sg_device} is not a valid tape library changer device")

        self._sg_device = sg_device
        self._context = context if context is not None else Context()

    @property
    @override
    def serial_number(self) -> str:
        """SCSI serial number from the `ID_SCSI_SERIAL` udev property.

        Raises:
            ValueError: If the device has no `ID_SCSI_SERIAL` property.
        """
        serial_number = self._sg_device.properties.get("ID_SCSI_SERIAL")
        if not serial_number:
            raise ValueError(f"Device {self._sg_device} does not have an ID_SCSI_SERIAL property")
        return serial_number

    @override
    def __repr__(self) -> str:
        return f"ScsiLibraryController(serial_number={self.serial_number})"

    @override
    def __hash__(self) -> int:
        return hash(self.serial_number)

    @override
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ScsiLibraryController):
            return False
        return self.serial_number == value.serial_number

    def _mount(self, ordinal: DriveOrdinal, vid: str) -> None:
        """Run the changer-side mount only. Asserts the drive is occupied afterwards.

        Args:
            ordinal: Drive ordinal as understood by the changer.
            vid: Volume identifier of the tape to mount.

        Raises:
            LibraryOperationError: If the changer command fails.
            DriveStateError: If the drive is still free after the mount —
                indicates the changer reported success but did not actually
                load the tape.
        """
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-smc", "-m", "-V", vid, "-D", str(ordinal)],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=60,
                check=True,
            )

            if self.drive_is_free(ordinal):
                raise DriveStateError(f"Drive {ordinal} is still free after mount operation: ERROR")
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Mount of tape {vid} in drive {ordinal} failed") from e

    def _unmount(self, ordinal: DriveOrdinal) -> None:
        """Run the changer-side unmount only. Asserts the drive is free afterwards.

        Args:
            ordinal: Drive ordinal as understood by the changer.

        Raises:
            LibraryOperationError: If the changer command fails.
            DriveStateError: If the drive is not free after the unmount —
                indicates the changer reported success but did not actually
                unload the tape.
        """
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-smc", "-d", "-D", str(ordinal)],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=60,
                check=True,
            )

            if not self.drive_is_free(ordinal):
                raise DriveStateError(f"Unmount of tape inside drive {ordinal}: ERROR")

        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"UnMount of drive {ordinal} failed") from e

    def _get_drive_vid(self, ordinal: DriveOrdinal) -> Optional[str]:
        """Return the VID of the tape currently inside the drive, or `None`.

        Args:
            ordinal: Drive ordinal as understood by the changer.

        Returns:
            The volume identifier reported by the changer, or `None` when
            the drive is empty.

        Raises:
            LibraryOperationError: If the changer query fails.
            ValueError: If the response cannot be parsed.
        """
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                [
                    "/usr/bin/cta-smc",
                    "--json",
                    "-q",
                    "D",
                    "-D",
                    str(ordinal),
                ],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=30,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Error during query state for drive {ordinal}") from e

        try:
            return result[0]["vid"] if (result := json.loads(cmd_call.stdout)) else None
        except (IndexError, KeyError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid response from cta-smc: {cmd_call.stdout}") from e

    @override
    def drive_is_free(self, drive_key: DriveOrdinal) -> bool:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.LibraryController.drive_is_free].

        Implementation: parses the `status` field of `cta-smc --json -q D <ordinal>`.
        """
        LOGGER.debug("Checking if drive %s is free", drive_key)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-smc", "--json", "-q", "D", "-D", str(drive_key)],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=30,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Error during query state for drive {drive_key}") from e

        try:
            return json.loads(cmd_call.stdout)[0]["status"] == "free"
        except (IndexError, KeyError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid response from cta-smc: {cmd_call.stdout}") from e

    @override
    def import_tapes(self) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.LibraryController.import_tapes].

        Implementation: runs `cta-smc -i`.
        """
        LOGGER.info("Importing tapes into library %s", self.serial_number)
        try:
            subprocess.run(
                ["/usr/bin/cta-smc", "-i"],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=120,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError("Failed to import tapes inside library slots") from e

    @override
    def export_tape(self, vid: str) -> None:
        """See [LibraryController.export_tape][ctaops.sdk.controllers.protocols.library.LibraryController.export_tape].

        Implementation: runs `cta-smc --json -e -V <vid>`.
        """
        LOGGER.info("Exporting tape vid=%s from library %s", vid, self.serial_number)
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-smc", "--json", "-e", "-V", vid],
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=120,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Failed to export tape {vid}") from e

    @override
    def get_tape_location(self, vid: str) -> TapeLocation:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.LibraryController.get_tape_location].

        Implementation: parses `cta-smc --json -q V -V <vid>` and maps the
        `elementType` field onto the
        [TapeLocation][ctaops.sdk.controllers.protocols.library.TapeLocation]
        location enum.

        Raises:
            ResourceNotFoundError: If the changer reports no such tape.
            LibraryOperationError: If the query fails.
            ValueError: If the response cannot be parsed.
        """
        LOGGER.debug("Getting tape location for vid=%s in library %s", vid, self.serial_number)
        try:
            cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-smc", "--json", "-q", "V", "-V", vid],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=30,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Error during query state for tape {vid}") from e

        try:
            if not (result := json.loads(cmd_call.stdout)):
                raise ResourceNotFoundError(f"Tape {vid} not found in library {self.serial_number}")
            tape_info: dict[str, str] = result[0]
            if tape_info.get("elementType") == "drive":
                location = "DRIVE"
            elif tape_info.get("elementType") == "slot":
                location = "HOME"
            else:
                location = "UNKNOWN"

            return TapeLocation(
                vid,
                location,
                int(tape_info.get("elementAddress", 0)),
                self.serial_number,
            )
        except (IndexError, KeyError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid response from cta-smc: {cmd_call.stdout}") from e

    @override
    def get_max_io_slots(self) -> int:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.LibraryController.get_max_io_slots].

        Implementation: parses `cta-smc --json -q L` and reads the
        `port.count` field, raising
        [LibraryOperationError][ctaops.sdk.exceptions.LibraryOperationError]
        if the payload shape does not match.
        """
        LOGGER.debug("Getting max I/O slots for library %s", self.serial_number)
        try:
            cmd_call = subprocess.run(
                ["/usr/bin/cta-smc", "--json", "-q", "L"],
                stdout=subprocess.PIPE,
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                timeout=30,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise LibraryOperationError(f"Error during query state for lib {self}") from e

        try:
            parsed = json.loads(cmd_call.stdout)
        except json.JSONDecodeError as e:
            raise LibraryOperationError(f"cta-smc -q L returned non-JSON output: {cmd_call.stdout!r}") from e

        if not isinstance(parsed, list) or not parsed:
            raise LibraryOperationError(f"cta-smc -q L returned an empty or non-list payload: {cmd_call.stdout!r}")

        port_section = parsed[0].get("port") if isinstance(parsed[0], dict) else None
        if not isinstance(port_section, dict) or "count" not in port_section:
            raise LibraryOperationError(f"cta-smc -q L payload missing port.count: {cmd_call.stdout!r}")

        try:
            return int(port_section["count"])
        except (TypeError, ValueError) as e:
            raise LibraryOperationError(
                f"cta-smc -q L returned non-integer port.count {port_section['count']!r}"
            ) from e

    @override
    def mount(self, drive_key: DriveOrdinal, vid: str) -> Any:
        """See [LibraryController.mount][ctaops.sdk.controllers.protocols.library.LibraryController.mount].

        Implementation: if the drive already holds `vid`, the existing
        mount is reused (no changer command is run); otherwise the drive
        must be free, `cta-smc -m` is invoked.

        Warning:
            This method does not return a drive controller and does not
            install an unmount callback — releasing the tape is the
            caller's responsibility. Always prefer
            [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController],
            which wires up a context-managed
            [MountedDriveController][ctaops.sdk.controllers.protocols.drive.MountedDriveController]
            for you.
        """
        if not self.drive_is_free(drive_key) and self._get_drive_vid(drive_key) == vid:
            LOGGER.info(
                "Reusing existing mount of vid=%s on drive=%s (library=%s)",
                vid,
                drive_key,
                self.serial_number,
            )
            return

        LOGGER.info("Mounting tape vid=%s on drive=%s (library=%s)", vid, drive_key, self.serial_number)
        if not self.drive_is_free(drive_key):
            raise DriveStateError(f"Drive {drive_key} is not free, a tape is inside, no mount possible")

        self._mount(drive_key, vid)

    @override
    def unmount(self, drive_key: DriveOrdinal) -> None:
        """See [LibraryController.unmount][ctaops.sdk.controllers.protocols.library.LibraryController.unmount].

        Implementation: no-op when the drive is already free; otherwise runs
        `cta-smc -d` to return the tape to its home slot.

        Warning:
            You need to unload the tape from the drive before unmounting,
            otherwise the changer will reject the unmount command. Always
            prefer
            [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController]
            which implements this logic for you.
        """
        if self.drive_is_free(drive_key):
            return

        LOGGER.info("Unmounting drive=%s on library=%s", drive_key, self.serial_number)
        self._unmount(drive_key)


class ScsiManagedLibraryController(ScsiLibraryController, ManagedLibraryController):
    """SCSI-backed [ManagedLibraryController][ctaops.sdk.controllers.protocols.library.ManagedLibraryController].

    Wraps a single `scsi_generic` medium-changer device. All hardware
    interaction happens through `cta-smc`. Drive ordinal/name resolution
    is delegated to a
    [LocalLocatedNamedDriveRepository][ctaops.sdk.repositories.local.drive.LocalLocatedNamedDriveRepository]
    constructed internally; logical library name resolution goes through a
    [LocalNamedLibraryRepository][ctaops.sdk.repositories.local.library.LocalNamedLibraryRepository].
    Both repos share the controller's pyudev `Context` so a single udev
    snapshot drives the whole controller graph.

    Equality and hashing are based on the SCSI serial number, so two
    controllers built from the same changer compare equal even if their
    pyudev `Device` objects differ.

    Args:
        sg_device: The medium-changer `scsi_generic` udev device
            (`SCSI_TYPE=medium changer`).
        context: Optional pyudev `Context`. When omitted, a fresh context is
            created and reused by the controller and the local repositories
            it spawns internally.

    Raises:
        ValueError: If `sg_device` is not a medium-changer `scsi_generic`
            device.

    Example:
        ```python
        from ctaops.sdk import ScsiManagedLibraryControllerProvider

        library = ScsiManagedLibraryControllerProvider().get_library_controller_by_name("T0-LIB0")
        with library.mount("T0-LIB0-DRV0", "X12345") as drive:
            drive.set_block_size(256 * 1024)
            for index, checksum in drive.write_files(file_size=1 << 30, n=5):
                print(index, checksum)
        ```
    """

    def __init__(
        self,
        sg_device: Device,
        context: Optional[Context] = None,
    ):
        super().__init__(sg_device, context)
        LOGGER.debug("Initializing ScsiManagedLibraryController for device %s", sg_device)
        self._drive_controller_provider = ScsiDriveControllerProvider(self._context)
        self._drive_repo = LocalLocatedNamedDriveRepository(self._context)

    @property
    @override
    def name(self) -> str:
        """Logical library name resolved through the infrastructure repository."""
        return LocalNamedLibraryRepository(self._context).get_library(self.serial_number).name

    def _get_drive_metadata(self, drive_key: object) -> LocatedNamedDrive:
        """
        Resolve a drive key into library metadata.

        Return [LocatedNamedDrive][ctaops.sdk.repositories.protocols.drive.LocatedNamedDrive] metadata.

        Args:
            drive_key: Drive ordinal (`int`) or logical name (`str`).

        Raises:
            TypeError: If `drive_key` is neither `int` nor `str`.
            ResourceNotFoundError: If no matching drive is bound to this
                library.
        """
        if isinstance(drive_key, int):
            return self._drive_repo.get_drive_by_ordinal(drive_key, self.serial_number)
        elif isinstance(drive_key, str):
            return self._drive_repo.get_drive_by_name(drive_key, self.serial_number)
        else:
            raise TypeError(f"drive_key is of type {type(drive_key)} but should be one of following types (int,str)")

    @override
    def label_tape(
        self,
        drive_key: Union[DriveOrdinal, DriveName],
        vid: str,
        old_vid: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.ManagedLibraryController.label_tape].

        Implementation: invokes `cta-tape-label` against the resolved
        drive name, optionally with `--oldlabel` and/or `--force`.
        """
        LOGGER.info(
            "Labelling tape vid=%s on drive=%s (old_vid=%s, force=%s)",
            vid,
            drive_key,
            old_vid,
            force,
        )
        try:
            subprocess.run(  # noqa: S603 - fixed tool, argv only.
                ["/usr/bin/cta-tape-label", "--drive", self._get_drive_metadata(drive_key).name, "--vid", vid]
                + (["--oldlabel", old_vid] if old_vid else [])
                + (["--force"] if force else [])
                + (["--debug"] if LOGGER.level == logging.DEBUG else []),
                stdout=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                stderr=(None if LOGGER.level == logging.DEBUG else subprocess.DEVNULL),
                text=True,
                check=True,
            )
        except subprocess.SubprocessError as e:
            raise TapeLabelError(f"Failed to label tape {vid}") from e

    @override
    def drive_is_free(self, drive_key: Union[DriveOrdinal, DriveName]) -> bool:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.ManagedLibraryController.drive_is_free].

        Implementation: parses the `status` field of `cta-smc --json -q D <ordinal>`.
        """
        return super().drive_is_free(self._get_drive_metadata(drive_key).location.ordinal)

    @override
    def mount(self, drive_key: Union[DriveOrdinal, DriveName], vid: str) -> ScsiMountedDriveController:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.ManagedLibraryController.mount].

        Implementation: if the drive already holds `vid` and is online, the
        existing mount is reused (no changer command is run); otherwise the
        drive must be free, `cta-smc -m` is invoked, and a fresh
        [ScsiMountedDriveController][ctaops.sdk.controllers.scsi.drive.ScsiMountedDriveController]
        is wired with an unmount callback that calls
        [unmount][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController.unmount]
        against this library.
        """
        drive = self._get_drive_metadata(drive_key)

        drive_controller = self._drive_controller_provider.get_drive_controller(drive.serial_number)

        if (
            not super().drive_is_free(drive.location.ordinal)
            and self._get_drive_vid(drive.location.ordinal) == vid
            and drive_controller.has_loaded_tape()
        ):
            LOGGER.info(
                "Reusing existing mount of vid=%s on drive=%s (library=%s)",
                vid,
                drive_key,
                self.serial_number,
            )
            return ScsiMountedDriveController.from_drive_controller(
                drive_controller, vid, (lambda: self.unmount(drive_key))
            )

        if not self.drive_is_free(drive_key):
            raise DriveStateError(
                f"Drive {drive_key} is not free, a different tape is inside "
                "or a tape is in ejected state, no mount possible"
            )

        LOGGER.info("Mounting tape vid=%s on drive=%s (library=%s)", vid, drive_key, self.serial_number)
        self._mount(drive.location.ordinal, vid)
        return ScsiMountedDriveController.from_drive_controller(
            drive_controller, vid, (lambda: self.unmount(drive_key))
        )

    @override
    def unmount(self, drive_key: Union[DriveOrdinal, DriveName]) -> None:
        """
        See the base implementation.

        [Reference][ctaops.sdk.controllers.protocols.library.ManagedLibraryController.unmount].

        Implementation: no-op when the drive is already free; otherwise
        ejects the tape via the drive controller's
        [unload][ctaops.sdk.controllers.protocols.drive.DriveController.unload]
        and then runs `cta-smc -d` to return it to the home slot.
        """
        drive = self._get_drive_metadata(drive_key)

        if self.drive_is_free(drive_key):
            return

        LOGGER.info("Unmounting drive=%s on library=%s", drive_key, self.serial_number)
        drive_controller = self._drive_controller_provider.get_drive_controller(drive.serial_number)
        drive_controller.unload()
        self._unmount(drive.location.ordinal)

    @classmethod
    def _from_library_controller(
        cls,
        lib_ctrl: ScsiLibraryController,
    ) -> "ScsiManagedLibraryController":
        """
        Upgrade a [ScsiLibraryController][ctaops.sdk.controllers.scsi.library.ScsiLibraryController] into a managed one.

        Reuses the wrapped `sg_device` and pyudev `Context` so the resulting
        controller shares the udev snapshot of the source. Useful when a
        caller obtained a minimal controller (for example from a generic
        provider) and now needs the richer ctaops-aware operations.

        Args:
            lib_ctrl: Source controller to wrap.

        Returns:
            A new
            [ScsiManagedLibraryController][ctaops.sdk.controllers.scsi.library.ScsiManagedLibraryController]
            wrapping the same device.
        """
        return cls(
            sg_device=lib_ctrl._sg_device,
            context=lib_ctrl._context,
        )

    @override
    def __repr__(self) -> str:
        return f"ScsiManagedLibraryController(serial_number={self.serial_number}, name={self.name})"
