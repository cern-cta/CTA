# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Commands for managing CTA tapes.

This module provides comprehensive tape management commands including:
- create: Add new tapes to CTA
- update: Modify tape properties
- list: Query tapes with various filters
- get: Retrieve detailed information about a specific tape
- delete: Remove tapes from CTA
- reclaim: Reclaim tape space
- refresh_stats: Update tape statistics
- mount/unmount: Physical tape library operations (with tapeserver support)
- locate: Find tape location in library
- check: Perform tape integrity checks
- label: Label tapes with new VIDs
- import/export: Import/export tapes from/to library

Commands are aliased for convenience (e.g., "tape" -> "ta").
"""

import json
import subprocess
from datetime import timedelta
from logging import getLogger
from pathlib import Path
from time import perf_counter
from typing import Optional

import click
from cta.sdk.exceptions import ResourceNotFoundError
from cta.sdk.models import DriveOperatingState, Tape, TapeState, VerificationStatus
from cta.sdk.units import Byte, BytePerSecond
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import TapeStateType, enum_choice
from ctaops.cli.context import EXTRA_TPSRV, AppContext
from ctaops.cli.display.encoders import encode_table_field
from ctaops.cli.display.printers import print_dataclasses
from rich import print

LOGGER = getLogger("ctaops.cli")


@click.group(cls=ClickAliasedGroup, aliases=["ta"])
def tape():
    """Manage CTA Tapes."""


@tape.command(aliases=["add"])
@click.argument("vid")
@click.option("--media-type", "--mt", required=True)
@click.option("--vendor", "-v", required=True)
@click.option("--logical-library", "--ll", required=True)
@click.option("--tape-pool", "--tp", required=True)
@click.option("--comment", "-c", required=True)
@click.option("--full", "-f", is_flag=True, default=False)
@click.pass_obj
def create(
    obj: AppContext,
    vid: str,
    media_type: str,
    vendor: str,
    logical_library: str,
    tape_pool: str,
    full: bool,
    comment: str,
):
    """Create a new tape and add it to CTA."""
    obj.cta_client.tape.create(
        vid,
        media_type_name=media_type,
        vendor=vendor,
        logical_library_name=logical_library,
        tape_pool_name=tape_pool,
        comment=comment,
        full=full,
    )
    print(f"Tape({vid}) CREATED")


@tape.command(aliases=["ch"])
@click.argument("vid")
@click.option("--media-type", "--mt")
@click.option("--vendor", "-v")
@click.option("--logical-library", "--ll")
@click.option("--tape-pool", "--tp")
@click.option("--encryption-key")
@click.option("--full/--not-full", "-f/", default=None)
@click.option("--verification-status", "--vs")
@click.option(
    "--state",
    "-s",
    type=TapeStateType(),
    metavar="<STATE>(:<REASON>)",
    help=(
        'Choose state between "ACTIVE", "DISABLED", "BROKEN", '
        '"EXPORTED", "REPACKING", "REPACK_DISABLED". '
        "A reason is required except for ACTIVE."
    ),
)
@click.option("--purchase-order", "-p")
@click.option("--dirty/--clean", "-d/-c", default=None)
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    vid: str,
    media_type: Optional[str],
    vendor: Optional[str],
    logical_library: Optional[str],
    tape_pool: Optional[str],
    full: Optional[bool],
    verification_status: Optional[str],
    state: Optional[tuple[TapeState, str]],
    purchase_order: Optional[str],
    dirty: Optional[bool],
    comment: Optional[str],
    encryption_key: Optional[str],
):
    """Update tape properties or state.

    Modify one or more tape properties such as media type, vendor, pool, or state.
    Use --state to transition tape to a new state with a reason.
    """
    if verification_status is not None:
        try:
            verification_status_instance = VerificationStatus.from_dict(json.loads(verification_status))
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise click.BadParameter("Invalid verification status format") from e
    else:
        verification_status_instance = None

    if state is not None:
        obj.cta_client.tape.change_state(vid, state=state[0], reason=state[1])
    else:
        obj.cta_client.tape.update(
            vid,
            media_type_name=media_type,
            vendor=vendor,
            logical_library_name=logical_library,
            tape_pool_name=tape_pool,
            verification_status=verification_status_instance,
            full=full,
            purchase_order=purchase_order,
            comment=comment,
            dirty=dirty,
            encryption_key_name=encryption_key,
        )
    print(f"Tape({vid}) UPDATED")


@tape.command("list", aliases=["ls"])
@click.option("--all", "-a", is_flag=True)
@click.option("--media-type", "--mt")
@click.option("--vendor", "-v")
@click.option("--logical-library", "--ll")
@click.option("--tape-pool", "--tp")
@click.option("--virtual-organisation", "--vo")
@click.option("--full/--not-full", "-f/--nf", default=None)
@click.option(
    "--state",
    "-s",
    type=click.Choice([f.name for f in TapeState], case_sensitive=False),
    callback=enum_choice(TapeState),
)
@click.option("--purchase-order", "-p")
@click.option("--physical-library", "--pl")
@click.option("--missing-file-copies", "--mfc", is_flag=True)
@click.pass_obj
def list_command(
    obj: AppContext,
    all: bool,
    media_type: Optional[str],
    vendor: Optional[str],
    logical_library: Optional[str],
    tape_pool: Optional[str],
    virtual_organisation: Optional[str],
    full: Optional[bool],
    state: Optional[TapeState],
    purchase_order: Optional[str],
    physical_library: Optional[str],
    missing_file_copies: bool,
):
    """List CTA tapes in CTA."""
    if not all and not any(
        [
            media_type,
            vendor,
            logical_library,
            tape_pool,
            virtual_organisation,
            full is not None,
            state,
            purchase_order,
            physical_library,
            missing_file_copies,
        ]
    ):
        raise click.UsageError("You must provide at least one filter option or use --all.")

    data = obj.cta_client.tape.list(
        media_type_name=media_type,
        vendor=vendor,
        logical_library_name=logical_library,
        tape_pool_name=tape_pool,
        virtual_organisation_name=virtual_organisation,
        full=full,
        state=state,
        purchase_order=purchase_order,
        physical_library_name=physical_library,
        missing_file_copies=missing_file_copies,
    )

    print_dataclasses(data, display_config=obj.config.display)


@tape.command()
@click.argument("vid")
@click.pass_obj
def get(obj: AppContext, vid: str):
    """Get tape registered in CTA."""
    print_dataclasses(obj.cta_client.tape.get(vid), display_config=obj.config.display)


@tape.command(aliases=["rm"])
@click.argument("vid")
@click.pass_obj
def delete(obj: AppContext, vid: str):
    """Remove a tape from CTA."""
    obj.cta_client.tape.delete(vid)
    print(f"Tape({vid}) DELETED")


@tape.command()
@click.argument("vid")
@click.pass_obj
def reclaim(obj: AppContext, vid: str):
    """Reclaim a tape from CTA."""
    obj.cta_client.tape.reclaim(vid)
    print(f"Tape ({vid}) RECLAIMED")


@tape.command()
@click.pass_obj
def refresh_stats(obj: AppContext):
    """Refresh tape statistics and print it."""
    cmd_call = subprocess.run(  # noqa: S603 - fixed tool, argv only.
        [
            "/usr/bin/cta-statistics-update",
            obj.config.cta_catalogue_conn_file,
        ],  # On CTA side we need to move this functionality to the frontend as an admin command
        check=False,
        encoding="utf-8",
        timeout=10,
    )
    if cmd_call.returncode != 0:
        raise click.ClickException(
            f"Error in subprocess during /usr/bin/cta-statistics-update execution: {cmd_call.stderr}"
        )


if EXTRA_TPSRV:
    from dataclasses import asdict

    from ctaops.sdk import (
        EOTError,
        LocalLibraryRepository,
        LocalLocatedDriveRepository,
        ManagedLibraryController,
        ResourceNotFoundError,
        ScsiManagedLibraryControllerProvider,
    )

    @tape.command(aliases=["m"])
    @click.argument("vid")
    @click.option("--drive", "-d", required=True)
    @click.option("--read-vol1", is_flag=True)
    @click.pass_obj
    def mount(obj: AppContext, vid: str, drive: str, read_vol1: bool):
        try:
            obj.cta_client.tape.get(vid)
            mounted_drive = (
                ScsiManagedLibraryControllerProvider(**asdict(obj.config.tape_server.udev_properties_map))
                .get_library_controller_from_drive_name(drive)
                .mount(drive, vid)
            )
        except StopIteration as e:
            raise click.ClickException(str(e)) from e

        if read_vol1:
            print(f"Read vol1: {mounted_drive.get_VOL1()!r}")

    @tape.command(aliases=["u"])
    @click.argument("vid")
    @click.pass_obj
    def unmount(obj: AppContext, vid: str):
        try:
            tape = obj.cta_client.tape.get(vid)
            lib_ctrl = ScsiManagedLibraryControllerProvider(
                **asdict(obj.config.tape_server.udev_properties_map)
            ).get_library_controller_by_name(tape.physical_library_name)
        except StopIteration as e:
            raise click.ClickException(str(e)) from e

        tape_location = lib_ctrl.get_tape_location(vid)
        if tape_location.location == "HOME":
            LOGGER.info("Tape %s is already unmounted", vid)
        elif tape_location.location == "DRIVE":
            LOGGER.info("Searching for drive with tape %s mounted at address %s", vid, tape_location.address)
            try:
                local_drive = next(
                    (
                        d
                        for d in LocalLocatedDriveRepository(
                            drive_library_address_key=obj.config.tape_server.udev_properties_map.drive_library_address_key,
                            drive_library_ordinal_key=obj.config.tape_server.udev_properties_map.drive_library_ordinal_key,
                        ).list_drives(lib_ctrl.serial_number)
                        if d.location.address == tape_location.address
                    ),
                )
            except (ResourceNotFoundError, StopIteration) as e:
                raise click.ClickException(f"Drive with address {tape_location.address} not found locally") from e
            lib_ctrl.unmount(local_drive.location.ordinal)

        else:
            raise click.ClickException(f"Tape {vid} not in a good location: {tape_location.location}")

    @tape.command(aliases=["lo"])
    @click.argument("vid")
    @click.pass_obj
    def locate(obj: AppContext, vid: str):
        tape = obj.cta_client.tape.get(vid)
        try:
            tape_location = (
                ScsiManagedLibraryControllerProvider(**asdict(obj.config.tape_server.udev_properties_map))
                .get_library_controller_by_name(tape.physical_library_name)
                .get_tape_location(vid)
            )
        except ResourceNotFoundError as e:
            raise click.ClickException(str(e)) from e
        print_dataclasses(tape_location, display_config=obj.config.display)

    @tape.command(aliases=["c"])
    @click.argument("vid")
    @click.option("--drive", "-d", required=True)
    @click.option("--file-size", "-s", type=click.IntRange(1000**3), default=10 * 1024**3, show_default=True)
    @click.pass_obj
    def check(obj: AppContext, vid: str, drive: str, file_size: int):
        def check_drive_input(drive: str) -> ManagedLibraryController:
            cta_drive = obj.cta_client.drive.get(drive)

            if cta_drive.state is not DriveOperatingState.DOWN or not cta_drive.state_reason:
                raise click.ClickException(f"Drive {cta_drive.name} must be DOWN with a reason")

            try:
                library_controller = ScsiManagedLibraryControllerProvider(
                    **asdict(obj.config.tape_server.udev_properties_map)
                ).get_library_controller_from_drive_name(drive)
            except ResourceNotFoundError as e:
                raise click.ClickException(str(e)) from e

            return library_controller

        def check_tape_input(vid: str, library_controller: ManagedLibraryController) -> Tape:
            tape = obj.cta_client.tape.get(vid)

            if not tape.tape_pool_name.startswith(("test_", "tolabel_", "erase_")):
                raise click.ClickException(
                    "Tape must belongs to a tapepool with one of the following prefix: 'test_','tolabel_' ,'erase_'"
                )

            if not tape.is_empty:
                raise click.ClickException("Tape is not empty, there are CTA files on this tape ! Aborting...")

            if library_controller.name != tape.physical_library_name:
                raise click.ClickException("Drive and lib not in the same logical library")

            return tape

        library_controller = check_drive_input(drive)
        tape = check_tape_input(vid, library_controller)

        with library_controller.mount(drive, vid) as md:
            if vid not in (vol1 := md.get_VOL1()):
                raise click.ClickException(f"Expected {vid=} has a wrong label: {vol1=!r}")

            md.rewind()

            LOGGER.info("Start writing files of size %s on %s", encode_table_field(file_size, Byte), tape)
            t0 = perf_counter()
            w_checksum = 1
            with click.progressbar(length=tape.capacity // file_size, label="Writing") as bar:
                file_index = 0
                try:
                    for file_index, c_checksum in md.write_files(file_size, None):
                        bar.update(1, file_index)
                        w_checksum = c_checksum
                except EOTError as e:
                    LOGGER.warning(
                        "End of tape reached after writing %s files of size %s",
                        file_index,
                        encode_table_field(file_size, Byte),
                    )
                    file_count = e.written_files
                else:
                    file_count = file_index

            w_time = timedelta(seconds=perf_counter() - t0)
            LOGGER.info(
                "%s files of size %s witten on tape %s in %s",
                file_count,
                encode_table_field(file_size, Byte),
                tape,
                encode_table_field(w_time),
            )
            LOGGER.info(
                "Average speed: %s", encode_table_field(file_count * file_size / w_time.total_seconds(), BytePerSecond)
            )

            md.rewind()

            LOGGER.info(
                "Start reading %s files of size %s on tape %s", file_count, encode_table_field(file_size, Byte), tape
            )
            t0 = perf_counter()
            with click.progressbar(length=file_count, label="Reading") as bar:
                for file_index, r_checksum in md.read_files(file_count):
                    bar.update(1, file_index)
                    if r_checksum != w_checksum:
                        raise click.ClickException(
                            f"Checksum mistmatch on file index {file_index}. "
                            f"Read checksum : {r_checksum} (expected {w_checksum})"
                        )
            r_time = timedelta(seconds=perf_counter() - t0)

            LOGGER.info(
                "%s files of size %s read on tape %s in %s",
                file_count,
                encode_table_field(file_size, Byte),
                tape,
                encode_table_field(r_time),
            )
            LOGGER.info(
                "Average speed: %s", encode_table_field(file_count * file_size / r_time.total_seconds(), BytePerSecond)
            )

    def _label_library_for_drive(obj: AppContext, drive: str) -> ManagedLibraryController:
        cta_drive = obj.cta_client.drive.get(drive)

        if cta_drive.state is not DriveOperatingState.DOWN or not cta_drive.state_reason:
            raise click.ClickException(f"Drive {cta_drive.name} must be DOWN with a reason")

        try:
            library_controller = ScsiManagedLibraryControllerProvider(
                **asdict(obj.config.tape_server.udev_properties_map)
            ).get_library_controller_from_drive_name(drive)
        except ResourceNotFoundError as e:
            raise click.ClickException(str(e)) from e

        return library_controller

    def _move_labelled_tape(obj: AppContext, vid: str, dstpool: str) -> None:
        LOGGER.info("Moving tape %s to tapepool %s", vid, dstpool)
        try:
            obj.cta_client.tape_pool.get(dstpool)
            obj.cta_client.tape.update(vid, tape_pool_name=dstpool)
        except ResourceNotFoundError:
            LOGGER.exception("Tape pool %s does not exist", dstpool)
        else:
            LOGGER.info("Successfully moved tape %s to tapepool %s", vid, dstpool)
            return

        LOGGER.error("Failed to moved tape %s to %s tapeppol", vid, dstpool)

    @tape.command(aliases=["la"])
    @click.argument("vid", required=False)
    @click.option("--file", type=click.Path(exists=True, readable=True, path_type=Path))
    @click.option("--srcpool", "--sp")
    @click.option("--limit", "-l", type=click.IntRange(min=1))
    @click.option("--drive", "-d", required=True)
    @click.option("--old-label", "-o")
    @click.option("--force", "-f", is_flag=True)
    @click.option("--dstpool", "--dp")
    @click.pass_obj
    def label(
        obj: AppContext,
        vid: Optional[str],
        file: Optional[Path],
        srcpool: Optional[str],
        limit: Optional[int],
        drive: str,
        old_label: Optional[str],
        force: bool,
        dstpool: Optional[str],
    ):
        if bool(vid) + bool(srcpool) + bool(file) != 1:
            raise click.UsageError("Please choose either vid arg, a srcpool, or a file")

        def treat_single_vid(library_controller: ManagedLibraryController, vid: str):
            try:
                tape = obj.cta_client.tape.get(vid)
            except ResourceNotFoundError as e:
                raise click.ClickException(str(e)) from e
            else:
                if library_controller.name != tape.physical_library_name:
                    raise click.ClickException(f"Tape {vid} is not in the same logical library than drive {drive}")

            library_controller.label_tape(drive, vid, old_vid=old_label, force=force)
            if dstpool:
                _move_labelled_tape(obj, vid, dstpool)

        def treat_srcpool(library_controller: ManagedLibraryController, srcpool: str, limit: Optional[int]):
            LOGGER.info("Getting tape list from tapepool %s", srcpool)
            obj.cta_client.tape_pool.get(srcpool)
            tapes = obj.cta_client.tape.list(tape_pool_name=srcpool)
            for index, t in enumerate(tapes):
                LOGGER.info("Labeling tape %s from tapepool %s on drive %s", t.vid, srcpool, drive)
                if limit and index > limit:
                    LOGGER.info("Already %s treated tapes, exist the program according to limit argument", limit)
                    return
                if library_controller.name != t.physical_library_name:
                    LOGGER.warning(
                        "Tape %s is not in the same logical library than drive %s,Skipping tape %s...",
                        t.vid,
                        drive,
                        t.vid,
                    )
                library_controller.label_tape(drive, t.vid, old_vid=old_label, force=force)
                if dstpool:
                    _move_labelled_tape(obj, t.vid, dstpool)

        def treat_file(library_controller: ManagedLibraryController, file: Path, limit: Optional[int]):
            tapes = []
            LOGGER.info("Reading tape list from file %s", file)
            with open(file, encoding="utf-8") as f:
                tapes.extend(line.strip() for line in f if not line.startswith("#"))

            for index, tape_vid in enumerate(tapes):
                LOGGER.info("Labeling tape %s from file %s on drive %s", tape_vid, file, drive)
                if limit and index > limit:
                    LOGGER.info("Already %s tape labeled, exist the program according to limit argument", limit)
                    return
                try:
                    library_controller.get_tape_location(tape_vid)
                except ResourceNotFoundError:
                    LOGGER.warning(
                        "Tape %s is not in the same logical library than drive %s,Skipping tape %s...",
                        tape_vid,
                        drive,
                        tape_vid,
                    )

                library_controller.label_tape(drive, tape_vid, old_vid=old_label, force=force)
                if dstpool:
                    _move_labelled_tape(obj, tape_vid, dstpool)

        library_controller = _label_library_for_drive(obj, drive)

        if vid:
            treat_single_vid(library_controller, vid)

        elif srcpool:
            treat_srcpool(library_controller, srcpool, limit)

        elif file:
            treat_file(library_controller, file, limit)

    @tape.command("import", aliases=["im"])
    @click.argument(
        "physical_library_name",
        required=False,
    )
    @click.option("--all", "-a", is_flag=True, help="Import for all connected local libraries (even no CTA ones !)")
    @click.pass_obj
    def tape_import(obj: AppContext, physical_library_name: Optional[str], all: bool):
        if not (bool(physical_library_name is not None) ^ all):
            raise click.UsageError("Drive name and --all option are mutually exclusive")

        if physical_library_name is not None:
            try:
                ScsiManagedLibraryControllerProvider(
                    **asdict(obj.config.tape_server.udev_properties_map)
                ).get_library_controller_by_name(physical_library_name).import_tapes()
            except (KeyError, ResourceNotFoundError) as e:
                raise click.ClickException(str(e)) from e
            return

        if all:
            for ll in LocalLibraryRepository().list_libraries():
                LOGGER.info("Importing tapes for local library %s", ll)
                try:
                    ScsiManagedLibraryControllerProvider(
                        **asdict(obj.config.tape_server.udev_properties_map)
                    ).get_library_controller(ll.serial_number).import_tapes()
                except Exception:
                    LOGGER.exception("An exception occurred during unmounting %s", ll)

    @tape.command("export", aliases=["ex"])
    @click.argument("vid", required=False)
    @click.option("--physical-library", "--pl", required=True)
    @click.option("--file", type=click.Path(exists=True, readable=True, path_type=Path))
    @click.option("--srcpool", "--sp")
    @click.option("--ioport-capacity", "-c", type=click.IntRange(min=1))
    @click.option("--limit", "-l", type=click.IntRange(min=1))
    @click.pass_obj
    def tape_export(
        obj: AppContext,
        vid: Optional[str],
        file: Optional[Path],
        srcpool: Optional[str],
        ioport_capacity: Optional[int],
        limit: Optional[int],
        physical_library: str,
    ):
        if bool(vid) + bool(srcpool) + bool(file) != 1:
            raise click.UsageError("Please choose either vid arg, a srcpool, or a file")

        try:
            library_controller = ScsiManagedLibraryControllerProvider(
                **asdict(obj.config.tape_server.udev_properties_map)
            ).get_library_controller_by_name(physical_library)
        except KeyError as e:
            raise click.ClickException(str(e)) from e

        def get_vid(vid: str) -> str:
            tape = obj.cta_client.tape.get(vid)
            if library_controller.name != tape.physical_library_name:
                raise click.ClickException(
                    f"Tape {vid} is not in the same logical library than local tape server,Skipping tape {vid}...",
                )
            return tape.vid

        def get_vids_from_srcpool(srcpool: str, limit: Optional[int]) -> list[str]:
            LOGGER.info("Getting tape list from tapepool %s", srcpool)
            obj.cta_client.tape_pool.get(srcpool)
            tapes = obj.cta_client.tape.list(tape_pool_name=srcpool)
            return [t.vid for t in tapes if library_controller.name == t.physical_library_name][slice(limit)]

        def get_vids_from_file(file: Path, limit: Optional[int]) -> list[str]:
            LOGGER.info("Reading tape list from file %s", file)
            with open(file, encoding="utf-8") as f:
                return [line.strip() for line in f if not line.startswith("#")][slice(limit)]

        max_io_slots = (
            min(library_controller.get_max_io_slots(), ioport_capacity)
            if ioport_capacity is not None
            else library_controller.get_max_io_slots()
        )
        LOGGER.info("Using %s io slots for export", max_io_slots)

        if srcpool:
            vids = get_vids_from_srcpool(srcpool, limit)
        elif file:
            vids = get_vids_from_file(file, limit)
        else:
            assert vid is not None  # The mutually exclusive input guard ensures a VID here.
            vids = [get_vid(vid)]

        export_tape_counter = 0
        for tape_vid in vids:
            LOGGER.info("Exporting tape %s from physical library %s", tape_vid, physical_library)
            if (
                export_tape_counter != 0
                and export_tape_counter % max_io_slots == 0
                and not click.confirm(
                    f"{'*' * 50}\n"
                    f"Reached I/O port capacity of {ioport_capacity} (physical or virtual) slots.\n"
                    "Please remove the exported tapes from the I/O port "
                    "(several extractions might be needed depending on the number of physical slots).\n"
                    f"{'*' * 50}\n"
                    "Do you wish to continue with the execution of the script? (yes|no): "
                )
            ):
                return

            try:
                if library_controller.get_tape_location(tape_vid).location != "HOME":
                    LOGGER.warning("Skipping tape %s, because tape is not in its slot", tape_vid)
                    continue
            except ResourceNotFoundError:
                LOGGER.warning("Skipping tape %s, because tape is not in the library", tape_vid)
                continue

            try:
                library_controller.export_tape(tape_vid)
            except Exception as e:
                LOGGER.warning("An error occur during exportation of tape %s: %s", tape_vid, e)
                continue

            export_tape_counter += 1
