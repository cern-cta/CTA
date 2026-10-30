# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable
from datetime import timedelta
from logging import getLogger
from socket import gethostname
from time import perf_counter
from typing import Optional

import click
from cta.sdk.models import Drive, DriveOperatingState, LogicalLibraryState
from cta.sdk.units import Byte, BytePerSecond
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import EXTRA_TPSRV, AppContext, tapeserver_option
from ctaops.cli.display.encoders import encode_table_field
from ctaops.cli.display.printers import print_dataclasses
from rich import print

LOGGER = getLogger("ctaops.cli")


@click.group(cls=ClickAliasedGroup, aliases=["dr"])
def drive():
    """Manage CTA Tape Drives."""


@drive.command("list", aliases=["ls"])
@click.option("--all", "-a", is_flag=True, help="Mutually exclusive with --local.")
@tapeserver_option("--local", "-l", is_flag=True, help="Mutually exclusive with --all.")
@click.pass_obj
def list_command(obj: AppContext, all: bool, local: Optional[bool] = None):
    """List drive in cta catalogue."""

    def updated_drives_with_disabled_libraries(drives: Iterable[Drive]) -> list[Drive]:
        drives = list(drives)
        disabled_library_names = [
            ll.name for ll in obj.cta_client.logical_library.list(state=LogicalLibraryState.DISABLED)
        ]
        for d in drives:
            if d.logical_library_name in disabled_library_names:
                d.logical_library_name = "!" + d.logical_library_name
        return drives

    if EXTRA_TPSRV:
        from ctaops.cli.models.drive import EnrichedDrive
        from ctaops.sdk import LocalDriveRepository, LocalNamedDriveRepository

        local_named_drives = LocalNamedDriveRepository(
            drive_name_key=obj.config.tape_server.udev_properties_map.drive_name_key
        ).list_drives()
        print_dataclasses(
            (
                EnrichedDrive.from_drive(
                    dr,
                    serial_number=(
                        sn := next((lnd.serial_number for lnd in local_named_drives if lnd.name == dr.name), None)
                    ),
                    is_connected=bool(sn),
                )
                for dr in updated_drives_with_disabled_libraries(obj.cta_client.drive.list(all=all))
                if (
                    not local
                    or next((lnd.serial_number for lnd in local_named_drives if lnd.name == dr.name), None) is not None
                )
            ),
            display_config=obj.config.display,
        )

        if local:
            cta_drive_names = {pl.name for pl in obj.cta_client.drive.list(all=True)}
            if cta_unknown_named_libs := {lnd for lnd in local_named_drives if lnd.name not in cta_drive_names}:
                print_dataclasses(
                    cta_unknown_named_libs, display_config=obj.config.display, title="Non-CTA Drives (Named)"
                )

            if others_drives := (LocalDriveRepository().list_drives() - local_named_drives):
                print_dataclasses(others_drives, display_config=obj.config.display, title="Non-CTA Drives")
    else:
        print_dataclasses(
            updated_drives_with_disabled_libraries(obj.cta_client.drive.list(all=all)),
            display_config=obj.config.display,
        )


@drive.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get drive registered in CTA."""
    drive = obj.cta_client.drive.get(name)

    if EXTRA_TPSRV:
        from ctaops.cli.models.drive import EnrichedDrive
        from ctaops.sdk import LocalNamedDriveRepository, ResourceNotFoundError

        try:
            local_drive = LocalNamedDriveRepository(
                drive_name_key=obj.config.tape_server.udev_properties_map.drive_name_key
            ).get_drive_by_name(name)
        except ResourceNotFoundError:
            drive = EnrichedDrive.from_drive(drive, serial_number=None, is_connected=False)
        else:
            drive = EnrichedDrive.from_drive(drive, serial_number=local_drive.serial_number, is_connected=True)

    print_dataclasses(drive, display_config=obj.config.display)


@drive.command()
@click.argument("name", required=False)
@click.option("-r", "--reason")
@tapeserver_option("-l", "--local", is_flag=True)
@click.pass_obj
def up(obj: AppContext, name: Optional[str], reason: Optional[str], local: Optional[bool] = None):
    """Put the CTA drive up."""
    if not (bool(name is not None) ^ bool(local)):
        raise click.UsageError("Provide either a drive arg or --local flag")

    if name is not None:
        obj.cta_client.drive.up(name, reason=reason)
        print(f"Drive({name}) UP")
        return

    if local is True:
        # easy implementation maybe move to ctaops.sdk for better accuracy
        for d in obj.cta_client.drive.list(all=True):
            if d.host_name.split(".")[0].lower().strip() == gethostname().split(".")[0].lower().strip():
                obj.cta_client.drive.up(d.name)
                print(f"Drive({name}) UP")
        return


@drive.command()
@click.argument("name", required=False)
@click.option("-r", "--reason", required=True)
@click.option("-f", "--force", is_flag=True)
@tapeserver_option("-l", "--local", is_flag=True)
@click.pass_obj
def down(obj: AppContext, name: Optional[str], reason: str, force: bool, local: Optional[bool] = None):
    """Put the CTA drive down."""
    if not (bool(name is not None) ^ bool(local)):
        raise click.UsageError("Provide either a drive arg or --local flag")

    if name is not None:
        obj.cta_client.drive.down(name, reason=reason, force=force)
        print(f"Drive({name}) DOWN")
        return

    if local is True:
        # easy implementation maybe move to ctaops.sdk for better accuracy
        for d in obj.cta_client.drive.list(all=True):
            if d.host_name.split(".")[0].lower().strip() == gethostname().split(".")[0].lower().strip():
                obj.cta_client.drive.down(d.name, reason=reason, force=force)
                print(f"Drive({name}) DOWN")

        return


@drive.command(aliases=["ch"])
@click.argument("name")
@click.option("--comment", "-c")
@click.pass_obj
def update(obj: AppContext, name: str, comment: Optional[str]):
    """Update the comment of a drive."""
    obj.cta_client.drive.update(name, comment=comment)
    print(f"Drive({name}) UPDATED")


@drive.command(aliases=["rm"])
@click.argument("name")
@click.option("-f", "--force", is_flag=True)
@click.pass_obj
def delete(obj: AppContext, name: str, force: bool):
    """Remove a drive from CTA."""
    obj.cta_client.drive.delete(name, force=force)
    print(f"Drive({name}) DELETED")


if EXTRA_TPSRV:
    from dataclasses import asdict

    from ctaops.sdk import (
        LocalLibraryRepository,
        LocalLocatedDriveRepository,
        ResourceNotFoundError,
        ScsiManagedLibraryControllerProvider,
    )

    @drive.command(aliases=["m"])
    @click.argument("drive_name")
    @click.option("--vid", "-v", required=True)
    @click.option("--read-vol1", is_flag=True)
    @click.pass_obj
    def mount(obj: AppContext, drive_name: str, vid: str, read_vol1: bool):
        try:
            obj.cta_client.tape.get(vid)
            mounted_drive = (
                ScsiManagedLibraryControllerProvider(**asdict(obj.config.tape_server.udev_properties_map))
                .get_library_controller_from_drive_name(drive_name)
                .mount(drive_name, vid)
            )
        except ResourceNotFoundError as e:
            raise click.ClickException(str(e)) from e

        if read_vol1:
            print(f"Read vol1: {mounted_drive.get_VOL1()!r}")

    @drive.command(aliases=["u"])
    @click.argument("drive_name", required=False)
    @click.option("--all", "-a", is_flag=True, help="Unmount all local drives (even no CTA ones !)")
    @click.pass_obj
    def unmount(obj: AppContext, drive_name: Optional[str], all: bool):
        if not (bool(drive_name is not None) ^ all):
            raise click.UsageError("Drive name and --all option are mutually exclusive")

        if drive_name is not None:
            try:
                ScsiManagedLibraryControllerProvider(
                    **asdict(obj.config.tape_server.udev_properties_map)
                ).get_library_controller_from_drive_name(drive_name).unmount(drive_name)
            except ResourceNotFoundError as e:
                raise click.ClickException(str(e)) from e

        if all:
            local_libraries = {ll.serial_number: ll for ll in LocalLibraryRepository().list_libraries()}
            LOGGER.debug("Local libraries found: %s", local_libraries)

            for drive in LocalLocatedDriveRepository(
                drive_library_address_key=obj.config.tape_server.udev_properties_map.drive_library_address_key,
                drive_library_ordinal_key=obj.config.tape_server.udev_properties_map.drive_library_ordinal_key,
            ).list_drives():
                if drive.location.library_serial_number in local_libraries:
                    LOGGER.info("Found %s in library %s", drive, drive.location.library_serial_number)
                    try:
                        ScsiManagedLibraryControllerProvider(
                            **asdict(obj.config.tape_server.udev_properties_map)
                        ).get_library_controller(drive.location.library_serial_number).unmount(drive.location.ordinal)
                    except Exception:
                        LOGGER.exception("An exception occurred during unmounting %s", drive)
                else:
                    LOGGER.warning("Skipping %s, unable to find mediachanger locally", drive)

    @drive.command(aliases=["t"])
    @click.argument("drive_name")
    @click.option("--vid", "-v", required=True)
    @click.option("--file-count", "-n", type=int, default=100, show_default=True)
    @click.option("--file-size", "-s", type=int, default=10 * 1024**3, show_default=True)
    @click.pass_obj
    def test(obj: AppContext, drive_name: str, vid: str, file_count: int, file_size: int):
        cta_tape = obj.cta_client.tape.get(vid)

        if not cta_tape.tape_pool_name.startswith(("test_", "tolabel_", "erase_")):
            raise click.ClickException(
                "Tape must belongs to a tapepool with one of the following prefix: 'test_','tolabel_' ,'erase_'"
            )

        if not cta_tape.statistics.file_count == 0:
            raise click.ClickException("There are CTA files on this tape ! Aborting...")

        try:
            lib_ctrl = ScsiManagedLibraryControllerProvider(
                **asdict(obj.config.tape_server.udev_properties_map)
            ).get_library_controller_from_drive_name(drive_name)
        except ResourceNotFoundError as e:
            raise click.ClickException(str(e)) from e

        cta_drive = obj.cta_client.drive.get(drive_name)

        if cta_drive.state is not DriveOperatingState.DOWN or not cta_drive.state_reason:
            raise click.ClickException(f"Drive {cta_drive.name} must be DOWN with a reason")

        with lib_ctrl.mount(drive_name, vid) as md:
            if vid not in (vol1 := md.get_VOL1()):
                raise click.ClickException(f"Expected {vid=} has a wrong label: {vol1=!r}")

            md.rewind()

            LOGGER.info(
                "Start writing %s files of size %s on tape %s", file_count, encode_table_field(file_size, Byte), vid
            )
            t0 = perf_counter()
            w_checksum = 1
            with click.progressbar(length=file_count, label="Writing") as bar:
                for file_index, c_checksum in md.write_files(file_size, file_count):
                    bar.update(1, file_index)
                    w_checksum = c_checksum
            w_time = timedelta(seconds=perf_counter() - t0)
            LOGGER.info(
                "%s files of size %s written on tape %s in %s",
                file_count,
                encode_table_field(file_size, Byte),
                vid,
                encode_table_field(w_time),
            )
            LOGGER.info(
                "Average speed: %s", encode_table_field(file_count * file_size / w_time.total_seconds(), BytePerSecond)
            )

            md.rewind()

            LOGGER.info(
                "Start reading %s files of size %s on tape %s", file_count, encode_table_field(file_size, Byte), vid
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
                vid,
                encode_table_field(r_time),
            )
            LOGGER.info(
                "Average speed: %s", encode_table_field(file_count * file_size / r_time.total_seconds(), BytePerSecond)
            )
