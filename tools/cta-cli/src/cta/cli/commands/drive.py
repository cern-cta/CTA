# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from logging import getLogger
from socket import gethostname
from time import perf_counter
from typing import Callable, Literal, Optional

import click
from cta.cli.click_extension.aliases import ClickAliasedGroup
from cta.cli.context import EXTRA_TPSRV, AppContext, tapeserver_option
from cta.cli.display.encoders import encode_table_field
from cta.cli.display.printers import print_dataclasses
from cta.sdk.exceptions import ResourceNotFoundError as CTAResourceNotFoundError
from cta.sdk.models import Drive, DriveOperatingState, LogicalLibraryState
from cta.sdk.units import Byte, BytePerSecond
from rich import print

LOGGER = getLogger("cta.cli")


@click.group(cls=ClickAliasedGroup, aliases=["dr"])
def drive():
    """Manage CTA Tape Drives."""


@drive.command("list", aliases=["ls"])
@click.option("--all", "-a", is_flag=True, help="Mutually exclusive with --local.")
@tapeserver_option("--local", "-l", is_flag=True, help="Mutually exclusive with --all.")
@click.pass_obj
def list_command(obj: AppContext, all: bool, local: Optional[bool] = None):
    """List drive in cta catalogue."""

    def update_drives_with_disabled_libraries(drives: Iterable[Drive]) -> list[Drive]:
        drives = list(drives)
        disabled_library_names = [
            ll.name for ll in obj.cta_client.logical_library.list(state=LogicalLibraryState.DISABLED)
        ]
        for d in drives:
            if d.logical_library_name in disabled_library_names:
                d.logical_library_name = "!" + d.logical_library_name
        return drives

    if EXTRA_TPSRV:
        from cta.cli.models.drive import EnrichedDrive
        from cta.tapeserver.sdk import LocalDriveRepository, LocalLocatedNamedDriveRepository

        local_located_named_drives = LocalLocatedNamedDriveRepository(
            drive_name_key=obj.config.tape_server.udev_properties_map.drive_name_key
        ).list_drives()
        print_dataclasses(
            (
                EnrichedDrive.merge(
                    dr,
                    local_drive=next((llnd for llnd in local_located_named_drives if llnd.name == dr.name), None),
                )
                for dr in update_drives_with_disabled_libraries(obj.cta_client.drive.list(all=all))
                if (
                    not local
                    or next((llnd for llnd in local_located_named_drives if llnd.name == dr.name), None) is not None
                )
            ),
            display_config=obj.config.display,
        )

        if local:
            cta_drive_names = {pl.name for pl in obj.cta_client.drive.list(all=True)}
            if cta_unknown_named_libs := {lnd for lnd in local_located_named_drives if lnd.name not in cta_drive_names}:
                print_dataclasses(
                    cta_unknown_named_libs, display_config=obj.config.display, title="Non-CTA Drives (Named & Located)"
                )

            if others_drives := (LocalDriveRepository().list_drives() - local_located_named_drives):
                print_dataclasses(others_drives, display_config=obj.config.display, title="Non-CTA Drives")
    else:
        print_dataclasses(
            update_drives_with_disabled_libraries(obj.cta_client.drive.list(all=all)),
            display_config=obj.config.display,
        )


@drive.command()
@click.argument("name")
@tapeserver_option("-l", "--local", is_flag=True, help="Don't fail if library not present in CTA")
@click.pass_obj
def get(obj: AppContext, name: str, local: Optional[bool] = None):
    """Get drive registered in CTA."""
    if EXTRA_TPSRV:
        from cta.cli.models.drive import EnrichedDrive
        from cta.tapeserver.sdk import (
            LocalLocatedNamedDriveRepository,
        )
        from cta.tapeserver.sdk import (
            ResourceNotFoundError as LocalResourceNotFoundError,
        )

        if local:
            local_drive = LocalLocatedNamedDriveRepository(
                drive_name_key=obj.config.tape_server.udev_properties_map.drive_name_key
            ).get_drive_by_name(name)
            try:
                cta_drive = obj.cta_client.drive.get(name)
            except CTAResourceNotFoundError:
                drive = local_drive
            else:
                drive = EnrichedDrive.merge(cta_drive, local_drive)

        else:
            cta_drive = obj.cta_client.drive.get(name)
            try:
                local_drive = LocalLocatedNamedDriveRepository(
                    drive_name_key=obj.config.tape_server.udev_properties_map.drive_name_key
                ).get_drive_by_name(name)
            except LocalResourceNotFoundError:
                local_drive = None

            drive = EnrichedDrive.merge(cta_drive, local_drive)
    else:
        drive = obj.cta_client.drive.get(name)

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
        # easy implementation maybe move to cta.tapeserver.sdk for better accuracy
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
        # easy implementation maybe move to cta.tapeserver.sdk for better accuracy
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

    from cta.cli.models.drive import DriveSensors
    from cta.common.prometheus import Labels, Value, sample, write_atomic
    from cta.tapeserver.sdk import (
        DriveController,
        DriveOperationError,
        LocalLibraryRepository,
        LocalLocatedDriveRepository,
        LocalNamedDriveRepository,
        NamedDrive,
        ResourceNotFoundError,
        ScsiDriveControllerProvider,
        ScsiManagedLibraryControllerProvider,
    )
    from pyudev import Context

    def read_sensor(read: Callable[[], int], drive: NamedDrive, sensor: str) -> Optional[int]:
        try:
            return read()
        except DriveOperationError as e:
            LOGGER.debug("Unable to read %s of drive %s: %s", sensor, drive.name, e)
            return None

    def read_sensors(drive: NamedDrive, controller: DriveController) -> DriveSensors:
        return DriveSensors(
            name=drive.name,
            serial_number=drive.serial_number,
            temperature_celsius=read_sensor(controller.get_drive_temperature, drive, "temperature"),
            humidity_percent=read_sensor(controller.get_drive_humidity, drive, "humidity"),
        )

    def render_sensors(sensors: list[DriveSensors]) -> str:
        prefix = "ctaops_drive"
        # (metric, help, samples)
        metrics: list[tuple[str, str, list[tuple[Labels, Value]]]] = [
            (
                "sensor_read_success",
                "1 if the sensor of the drive could be read.",
                [
                    ({**e.labels(), "sensor": sensor}, int(value is not None))
                    for e in sensors
                    for sensor, value in (("temperature", e.temperature_celsius), ("humidity", e.humidity_percent))
                ],
            ),
            (
                "temperature_celsius",
                "Internal temperature of the drive in degrees Celsius.",
                [(e.labels(), e.temperature_celsius) for e in sensors if e.temperature_celsius is not None],
            ),
            (
                "humidity_percent",
                "Internal relative humidity of the drive in percent.",
                [(e.labels(), e.humidity_percent) for e in sensors if e.humidity_percent is not None],
            ),
        ]
        lines: list[str] = []
        for metric, help_text, samples in metrics:
            lines.append(f"# HELP {prefix}_{metric} {help_text}")
            lines.append(f"# TYPE {prefix}_{metric} gauge")
            lines.extend(sample(metric, labels, value, prefix=prefix) for labels, value in samples)
        return "\n".join(lines) + "\n"

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

    @drive.command("sensors", aliases=["s"])
    @click.argument("drive_names", nargs=-1)
    @click.option("--all", "-a", is_flag=True, help="Read all local named drives. Mutually exclusive with drive names.")
    @click.option(
        "-j", "--jobs", type=click.IntRange(min=1), default=8, show_default=True, help="Number of parallel reads."
    )
    @click.option(
        "-c",
        "--output-content",
        type=click.Choice(["sensors", "prom-metrics"]),
        default="sensors",
        show_default=True,
        help="Choose the content of the output",
    )
    @click.option(
        "-o", "--prom-output-file", type=click.Path(dir_okay=False), help="Write metrics to this file atomically"
    )
    @click.pass_obj
    def sensors_command(
        obj: AppContext,
        drive_names: tuple[str, ...],
        all: bool,
        jobs: int,
        output_content: Literal["sensors", "prom-metrics"],
        prom_output_file: Optional[str],
    ):
        """Show temperature and humidity of local drives."""
        if not (bool(drive_names) ^ all):
            raise click.UsageError("Provide either drive names or --all flag")
        if prom_output_file is not None and output_content != "prom-metrics":
            raise click.UsageError("You can't use --prom-output-file if --output-content/-c is not 'prom-metrics'")

        drive_name_key = obj.config.tape_server.udev_properties_map.drive_name_key
        context = Context()
        local_drives = sorted(
            LocalNamedDriveRepository(context, drive_name_key=drive_name_key).list_drives(), key=lambda d: d.name
        )

        if unknown_names := set(drive_names) - {d.name for d in local_drives}:
            raise click.UsageError(f"Drive(s) not found locally: {', '.join(sorted(unknown_names))}")

        drives = local_drives if all else [d for d in local_drives if d.name in drive_names]
        provider = ScsiDriveControllerProvider(context, drive_name_key=drive_name_key)
        controllers = [provider.get_drive_controller(d.serial_number) for d in drives]

        with ThreadPoolExecutor(max_workers=jobs) as pool:
            sensors = list(pool.map(read_sensors, drives, controllers))

        if output_content == "prom-metrics":
            metrics = render_sensors(sensors)
            if prom_output_file is None:
                click.echo(metrics)
            else:
                write_atomic(prom_output_file, metrics)
        elif output_content == "sensors":
            print_dataclasses(sensors, display_config=obj.config.display)
