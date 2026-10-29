# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["tf"])
def tape_file():
    """Manage CTA Tapes Files"""


@tape_file.command("list", aliases=["ls"])
@click.option("--vid", "-v")
@click.option("--disk-instance", "--di")
@click.option("--disk-file-id", "--dfid")
@click.option("--archive-id", "-I", type=click.INT)
@click.option(
    "--fid-file", "-F", type=click.Path(exists=True, resolve_path=True, dir_okay=False, readable=True, path_type=Path)
)
@click.pass_obj
def list_command(
    obj: AppContext,
    vid: Optional[str],
    disk_instance: Optional[str],
    disk_file_id: Optional[str],
    archive_id: Optional[int],
    fid_file: Optional[str],
):
    """List CTA tape files"""

    if fid_file and not disk_instance:
        raise click.UsageError("--disk-instance is required when using --fid-file")

    if disk_file_id and not disk_instance:
        raise click.UsageError("--disk-instance is required when using --disk-file-id")

    if vid:
        data = obj.cta_client.tape_file.list(vid=vid)
    elif fid_file and disk_instance:
        data = obj.cta_client.tape_file.list(fids_file=fid_file, disk_instance_name=disk_instance)
    elif archive_id:
        data = (
            [obj.cta_client.tape_file.get(archive_id, vid=vid)]
            if vid
            else list(obj.cta_client.tape_file.get_all_copies(archive_id))
        )
    elif disk_file_id and disk_instance:
        data = (
            [obj.cta_client.tape_file.get(disk_file_id, disk_instance, vid=vid)]
            if vid
            else list(obj.cta_client.tape_file.get_all_copies(disk_file_id, disk_instance))
        )
    else:
        raise click.UsageError("You must specify one of: --vid, --fid-file, --archive-id, or --disk-file-id")

    print_dataclasses(data, display_config=obj.config.display)


@tape_file.command()
@click.option("--vid", "-v", required=True)
@click.option("--archive-id", "-I", type=click.INT, required=True)
@click.pass_obj
def get(obj: AppContext, vid: str, archive_id: int):
    """List CTA tape files"""
    print_dataclasses(obj.cta_client.tape_file.get(archive_id, vid=vid), display_config=obj.config.display)


@tape_file.command(aliases=["rm"])
@click.argument("vid")
@click.option("--disk-instance", "--di")
@click.option("--disk-file-id", "--dfid")
@click.option("--archive-id", "-I")
@click.option("--reason", "-r", required=True)
@click.pass_obj
def delete(
    obj: AppContext,
    vid: str,
    disk_instance: Optional[str],
    disk_file_id: Optional[str],
    archive_id: Optional[int],
    reason: str,
):
    """Remove CTA tape files located on a tape"""
    if bool(disk_instance) != (bool(disk_file_id)):
        raise click.UsageError("--disk-instance and --disk-file-id/--fxid are mutually needed")

    if archive_id:
        obj.cta_client.tape_file.delete(archive_id, vid=vid, reason=reason)
        print(f"TapeFile({archive_id},{vid},*) DELETED")
    elif disk_instance and disk_file_id:
        obj.cta_client.tape_file.delete(disk_file_id, disk_instance, vid=vid, reason=reason)
        print(f"TapeFile({disk_file_id},{vid},*) DELETED")
    else:
        raise click.UsageError("You must specify either --archive-id or both --disk-instance and --disk-file-id")
