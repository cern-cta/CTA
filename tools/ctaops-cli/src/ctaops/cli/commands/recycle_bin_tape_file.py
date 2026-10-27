# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path
from typing import Optional

import click

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["rbtf"])
def recycle_bin_tape_file():
    """Inspect CTA files in the recycle bin"""


@recycle_bin_tape_file.command("list", aliases=["ls"])
@click.option("--vid", "-v")
@click.option(
    "--fids-file", "-F", type=click.Path(exists=True, resolve_path=True, dir_okay=False, readable=True, path_type=Path)
)
@click.option("--disk-instance", "--di")
@click.option("--min-deletion-time", "--lmint", type=click.INT)
@click.option("--max-deletion-time", "--lmaxt", type=click.INT)
@click.option("--virtual-organisation", "--vo")
@click.pass_obj
def list_command(
    obj: AppContext,
    vid: Optional[str],
    fids_file: Optional[str],
    disk_instance: Optional[str],
    min_deletion_time: Optional[int],
    max_deletion_time: Optional[int],
    virtual_organisation: Optional[str],
):
    """List CTA files in the recycle bin"""
    if not any([vid, virtual_organisation, disk_instance]):
        raise click.UsageError("You must specify one of: --vid, --disk-instance, or --virtual-organisation")

    kwargs = {}
    kwargs |= {"vid": vid} if vid else {}
    kwargs |= {"virtual_organisation_name": virtual_organisation} if virtual_organisation else {}
    kwargs |= {"min_deletion_time": min_deletion_time} if min_deletion_time else {}
    kwargs |= {"max_deletion_time": max_deletion_time} if max_deletion_time else {}
    kwargs |= {"fids_file": fids_file} if fids_file else {}
    kwargs |= {"disk_instance_name": disk_instance} if disk_instance else {}

    print_dataclasses(obj.cta_client.recycle_bin_tape_file.list(**kwargs), display_config=obj.config.display)


@recycle_bin_tape_file.command()
@click.option("--archive-id", type=click.INT)
@click.option("--disk-id", type=str)
@click.option("--disk-instance", type=str)
@click.option("--vid", "-v", required=True)
@click.pass_obj
def get(obj: AppContext, archive_id: Optional[int], vid: str, disk_id: Optional[str], disk_instance: Optional[str]):
    """Get CTA file in the recycle bin"""
    if archive_id:
        print_dataclasses(
            obj.cta_client.recycle_bin_tape_file.get(archive_id, vid=vid), display_config=obj.config.display
        )
    elif disk_id and disk_instance:
        print_dataclasses(
            obj.cta_client.recycle_bin_tape_file.get(disk_id, disk_instance, vid=vid), display_config=obj.config.display
        )
    else:
        raise click.UsageError("You must specify either --archive-id or both --disk-id and --disk-instance")


@recycle_bin_tape_file.command()
@click.option("--archive-id", type=click.INT)
@click.option("--disk-id", type=str)
@click.option("--disk-instance", type=str)
@click.pass_obj
def get_all_copies(obj: AppContext, archive_id: Optional[int], disk_id: Optional[str], disk_instance: Optional[str]):
    """Get all copies of a CTA file in the recycle bin"""
    if archive_id:
        copies = obj.cta_client.recycle_bin_tape_file.get_all_copies(archive_id)
    elif disk_id and disk_instance:
        copies = obj.cta_client.recycle_bin_tape_file.get_all_copies(disk_id, disk_instance)
    else:
        raise click.UsageError("You must specify either --archive-id or both --disk-id and --disk-instance")
    print_dataclasses(copies, display_config=obj.config.display)
