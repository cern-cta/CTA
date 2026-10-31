# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["vo"])
def virtual_organisation():
    """Manage CTA Virtual organizations."""


@virtual_organisation.command(aliases=["add"])
@click.argument("name")
@click.option("--disk-instance", "--di", required=True)
@click.option("--read-max-drives", "--rmd", required=True, type=click.INT)
@click.option("--write-max-drives", "--wmd", required=True, type=click.INT)
@click.option("--type", "-t", type=click.Choice(["DEFAULT", "REPACK"]), default="DEFAULT", show_default=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(
    obj: AppContext, name: str, disk_instance: str, read_max_drives: int, write_max_drives: int, type: str, comment: str
):
    """Create a virtual organization to CTA."""
    obj.cta_client.virtual_organisation.create(
        name,
        disk_instance_name=disk_instance,
        is_repack_vo=(type == "REPACK"),
        max_read_drives=read_max_drives,
        max_write_drives=write_max_drives,
        comment=comment,
    )
    print(f"VirtualOrganisation({name}) CREATED")


@virtual_organisation.command(aliases=["ch"])
@click.argument("name")
@click.option("--disk-instance", "--di")
@click.option("--read-max-drives", "--rmd", type=click.INT)
@click.option("--write-max-drives", "--wmd", type=click.INT)
@click.option("--max-file-size", "--mfs", type=click.INT)
@click.option("--type", "-t", type=click.Choice(["DEFAULT", "REPACK"]))
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    disk_instance: Optional[str],
    read_max_drives: Optional[int],
    write_max_drives: Optional[int],
    max_file_size: Optional[int],
    type: Optional[str],
    comment: Optional[str],
):
    """Update CTA virtual organization."""
    obj.cta_client.virtual_organisation.update(
        name,
        disk_instance_name=disk_instance,
        is_repack_vo=(type == "REPACK") if type else None,
        max_read_drives=read_max_drives,
        max_write_drives=write_max_drives,
        max_file_size=max_file_size,
        comment=comment,
    )
    print(f"VirtualOrganisation({name}) UPDATED")


@virtual_organisation.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List CTA virtual organizations."""
    data = obj.cta_client.virtual_organisation.list()
    print_dataclasses(data, display_config=obj.config.display)


@virtual_organisation.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get virtual organization registered in CTA."""
    print_dataclasses(obj.cta_client.virtual_organisation.get(name), display_config=obj.config.display)


@virtual_organisation.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a virtual organization from CTA."""
    obj.cta_client.virtual_organisation.delete(name)
    print(f"VirtualOrganisation({name}) DELETED")
