# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["di"])
def disk_instance():
    """Manage CTA disk instances."""


@disk_instance.command(aliases=["add"])
@click.argument("name")
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, comment: str):
    """Create a CTA disk instance."""
    obj.cta_client.disk_instance.create(name, comment=comment)
    print(f"DiskInstance({name}) CREATED")


@disk_instance.command(aliases=["ch"])
@click.argument("name")
@click.option("--comment", "-c")
@click.pass_obj
def update(obj: AppContext, name: str, comment: Optional[str]):
    """Update a CTA disk instance."""
    obj.cta_client.disk_instance.update(name, comment=comment)
    print(f"DiskInstance({name}) UPDATED")


@disk_instance.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List disk instances in cta catalogue."""
    print_dataclasses(obj.cta_client.disk_instance.list(), display_config=obj.config.display)


@disk_instance.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get disk instance registered in CTA."""
    print_dataclasses(obj.cta_client.disk_instance.get(name), display_config=obj.config.display)


@disk_instance.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA disk instance."""
    obj.cta_client.disk_instance.delete(name)
    print(f"DiskInstance({name}) DELETED")
