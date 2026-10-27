# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["sc"])
def storage_class():
    """Manage CTA Storage Classes"""


@storage_class.command(aliases=["add"])
@click.argument("name")
@click.option("--number-of-copies", "--noc", required=True, type=click.INT)
@click.option("--virtual-organisation", "--vo", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, number_of_copies: int, virtual_organisation: str, comment: str):
    """Create CTA storage class"""
    obj.cta_client.storage_class.create(
        name, comment=comment, virtual_organisation_name=virtual_organisation, replica_count=number_of_copies
    )
    print(f"StorageClass({name}) CREATED")


@storage_class.command(aliases=["ch"])
@click.argument("name")
@click.option("--number-of-copies", "--noc", type=click.INT)
@click.option("--virtual-organisation", "--vo")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    number_of_copies: Optional[int],
    virtual_organisation: Optional[str],
    comment: Optional[str],
):
    """Update CTA storage class"""
    obj.cta_client.storage_class.update(
        name, replica_count=number_of_copies, virtual_organisation_name=virtual_organisation, comment=comment
    )
    print(f"StorageClass({name}) UPDATED")


@storage_class.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List CTA storage classes in CTA"""
    print_dataclasses(obj.cta_client.storage_class.list(), display_config=obj.config.display)


@storage_class.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get storage class registered in CTA"""
    print_dataclasses(obj.cta_client.storage_class.get(name), display_config=obj.config.display)


@storage_class.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA storage class"""
    obj.cta_client.storage_class.delete(name)
    print(f"StorageClass({name}) DELETED")
