# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import BytesType
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["mt"])
def media_type():
    """Manage CTA Media Types"""


@media_type.command(aliases=["add"])
@click.argument("name")
@click.option("--cartridge", required=True)
@click.option("--capacity", "-C", required=True, type=BytesType())
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, cartridge: str, capacity: int, comment: str):
    """Create CTA mediatype"""
    obj.cta_client.media_type.create(name, cartridge_type=cartridge, capacity=capacity, comment=comment)
    print(f"MediaType({name}) CREATED")


@media_type.command(aliases=["ch"])
@click.argument("name")
@click.option("--cartridge")
@click.option("--primary-density-code", "--pdc", type=click.INT)
@click.option("--secondary-density-code", "--sdc", type=click.INT)
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    cartridge: Optional[str],
    primary_density_code: Optional[int],
    secondary_density_code: Optional[int],
    comment: Optional[str],
):
    """Update CTA mediatype"""
    obj.cta_client.media_type.update(
        name,
        cartridge_type=cartridge,
        primary_density_code=primary_density_code,
        secondary_density_code=secondary_density_code,
        comment=comment,
    )
    print(f"MediaType({name}) UPDATED")


@media_type.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List media types registered in CTA"""
    print_dataclasses(obj.cta_client.media_type.list(), display_config=obj.config.display)


@media_type.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get media type registered in CTA"""
    print_dataclasses(obj.cta_client.media_type.get(name), display_config=obj.config.display)


@media_type.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA media type"""
    obj.cta_client.media_type.delete(name)
    print(f"MediaType({name}) DELETED")
