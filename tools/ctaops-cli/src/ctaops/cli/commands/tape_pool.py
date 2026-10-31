# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["tp"])
def tape_pool():
    """Manage CTA Tape Pools."""


@tape_pool.command(aliases=["add"])
@click.argument("name")
@click.option("--virtual-organisation", "--vo", required=True)
@click.option("--partial-tape-number", "--ptn", required=True, type=click.INT)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, virtual_organisation: str, partial_tape_number: int, comment: str):
    """Create a tapepool to CTA."""
    obj.cta_client.tape_pool.create(
        name,
        virtual_organisation_name=virtual_organisation,
        min_writable_tape_count=partial_tape_number,
        comment=comment,
    )
    print(f"TapePool({name}) CREATED")


@tape_pool.command(aliases=["ch"])
@click.argument("name")
@click.option("--virtual-organisation", "--vo")
@click.option("--partial-tape-number", "--ptn", type=click.INT)
@click.option("--encryption-key", "--ek")
@click.option("--supply", "-s")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    virtual_organisation: Optional[str],
    partial_tape_number: Optional[int],
    encryption_key: Optional[str],
    supply: Optional[str],
    comment: Optional[str],
):
    """Update CTA tape pool."""
    supply_pools = supply.split(",") if supply else None
    obj.cta_client.tape_pool.update(
        name,
        virtual_organisation_name=virtual_organisation,
        min_writable_tape_count=partial_tape_number,
        supply_tape_pool_names=supply_pools,
        comment=comment,
    )
    if encryption_key:
        obj.cta_client.tape_pool.set_encryption_key_name(name, encryption_key_name=encryption_key)
    print(f"TapePool({name}) UPDATED")


@tape_pool.command("list", aliases=["ls"])
@click.option("--virtual-organisation", "--vo")
@click.option("--encrypted/--not-encrypted", "-e/--ne", default=None)
@click.pass_obj
def list_command(obj: AppContext, virtual_organisation: Optional[str], encrypted: Optional[bool]):
    """List CTA tape pools."""
    print_dataclasses(
        obj.cta_client.tape_pool.list(virtual_organisation_name=virtual_organisation, encrypted=encrypted),
        display_config=obj.config.display,
    )


@tape_pool.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get tape pool registered in CTA."""
    print_dataclasses(obj.cta_client.tape_pool.get(name), display_config=obj.config.display)


@tape_pool.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a tape pool from CTA."""
    obj.cta_client.tape_pool.delete(name)
    print(f"TapePool({name}) DELETED")
