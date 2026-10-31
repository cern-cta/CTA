# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Literal, Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import uppercase_choice
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["re"])
def repack():
    """Manage CTA Repacks."""


@repack.command(aliases=["add"])
@click.argument("name")
@click.option("--mount-policy", "--mp", required=True)
@click.option(
    "--action",
    type=click.Choice(["MOVE", "ADDCOPIES"], case_sensitive=False),
    callback=uppercase_choice,
    help="Does BOTH by default",
)
@click.option("--buffer-url", "--burl")
@click.option("--max-files-to-select", "--mfts", type=click.INT)
@click.option("--no-recall", "--nr", is_flag=True)
@click.pass_obj
def create(
    obj: AppContext,
    name: str,
    mount_policy: str,
    action: Literal["MOVE", "ADDCOPIES"],
    buffer_url: Optional[str],
    max_files_to_select: Optional[int],
    no_recall: bool,
):
    """Create CTA tape repack."""
    obj.cta_client.repack_request.create(
        name,
        mount_policy_name=mount_policy,
        custom_operation=action,
        buffer_url=buffer_url,
        max_files_to_select=max_files_to_select,
        no_recall=no_recall,
    )
    print(f"RepackRequest({name}) CREATED")


@repack.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List CTA tape repacks in CTA."""
    print_dataclasses(obj.cta_client.repack_request.list(), display_config=obj.config.display)


@repack.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get tape repack registered in CTA."""
    print_dataclasses(obj.cta_client.repack_request.get(name), display_config=obj.config.display)


@repack.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA tape repack."""
    obj.cta_client.repack_request.delete(name)
    print(f"RepackRequest({name}) DELETED")
