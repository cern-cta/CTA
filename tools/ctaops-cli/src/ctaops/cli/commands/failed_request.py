# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from cta.sdk.models import MountType
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import enum_choice
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["fr"])
def failed_request():
    """Manage CTA failed requests."""


@failed_request.command("list", aliases=["ls"])
@click.option(
    "--type",
    type=click.Choice([f.name for f in MountType], case_sensitive=False),
    callback=enum_choice(MountType),
)
@click.option("--tape-pool", "--tp")
@click.option("--vid", "-v")
@click.option("--logs", "-l", is_flag=True)
@click.pass_obj
def list_command(obj: AppContext, type: Optional[MountType], tape_pool: Optional[str], vid: Optional[str], logs: bool):
    """List CTA failed requests."""
    print_dataclasses(
        obj.cta_client.failed_request.list(type=type, tape_pool_name=tape_pool, vid=vid, logs=logs),
        display_config=obj.config.display,
    )


@failed_request.command(aliases=["s"])
@click.pass_obj
def summary(obj: AppContext):
    """List CTA failed requests."""
    print_dataclasses(
        obj.cta_client.failed_request.summary(),
        display_config=obj.config.display,
    )


@failed_request.command(aliases=["rm"])
@click.argument("object_id")
@click.pass_obj
def delete(obj: AppContext, object_id: str):
    """Remove a CTA failed request."""
    obj.cta_client.failed_request.delete(object_id)
