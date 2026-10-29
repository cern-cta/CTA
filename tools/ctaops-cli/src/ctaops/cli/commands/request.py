# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Commands for querying and summarizing CTA requests.

This module provides commands to view queued archive and retrieve request information,
with optional grouping by tape pool, logical library, or virtual organisation.
"""

from typing import Literal, Optional, get_args

import click
from cta.sdk.daos.protocols.user_request import GroupBy, OrderBy
from cta.sdk.models import MountType

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses

GroupByAlias = Literal["ll", "tp", "vo"]
GROUP_BY_CHOICES = get_args(GroupBy) + get_args(GroupByAlias)
GROUP_BY_ALIAS_MAP = dict(zip(get_args(GroupByAlias), get_args(GroupBy)))


@click.group(cls=ClickAliasedGroup, aliases=["rq"])
def request():
    """Manage CTA requests"""


@request.command("list", aliases=["ls"])
@click.option(
    "--type",
    "-t",
    type=click.Choice([f.name for f in MountType], case_sensitive=False),
    callback=lambda _, __, value: MountType[value.upper()] if value is not None else None,
)
@click.pass_obj
def list_command(obj: AppContext, type: Optional[MountType]):
    """List CTA requests by type."""
    queues = obj.cta_client.user_request.list(request_type=type)
    print_dataclasses(queues, display_config=obj.config.display)


@request.command(aliases=["s"])
@click.option(
    "--type",
    "-t",
    type=click.Choice([f.name for f in MountType], case_sensitive=False),
    callback=lambda _, __, value: MountType[value.upper()] if value is not None else None,
    required=True,
)
@click.option(
    "--group-by",
    "-g",
    type=click.Choice(GROUP_BY_CHOICES),
    callback=lambda _, __, value: GROUP_BY_ALIAS_MAP.get(value, value),
    default="tape_pool_name",
)
@click.option(
    "--order-by",
    "-o",
    type=click.Choice(get_args(OrderBy)),
    default="queued_byte_count",
)
@click.pass_obj
def summary(
    obj: AppContext,
    type: MountType,
    group_by: GroupBy,
    order_by: OrderBy,
):
    """Show request summary grouped by tape pool, library, or virtual organisation."""
    queues = list(obj.cta_client.user_request.list(request_type=type))
    summary = obj.cta_client.user_request.summary(
        queues,
        group_by=group_by,
        order_by=order_by,
    )
    print_dataclasses(reversed(summary), display_config=obj.config.display)
