# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from cta.sdk.models import ArchiveRouteType
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import enum_choice
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["ar"])
def archive_route():
    """Manage CTA Archive Routes."""


@archive_route.command(aliases=["add"])
@click.argument("storage_class_name")
@click.argument("number", type=click.INT)
@click.argument(
    "type",
    type=click.Choice([f.name for f in ArchiveRouteType], case_sensitive=False),
    callback=enum_choice(ArchiveRouteType),
)
@click.option("--tape-pool", "--tp", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, storage_class_name: str, number: int, type: ArchiveRouteType, tape_pool: str, comment: str):
    """Create a CTA archive route."""
    obj.cta_client.archive_route.create(storage_class_name, number, type, tape_pool_name=tape_pool, comment=comment)
    print(f"ArchiveRoute({storage_class_name}, {number}, {type}) CREATED")


@archive_route.command(aliases=["ch"])
@click.argument("storage_class_name")
@click.argument("number", type=click.INT)
@click.argument(
    "type",
    type=click.Choice([f.name for f in ArchiveRouteType], case_sensitive=False),
    callback=enum_choice(ArchiveRouteType),
)
@click.option("--tape-pool", "--tp")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    storage_class_name: str,
    number: int,
    type: ArchiveRouteType,
    tape_pool: Optional[str],
    comment: Optional[str],
):
    """Update a CTA archive route."""
    obj.cta_client.archive_route.update(storage_class_name, number, type, tape_pool_name=tape_pool, comment=comment)
    print(f"ArchiveRoute({storage_class_name}, {number}, {type}) UPDATED")


@archive_route.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List archive routes in cta catalogue."""
    print_dataclasses(obj.cta_client.archive_route.list(), display_config=obj.config.display)


@archive_route.command(aliases=["rm"])
@click.argument("storage_class_name")
@click.argument("number", type=click.INT)
@click.argument(
    "type",
    type=click.Choice([f.name for f in ArchiveRouteType], case_sensitive=False),
    callback=enum_choice(ArchiveRouteType),
)
@click.pass_obj
def delete(obj: AppContext, storage_class_name: str, number: int, type: ArchiveRouteType):
    """Remove a CTA archive route."""
    obj.cta_client.archive_route.delete(storage_class_name, number, type)
    print(f"ArchiveRoute({storage_class_name}, {number}, {type}) DELETED")
