# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import DurationType
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["dis"])
def disk_instance_space():
    """Manage CTA disk instance spaces"""


@disk_instance_space.command(aliases=["add"])
@click.argument("name")
@click.argument("disk_instance_name")
@click.option("--free-space-query-url", "--url", required=True)
@click.option(
    "--refresh-interval",
    "--ri",
    required=True,
    type=DurationType(),
)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(
    obj: AppContext,
    name: str,
    disk_instance_name: str,
    free_space_query_url: str,
    refresh_interval: timedelta,
    comment: str,
):
    """Create a CTA disk instance space"""
    obj.cta_client.disk_instance_space.create(
        name,
        disk_instance_name,
        comment=comment,
        refresh_interval=refresh_interval,
        free_space_url=free_space_query_url,
    )
    print(f"DiskInstanceSpace({name}, {disk_instance_name}) CREATED")


@disk_instance_space.command(aliases=["ch"])
@click.argument("name")
@click.argument("disk_instance_name")
@click.option("--free-space-query-url", "--url")
@click.option("--refresh-interval", "--ri", type=DurationType())
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    disk_instance_name: str,
    free_space_query_url: Optional[str],
    refresh_interval: Optional[timedelta],
    comment: Optional[str],
):
    """Update CTA disk instance space"""
    obj.cta_client.disk_instance_space.update(
        name,
        disk_instance_name,
        comment=comment,
        free_space_url=free_space_query_url,
        refresh_interval=refresh_interval,
    )
    print(f"DiskInstanceSpace({name}, {disk_instance_name}) UPDATED")


@disk_instance_space.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List disk instances spaces"""
    print_dataclasses(obj.cta_client.disk_instance_space.list(), display_config=obj.config.display)


@disk_instance_space.command()
@click.argument("name")
@click.argument("disk_instance_name")
@click.pass_obj
def get(obj: AppContext, name: str, disk_instance_name: str):
    """Get disk instance space registered in CTA"""
    print_dataclasses(
        obj.cta_client.disk_instance_space.get(name, disk_instance_name), display_config=obj.config.display
    )


@disk_instance_space.command(aliases=["rm"])
@click.argument("name")
@click.argument("disk_instance_name")
@click.pass_obj
def delete(obj: AppContext, name, disk_instance_name):
    """Remove a CTA disk instance space"""
    obj.cta_client.disk_instance_space.delete(name, disk_instance_name)
    print(f"DiskInstanceSpace({name}, {disk_instance_name}) DELETED")
