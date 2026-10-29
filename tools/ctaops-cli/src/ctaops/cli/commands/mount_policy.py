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


@click.group(cls=ClickAliasedGroup, aliases=["mp"])
def mount_policy():
    """Manage CTA Mount Policies"""


@mount_policy.command(aliases=["add"])
@click.argument("name")
@click.option("--archive-priority", "--ap", required=True, type=click.INT)
@click.option("--min-archive-request-age", "--mara", required=True, type=DurationType())
@click.option("--retrieve-priority", "--rp", required=True, type=click.INT)
@click.option("--min-retrieve-request-age", "--mrra", required=True, type=DurationType())
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(
    obj: AppContext,
    name: str,
    archive_priority: int,
    min_archive_request_age: timedelta,
    retrieve_priority: int,
    min_retrieve_request_age: timedelta,
    comment: str,
):
    """Create CTA mount policy"""
    obj.cta_client.mount_policy.create(
        name,
        archive_priority=archive_priority,
        min_archive_age=min_archive_request_age,
        retrieve_priority=retrieve_priority,
        min_retrieve_age=min_retrieve_request_age,
        comment=comment,
    )
    print(f"MountPolicy({name}) CREATED")


@mount_policy.command(aliases=["ch"])
@click.argument("name")
@click.option("--archive-priority", "--ap", type=click.INT)
@click.option("--min-archive-request-age", "--mara", type=DurationType())
@click.option("--retrieve-priority", "--rp", type=click.INT)
@click.option("--min-retrieve-request-age", "--mrra", type=DurationType())
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    archive_priority: Optional[int],
    min_archive_request_age: Optional[timedelta],
    retrieve_priority: Optional[int],
    min_retrieve_request_age: Optional[timedelta],
    comment: Optional[str],
):
    """Update CTA mount policy"""
    obj.cta_client.mount_policy.update(
        name,
        archive_priority=archive_priority,
        min_archive_age=min_archive_request_age,
        retrieve_priority=retrieve_priority,
        min_retrieve_age=min_retrieve_request_age,
        comment=comment,
    )
    print(f"MountPolicy({name}) UPDATED")


@mount_policy.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List mount policies registered in CTA"""
    print_dataclasses(obj.cta_client.mount_policy.list(), display_config=obj.config.display)


@mount_policy.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get mount policy registered in CTA"""
    print_dataclasses(obj.cta_client.mount_policy.get(name), display_config=obj.config.display)


@mount_policy.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove CTA mount policy"""
    obj.cta_client.mount_policy.delete(name)
    print(f"MountPolicy({name}) DELETED")
