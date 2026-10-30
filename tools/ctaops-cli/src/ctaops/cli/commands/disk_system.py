# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from datetime import timedelta
from typing import Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.click_extension.custom_types import BytesType, DurationType
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["ds"])
def disk_system():
    """Manage CTA disk systems."""


@disk_system.command(aliases=["add"])
@click.argument("name")
@click.option("--file-regex", "--fr", required=True)
@click.option("--disk-instance", "--di", required=True)
@click.option("--disk-instance-space", "--dis", required=True)
@click.option("--targeted-free-space", "--tfs", type=BytesType(), required=True)
@click.option("--sleep-time", "--st", type=DurationType(), required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(
    obj: AppContext,
    name: str,
    file_regex: str,
    disk_instance: str,
    disk_instance_space: str,
    targeted_free_space: int,
    sleep_time: timedelta,
    comment: str,
):
    """Create a CTA disk system."""
    obj.cta_client.disk_system.create(
        name,
        disk_instance_name=disk_instance,
        disk_instance_space_name=disk_instance_space,
        comment=comment,
        desired_min_free_space=targeted_free_space,
        sleep_time=sleep_time,
        file_regex=file_regex,
    )
    print(f"DiskSystem({name}) CREATED")


@disk_system.command(aliases=["ch"])
@click.argument("name")
@click.option("--file-regex", "--fr")
@click.option("--targeted-free-space", "--tfs", type=BytesType())
@click.option("--sleep-time", "--st", type=DurationType())
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    file_regex: Optional[str],
    targeted_free_space: Optional[int],
    sleep_time: Optional[timedelta],
    comment: Optional[str],
):
    """Update CTA disk disk system."""
    obj.cta_client.disk_system.update(
        name, file_regex=file_regex, desired_min_free_space=targeted_free_space, sleep_time=sleep_time, comment=comment
    )
    print(f"DiskSystem({name}) UPDATED")


@disk_system.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List disk system."""
    print_dataclasses(obj.cta_client.disk_system.list(), display_config=obj.config.display)


@disk_system.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get disk system registered in CTA."""
    print_dataclasses(obj.cta_client.disk_system.get(name), display_config=obj.config.display)


@disk_system.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA disk system."""
    obj.cta_client.disk_system.delete(name)
    print(f"DiskSystem({name}) DELETED")
