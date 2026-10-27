# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["gmr"])
def group_mount_rule():
    """Manage CTA group mount rules"""


@group_mount_rule.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List CTA group mount rules"""
    data = obj.cta_client.group_mount_rule.list()
    print_dataclasses(data, display_config=obj.config.display)


@group_mount_rule.command()
@click.argument("group_name")
@click.argument("disk_instance_name")
@click.pass_obj
def get(obj: AppContext, group_name: str, disk_instance_name: str):
    """Get group mount rule registered in CTA"""
    print_dataclasses(
        obj.cta_client.group_mount_rule.get(group_name, disk_instance_name), display_config=obj.config.display
    )


@group_mount_rule.command(aliases=["add"])
@click.argument("group_name")
@click.argument("disk_instance_name")
@click.option("--mount-policy", "--mp", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, group_name: str, disk_instance_name: str, mount_policy: str, comment: str):
    """Create a CTA group mount rule"""
    obj.cta_client.group_mount_rule.create(
        group_name, disk_instance_name, comment=comment, mount_policy_name=mount_policy
    )
    print(f"GroupMountRule({group_name}, {disk_instance_name}) CREATED")


@group_mount_rule.command(aliases=["ch"])
@click.argument("group_name")
@click.argument("disk_instance_name")
@click.option("--mount-policy", "--mp")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext, group_name: str, disk_instance_name: str, mount_policy: Optional[str], comment: Optional[str]
):
    """Update a CTA group mount rule"""
    obj.cta_client.group_mount_rule.update(
        group_name, disk_instance_name, comment=comment, mount_policy_name=mount_policy
    )
    print(f"GroupMountRule({group_name}, {disk_instance_name}) UPDATED")


@group_mount_rule.command(aliases=["rm"])
@click.argument("group_name")
@click.argument("disk_instance_name")
@click.pass_obj
def delete(obj: AppContext, group_name: str, disk_instance_name: str):
    """Remove a CTA group mount rule"""
    obj.cta_client.group_mount_rule.delete(group_name, disk_instance_name)
    print(f"GroupMountRule({group_name}, {disk_instance_name}) DELETED")
