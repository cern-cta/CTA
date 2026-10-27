# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["amr"])
def activity_mount_rule():
    """Manage CTA mount policy to username mappings"""


@activity_mount_rule.command(aliases=["add"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.argument("regex")
@click.option("--mount-policy", "--mp", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, user_name: str, disk_instance_name: str, regex: str, mount_policy: str, comment: str):
    """Add CTA activity mount rule"""
    obj.cta_client.activity_mount_rule.create(
        user_name, disk_instance_name, regex, comment=comment, mount_policy_name=mount_policy
    )
    print(f"ActivityMountRule({user_name}, {disk_instance_name}, {regex}) CREATED")


@activity_mount_rule.command(aliases=["ch"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.argument("regex")
@click.option("--mount-policy", "--mp")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    user_name: str,
    disk_instance_name: str,
    regex: str,
    mount_policy: Optional[str],
    comment: Optional[str],
):
    """Update CTA activity mount rule"""
    obj.cta_client.activity_mount_rule.update(
        user_name, disk_instance_name, regex, comment=comment, mount_policy_name=mount_policy
    )
    print(f"ActivityMountRule({user_name}, {disk_instance_name}, {regex}) UPDATED")


@activity_mount_rule.command("list", aliases=["ls"])
@click.pass_obj
def list_command(
    obj: AppContext,
):
    """List activity mount rules registered in CTA"""
    print_dataclasses(obj.cta_client.activity_mount_rule.list(), display_config=obj.config.display)


@activity_mount_rule.command()
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.argument("regex")
@click.pass_obj
def get(obj: AppContext, user_name: str, disk_instance_name: str, regex: str):
    """Get a CTA activity mount rule"""
    print_dataclasses(
        obj.cta_client.activity_mount_rule.get(user_name, disk_instance_name, regex), display_config=obj.config.display
    )


@activity_mount_rule.command(aliases=["rm"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.argument("regex")
@click.pass_obj
def delete(obj: AppContext, user_name: str, disk_instance_name: str, regex: str):
    """Remove a CTA activity mount rule"""
    obj.cta_client.activity_mount_rule.delete(user_name, disk_instance_name, regex)
    print(f"ActivityMountRule({user_name}, {disk_instance_name}, {regex}) DELETED")
