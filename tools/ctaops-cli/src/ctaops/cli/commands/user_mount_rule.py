# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["umr"])
def user_mount_rule():
    """Manage Mount Policy to username mappings"""


@user_mount_rule.command(aliases=["add"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.option("--mount-policy", "--mp", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, user_name: str, disk_instance_name: str, mount_policy: str, comment: str):
    """Create CTA requester mount rule"""
    obj.cta_client.user_mount_rule.create(
        user_name, disk_instance_name, comment=comment, mount_policy_name=mount_policy
    )
    print(f"UserMountRule({user_name}) CREATED")


@user_mount_rule.command(aliases=["ch"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.option("--mount-policy", "--mp")
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext, user_name: str, disk_instance_name: str, mount_policy: Optional[str], comment: Optional[str]
):
    """Update CTA requester mount rule"""
    obj.cta_client.user_mount_rule.update(
        user_name, disk_instance_name, comment=comment, mount_policy_name=mount_policy
    )
    print(f"UserMountRule({user_name}) UPDATED")


@user_mount_rule.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List requester mount rules registered in CTA"""
    data = obj.cta_client.user_mount_rule.list()
    print_dataclasses(data, display_config=obj.config.display)


@user_mount_rule.command()
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.pass_obj
def get(obj: AppContext, user_name: str, disk_instance_name: str):
    """Get requester mount rule registered in CTA"""
    print_dataclasses(
        obj.cta_client.user_mount_rule.get(user_name, disk_instance_name), display_config=obj.config.display
    )


@user_mount_rule.command(aliases=["rm"])
@click.argument("user_name")
@click.argument("disk_instance_name")
@click.pass_obj
def delete(obj: AppContext, user_name: str, disk_instance_name: str):
    """Remove a CTA requester mount rule"""
    obj.cta_client.user_mount_rule.delete(user_name, disk_instance_name)
    print(f"UserMountRule({user_name}) DELETED")
