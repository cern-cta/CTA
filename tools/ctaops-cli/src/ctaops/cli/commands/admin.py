# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["ad"])
def admin():
    """Manage CTA administrator accounts."""


@admin.command(aliases=["add"])
@click.argument("name")
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, comment: str):
    """Create CTA admin user."""
    obj.cta_client.admin_user.create(name, comment=comment)
    print(f"AdminUser({name}) CREATED")


@admin.command(aliases=["ch"])
@click.argument("name")
@click.option("--comment", "-c")
@click.pass_obj
def update(obj: AppContext, name: str, comment: Optional[str]):
    """Change CTA admin user."""
    obj.cta_client.admin_user.update(name, comment=comment)
    print(f"AdminUser({name}) UPDATED")


@admin.command("list", aliases=["ls"])
@click.pass_obj
def list_command(obj: AppContext):
    """List admin users registered in CTA."""
    print_dataclasses(obj.cta_client.admin_user.list(), display_config=obj.config.display)


@admin.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get admin user registered in CTA."""
    print_dataclasses(obj.cta_client.admin_user.get(name), display_config=obj.config.display)


@admin.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA admin user."""
    obj.cta_client.admin_user.delete(name)
    print(f"AdminUser({name}) DELETED")
