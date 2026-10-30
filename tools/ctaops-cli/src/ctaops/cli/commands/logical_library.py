# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from cta.sdk.models import LogicalLibraryState
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses
from rich import print


@click.group(cls=ClickAliasedGroup, aliases=["ll"])
def logical_library():
    """Manage CTA Logical Libraries."""


@logical_library.command(aliases=["add"])
@click.argument("name")
@click.option("--physical-library", "--pl", required=True)
@click.option("--comment", "-c", required=True)
@click.pass_obj
def create(obj: AppContext, name: str, physical_library: str, comment: str):
    """Create CTA logical library."""
    obj.cta_client.logical_library.create(name, physical_library_name=physical_library, comment=comment)
    print(f"LogicalLibrary({name}) CREATED")


@logical_library.command(aliases=["ch"])
@click.argument("name")
@click.option("--physical-library", "--pl")
@click.option("--comment", "-c")
@click.pass_obj
def update(obj: AppContext, name: str, physical_library: Optional[str], comment: Optional[str]):
    """Update CTA logical library."""
    obj.cta_client.logical_library.update(name, comment=comment, physical_library_name=physical_library)
    print(f"LogicalLibrary({name}) UPDATED")


@logical_library.command()
@click.argument("name")
@click.option("--reason", "-r", required=True)
@click.pass_obj
def disable(obj: AppContext, name: str, reason: str):
    """Disable a CTA logical library."""
    obj.cta_client.logical_library.disable(name, reason=reason)
    print(f"LogicalLibrary({name}) DISABLED")


@logical_library.command()
@click.argument("name")
@click.pass_obj
def enable(obj: AppContext, name: str):
    """Enable a CTA logical library."""
    obj.cta_client.logical_library.enable(name)
    print(f"LogicalLibrary({name}) ENABLED")


@logical_library.command("list", aliases=["ls"])
@click.option(
    "--disabled/--enabled",
    "-d/-e",
    is_flag=True,
    default=None,
    show_default=True,
    help="Filter logical libraries by state",
)
@click.pass_obj
def list_command(obj: AppContext, disabled: Optional[bool]):
    """List logical libraries registered in CTA."""
    state = None if disabled is None else (LogicalLibraryState.DISABLED if disabled else LogicalLibraryState.ENABLED)
    print_dataclasses(obj.cta_client.logical_library.list(state=state), display_config=obj.config.display)


@logical_library.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get logical library registered in CTA."""
    print_dataclasses(obj.cta_client.logical_library.get(name), display_config=obj.config.display)


@logical_library.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA logical library."""
    obj.cta_client.logical_library.delete(name)
    print(f"LogicalLibrary({name}) DELETED")
