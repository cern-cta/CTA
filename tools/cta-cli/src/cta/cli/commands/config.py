# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import replace

import click
from cta.cli.click_extension.aliases import ClickAliasedGroup
from cta.cli.context import AppContext
from cta.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["cf"])
def config():
    """Manage tool configuration."""


@config.command()
@click.pass_obj
def view(obj: AppContext):
    """View a CTA archive route."""
    print_dataclasses(obj.config, replace(obj.config.display, pretty_table=True))
