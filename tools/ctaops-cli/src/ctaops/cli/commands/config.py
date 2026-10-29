# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import replace

import click

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import AppContext
from ctaops.cli.display.printers import print_dataclasses


@click.group(cls=ClickAliasedGroup, aliases=["cf"])
def config():
    """Manage tool configuration"""


@config.command()
@click.pass_obj
def view(obj: AppContext):
    """View a CTA archive route"""

    print_dataclasses(obj.config, replace(obj.config.display, pretty_get=True))
