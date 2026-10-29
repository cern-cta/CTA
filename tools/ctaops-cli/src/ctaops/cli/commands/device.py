# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from logging import getLogger
from typing import Literal

import click
from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.context import EXTRA_TPSRV, AppContext
from ctaops.cli.display.printers import print_dataclasses

LOGGER = getLogger("ctaops.cli")

if EXTRA_TPSRV:

    @click.group(cls=ClickAliasedGroup, aliases=["de"])
    def device():
        """Integration with ctaops-udev package to discover CTA hardware resources present."""

    @device.command("list", aliases=["ls"])
    @click.argument("type", type=click.Choice(["drives", "libraries"]))
    @click.pass_obj
    def list_command(obj: AppContext, type: Literal["drives", "libraries"]):
        """List device present locally."""
        from ctaops.sdk.repositories.local.drive import LocalLocatedNamedDriveRepository
        from ctaops.sdk.repositories.local.library import LocalNamedLibraryRepository

        if type == "libraries":
            named_lib_repo = LocalNamedLibraryRepository()
            print_dataclasses(named_lib_repo.list_libraries(), display_config=obj.config.display)

        elif type == "drives":
            named_located_drive_repo = LocalLocatedNamedDriveRepository()
            print_dataclasses(named_located_drive_repo.list_drives(), display_config=obj.config.display)
