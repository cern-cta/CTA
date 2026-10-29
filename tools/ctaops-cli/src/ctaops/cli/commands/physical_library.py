# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Optional

import click
from cta.sdk.models import PhysicalLibraryState
from rich import print

from ctaops.cli.click_extension.aliases import ClickAliasedGroup
from ctaops.cli.display.printers import print_dataclasses

from ..context import EXTRA_TPSRV, AppContext, tapeserver_option


@click.group(cls=ClickAliasedGroup, aliases=["pl"])
def physical_library():
    """Manage CTA Physical Libraries"""


@physical_library.command(aliases=["add"])
@click.argument("name")
@click.option("--manufacturer", required=True)
@click.option("--model", required=True)
@click.option("--comment", "-c", required=True)
@click.option("--nb-physical-cartridge-slots", "--npcs", required=True, type=click.INT)
@click.option("--nb-physical-drive-slots", "--npds", required=True, type=click.INT)
@click.pass_obj
def create(
    obj: AppContext,
    name: str,
    manufacturer: str,
    model: str,
    nb_physical_cartridge_slots: int,
    nb_physical_drive_slots: int,
    comment: str,
):
    """Create CTA physical library"""
    obj.cta_client.physical_library.create(
        name,
        manufacturer=manufacturer,
        model=model,
        comment=comment,
        physical_cartridge_slots=nb_physical_cartridge_slots,
        physical_drive_slots=nb_physical_drive_slots,
    )
    print(f"PhysicalLibrary({name}) CREATED")


@physical_library.command(aliases=["ch"])
@click.argument("name")
@click.option("--gui-url", "--gurl")
@click.option("--webcam-url", "--wurl")
@click.option("--location", "-l")
@click.option("--nb-physical-cartridge-slots", "--npcs", type=click.INT)
@click.option("--nb-available-cartridge-slots", "--nacs", type=click.INT)
@click.option("--nb-physical-drive-slots", "--npds", type=click.INT)
@click.option("--nb-available-drive-slots", "--nads", type=click.INT)
@click.option("--comment", "-c")
@click.pass_obj
def update(
    obj: AppContext,
    name: str,
    gui_url: Optional[str],
    webcam_url: Optional[str],
    location: Optional[str],
    nb_physical_cartridge_slots: Optional[int],
    nb_available_cartridge_slots: Optional[int],
    nb_physical_drive_slots: Optional[int],
    comment: Optional[str],
):
    """Update CTA physical library"""
    obj.cta_client.physical_library.update(
        name,
        gui_url=gui_url,
        webcam_url=webcam_url,
        location=location,
        physical_cartridge_slots=nb_physical_cartridge_slots,
        physical_drive_slots=nb_physical_drive_slots,
        available_cartridge_slots=nb_available_cartridge_slots,
        comment=comment,
    )
    print(f"PhysicalLibrary({name}) UPDATED")


@physical_library.command()
@click.argument("name")
@click.option("--reason", "-r", required=True)
@click.pass_obj
def disable(obj: AppContext, name: str, reason: str):
    """Disable a CTA physical library"""
    obj.cta_client.physical_library.disable(name, reason=reason)
    print(f"PhysicalLibrary({name}) DISABLED")


@physical_library.command()
@click.argument("name")
@click.pass_obj
def enable(obj: AppContext, name: str):
    """Enable a CTA physical library"""
    obj.cta_client.physical_library.enable(name)
    print(f"PhysicalLibrary({name}) ENABLED")


@physical_library.command("list", aliases=["ls"])
@click.option(
    "--disabled/--enabled",
    "-d/-e",
    is_flag=True,
    default=None,
    show_default=True,
    help="Filter logical libraries by state",
)
@tapeserver_option("-l", "--local", is_flag=True, help="Show local information")
@click.pass_obj
def list_command(obj: AppContext, disabled: Optional[bool], local: Optional[bool] = None):
    """List physical libraries registered in CTA"""

    state = None if disabled is None else (PhysicalLibraryState.DISABLED if disabled else PhysicalLibraryState.ENABLED)

    if EXTRA_TPSRV:
        from ctaops.sdk import LocalLibraryRepository, LocalNamedLibraryRepository

        from ctaops.cli.models.physical_library import EnrichedPhysicalLibrary

        local_named_libraries = LocalNamedLibraryRepository(
            library_name_key=obj.config.tape_server.udev_properties_map.library_name_key
        ).list_libraries()
        print_dataclasses(
            (
                EnrichedPhysicalLibrary.from_physical_library(
                    pl,
                    serial_number=(
                        sn := next((lnl.serial_number for lnl in local_named_libraries if lnl.name == pl.name), None)
                    ),
                    is_connected=bool(sn),
                )
                for pl in obj.cta_client.physical_library.list(state=state)
                if (
                    not local
                    or next((lnl.serial_number for lnl in local_named_libraries if lnl.name == pl.name), None)
                    is not None
                )
            ),
            display_config=obj.config.display,
        )

        if local:
            cta_lib_names = set(pl.name for pl in obj.cta_client.physical_library.list())
            if cta_unknown_named_libs := set(lnl for lnl in local_named_libraries if lnl.name not in cta_lib_names):
                print_dataclasses(
                    cta_unknown_named_libs, display_config=obj.config.display, title="Non-CTA Libraries (Named)"
                )

            if others_libraries := (LocalLibraryRepository().list_libraries() - local_named_libraries):
                print_dataclasses(others_libraries, display_config=obj.config.display, title="Non-CTA Libraries")
    else:
        print_dataclasses(obj.cta_client.physical_library.list(state=state), display_config=obj.config.display)


@physical_library.command()
@click.argument("name")
@click.pass_obj
def get(obj: AppContext, name: str):
    """Get physical library registered in CTA"""
    library = obj.cta_client.physical_library.get(name)

    if EXTRA_TPSRV:
        from ctaops.sdk import LocalNamedLibraryRepository, ResourceNotFoundError

        from ctaops.cli.models.physical_library import EnrichedPhysicalLibrary

        try:
            local_library = LocalNamedLibraryRepository(
                library_name_key=obj.config.tape_server.udev_properties_map.library_name_key
            ).get_library_by_name(name)
        except ResourceNotFoundError:
            library = EnrichedPhysicalLibrary.from_physical_library(library, serial_number=None, is_connected=False)
        else:
            library = EnrichedPhysicalLibrary.from_physical_library(
                library, serial_number=local_library.serial_number, is_connected=True
            )

    print_dataclasses(library, display_config=obj.config.display)


@physical_library.command(aliases=["rm"])
@click.argument("name")
@click.pass_obj
def delete(obj: AppContext, name: str):
    """Remove a CTA physical library"""
    obj.cta_client.physical_library.delete(name)
    print(f"PhysicalLibrary({name}) DELETED")
