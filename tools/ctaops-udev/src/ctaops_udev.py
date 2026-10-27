#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import logging
import os
import re
from configparser import ConfigParser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Protocol
from xml.dom import minidom

logging.basicConfig(format="%(asctime)s %(process)d %(name)s %(levelname)s: %(message)s", level=logging.WARNING)

LOGGER = logging.getLogger()


@dataclass
class LibraryDriveInventoryEntry:
    """Represents information about a tape drive within a library.

    Attributes:
        sn: The serial number of the tape drive.
        address: The address of the drive within the library.
        ordinal: The ordinal position of the drive within the library.
        library: The name of the library the drive belongs to.
        location: The physical location of the drive within the library (e.g., frame, column, row).
        tech: The technology type of the drive (e.g., LTO8, LTO9).
    """

    serial_number: str
    address: int
    ordinal: int
    library: str
    location: str
    tech: str


TechModelDriveMapping = dict[str, list[str]]

_DEFAULT_TECH_MODEL_DRIVE_MAPPING: TechModelDriveMapping = {
    "LTO8": ["3588-F8C", "IBM Ultrium-TD8 Fibre"],
    "LTO9": ["3588-F9C", "IBM Ultrium-TD9 Full Height Fibre", "IBM ULTRIUM-TD9 Full Height Fibre"],
    "LTO10": ["3588-FAC", "IBM Ultrium-TDA Full Height Fibre", "IBM ULTRIUM-TDA Full Height Fibre"],
    "TS1155": ["3592-55F"],
    "TS1160": ["3592-60F"],
    "TS1170": ["3592-70F"],
}


@dataclass
class Config:
    library_config_dir: Path = Path("/etc/cta-ops/library_drive_config")
    tech_model_drive_map: TechModelDriveMapping = field(default_factory=lambda: _DEFAULT_TECH_MODEL_DRIVE_MAPPING)
    vendor_map_suffix: dict[str, str] = field(default_factory=lambda: {"SPECTRA": ".xml", "IBM": ".json"})
    exclude_vendor_logical_library: list[str] = field(default_factory=lambda: ["BACKUP"])

    def load_from_file(self, file: Path) -> "Config":
        configuration_parser = ConfigParser()
        configuration_parser.optionxform = str  # type: ignore[assignment]
        if not configuration_parser.read(file):
            return self
        self.library_config_dir = Path(
            configuration_parser.get("global", "library_config_dir", fallback=self.library_config_dir)
        )

        if configuration_parser.has_section("tech_model_drive_map"):
            self.tech_model_drive_map = {
                tech: [model.strip() for model in models.split(",")]
                for tech, models in configuration_parser.items("tech_model_drive_map")
            }

        if configuration_parser.has_section("vendor_map_suffix"):
            self.vendor_map_suffix = {
                vendor: suffix.strip() for vendor, suffix in configuration_parser.items("vendor_map_suffix")
            }

        if vendor_exclude := configuration_parser.get("global", "exclude_vendor_logical_library", fallback=""):
            self.exclude_vendor_logical_library = [lib.strip() for lib in vendor_exclude.split(",")]

        return self

    def validate(self) -> "Config":
        if not self.library_config_dir.is_dir():
            raise ValueError(
                f"Library configuration directory {self.library_config_dir} does not exist or is not a directory"
            )
        return self


class Parser(Protocol):
    def parse(self, file: Path) -> list[LibraryDriveInventoryEntry]: ...


class IBMInventoryParser:
    def __init__(self, tech_model_drive_map: Optional[TechModelDriveMapping] = None) -> None:
        self.tech_model_drive_map = (
            _DEFAULT_TECH_MODEL_DRIVE_MAPPING if tech_model_drive_map is None else tech_model_drive_map
        )

    def parse(self, file: Path) -> list[LibraryDriveInventoryEntry]:
        """Read tape drive information from a JSON library configuration file.

        This function is used for IBM 3584 tape libraries. The element addresses
        in the JSON file are sequential starting from 257, so the ordinal is
        calculated by subtracting 257 from the elementAddress.

        Args:
            file (Path): Path to the JSON configuration file.

        Returns:
            list[LibraryDriveInventoryEntry]: List of cached drive information dictionaries,
                each containing serial number and ordinal position.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            json.JSONDecodeError: If the JSON file is malformed.
            KeyError: If required fields (sn, elementAddress) are missing.
        """

        def drive_elt_to_dict(elt: dict) -> LibraryDriveInventoryEntry:
            # As observed on IBM tape libraries at CERN, the drive element
            # addresses are sequential and start from 257 so we will use that
            # value for substraction to calculate the drive ordinal number
            # (this assumption saves a call to the SCSI media changer
            # (implemented above))
            # [root@tpsrvXXX ~]# cta-smc -q D
            # Drive Ordinal   Element Addr.     Status     Vid
            #             0             257       free
            #             1             258       free
            #             2             259       free
            #             3             260       free

            location = re.match(r"drive_F(\d+)C(\d+)R(\d+)", elt["location"])
            if not location:
                raise TypeError(f"Drive element location {elt['location']} does not match expected format")

            return LibraryDriveInventoryEntry(
                serial_number=elt["sn"],
                address=int(elt["elementAddress"]),
                ordinal=int(elt["elementAddress"]) - 257,
                library=elt["logicalLibrary"],
                location=f"F{location.group(1).zfill(2)}C{location.group(2)}R{location.group(3)}",
                tech=next(
                    tech for tech, models in self.tech_model_drive_map.items() for m in models if m == elt["mtm"]
                ),
            )

        with open(file, "r", encoding="utf-8") as f:
            lib_drives: list[dict] = json.load(f)
            return [drive_elt_to_dict(drive) for drive in lib_drives]


class SpectraInventoryParser:
    def __init__(self, tech_model_drive_map: Optional[TechModelDriveMapping] = None) -> None:
        self.tech_model_drive_map = (
            _DEFAULT_TECH_MODEL_DRIVE_MAPPING if tech_model_drive_map is None else tech_model_drive_map
        )

    def parse(self, file: Path) -> list[LibraryDriveInventoryEntry]:
        """Read tape drive information from an XML library configuration file.

        This function is used for Spectra Python tape libraries. It parses the
        XML file to extract drive serial numbers and partition drive numbers,
        calculating ordinal by subtracting 1 from the partition drive number.

        Args:
            file (Path): Path to the XML configuration file.

        Returns:
            list[LibraryDriveInventoryEntry]: List of cached drive information dictionaries,
                each containing serial number and ordinal position.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            xml.parsers.expat.ExpatError: If the XML file is malformed.
            AttributeError: If required XML elements are missing.
        """

        def drive_elt_to_dict(elt) -> LibraryDriveInventoryEntry:
            location = re.match(r"FR(\d+)\/DBA(\d+)\/fLTO-DRV(\d+)", elt.getElementsByTagName("ID")[0].firstChild.data)
            if not location:
                raise TypeError()

            drive_type = elt.getElementsByTagName("driveType")[0].firstChild.data

            return LibraryDriveInventoryEntry(
                serial_number=elt.getElementsByTagName("serialNumber")[0].firstChild.data,
                address=int(elt.getElementsByTagName("partitionDriveNumber")[0].firstChild.data) + 255,
                ordinal=int(elt.getElementsByTagName("partitionDriveNumber")[0].firstChild.data) - 1,
                library=elt.getElementsByTagName("partition")[0].firstChild.data,
                location=f"F{location.group(1).zfill(2)}B{location.group(2)}S{location.group(3)}",
                tech=next(
                    tech for tech, models in self.tech_model_drive_map.items() for m in models if m == drive_type
                ),
            )

        with open(file, "r", encoding="utf-8") as f:
            lib_drives = minidom.parse(f).getElementsByTagName("drive")
            return [drive_elt_to_dict(drive) for drive in lib_drives]


def get_parser_from_file_suffix(
    suffix: str,
    tech_model_drive_map: Optional[TechModelDriveMapping] = None,
) -> Parser:
    if suffix == ".json":
        return IBMInventoryParser(tech_model_drive_map)
    elif suffix == ".xml":
        return SpectraInventoryParser(tech_model_drive_map)
    else:
        raise ValueError(f"{suffix} suffix unknown")


def get_parser_from_vendor(
    vendor: str,
    tech_model_drive_map: Optional[TechModelDriveMapping] = None,
) -> Parser:
    if vendor == "SPECTRA":
        return SpectraInventoryParser(tech_model_drive_map)
    elif vendor == "IBM":
        return IBMInventoryParser(tech_model_drive_map)
    else:
        raise ValueError("Library vendor unknown by this CTA OPS tool")


class UdevPropertiesGenerator(Protocol):
    def generate_udev_properties(self) -> str: ...


class MediaChangerPropertiesGenerator:
    def __init__(self, conf: Config) -> None:
        self.conf = conf

    @property
    def _vendor(self) -> str:
        return os.environ["SCSI_VENDOR"]

    @property
    def _cache_path(self) -> Path:
        return (
            self.conf.library_config_dir
            / f"{self._vendor}-{os.environ['SCSI_MODEL']}-{os.environ['ID_SCSI_SERIAL']}{self.conf.vendor_map_suffix[self._vendor]}"
        )

    def get_name(self) -> str:
        drive_inventory = get_parser_from_vendor(self._vendor, self.conf.tech_model_drive_map).parse(self._cache_path)
        cta_physical_library_name = next(
            d.library for d in drive_inventory if d.library not in self.conf.exclude_vendor_logical_library
        )
        return cta_physical_library_name

    def generate_udev_properties(self) -> str:
        return f"CTAOPS_NAME={self.get_name()}"


class DrivePropertiesGenerator:
    def __init__(self, conf: Config) -> None:
        self.conf = conf

    def get_drive_inventory_entry(self) -> LibraryDriveInventoryEntry:
        drive_generator = (
            d
            for p in Path(self.conf.library_config_dir).iterdir()
            if p.is_file() and p.suffix in [".json", ".xml"]
            for d in get_parser_from_file_suffix(p.suffix, self.conf.tech_model_drive_map).parse(p)
        )

        return next(d for d in drive_generator if d.serial_number == os.environ["ID_SCSI_SERIAL"].lstrip("0"))

    def generate_udev_properties(self) -> str:
        drive_inventory_entry = self.get_drive_inventory_entry()
        return f"CTAOPS_NAME={drive_inventory_entry.library}-{drive_inventory_entry.tech}-{drive_inventory_entry.location}\nCTAOPS_LIBRARY_ADDRESS={drive_inventory_entry.address}\nCTAOPS_LIBRARY_ORDINAL={drive_inventory_entry.ordinal}"


def get_udev_properties_generator(conf: Config) -> UdevPropertiesGenerator:
    if os.environ["SCSI_TYPE"] == "medium changer":
        return MediaChangerPropertiesGenerator(conf)
    elif os.environ["SUBSYSTEM"] == "scsi_tape" and os.path.basename(os.environ["DEVPATH"]).startswith("nst"):
        return DrivePropertiesGenerator(conf)
    else:
        raise ValueError(f"Unknown Device {os.environ['DEVPATH']}")


def main():
    conf = Config().load_from_file(Path("/etc/udev/ctaops-udev.ini")).validate()
    udev_generator = get_udev_properties_generator(conf)
    print(udev_generator.generate_udev_properties())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOGGER.exception("Uncaught exception", exc_info=True)
        raise
