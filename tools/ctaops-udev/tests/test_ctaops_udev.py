import json
from pathlib import Path

import pytest
from ctaops_udev import (  # pyright: ignore[reportMissingImports]  # path is injected by conftest.py
    Config,
    DrivePropertiesGenerator,
    IBMDriveEntry,
    IBMInventoryParser,
    LibraryDriveInventoryEntry,
    MediaChangerPropertiesGenerator,
    SpectraInventoryParser,
    get_parser_from_file_suffix,
    get_parser_from_vendor,
    get_udev_properties_generator,
)

# ---------------------------------------------------------------------------
# IBMInventoryParser
# ---------------------------------------------------------------------------


def test_ibm_parser_returns_one_entry_per_drive(tmp_path: Path) -> None:
    payload = [
        {
            "sn": "DRV-1",
            "elementAddress": 257,
            "logicalLibrary": "T0-LIB0",
            "location": "drive_F2C3R4",
            "mtm": "3588-F9C",
        },
        {
            "sn": "DRV-2",
            "elementAddress": 260,
            "logicalLibrary": "T0-LIB0",
            "location": "drive_F2C3R5",
            "mtm": "IBM Ultrium-TD8 Fibre",
        },
    ]
    inv = tmp_path / "IBM-03584-1234.json"
    inv.write_text(json.dumps(payload))

    entries = IBMInventoryParser().parse(inv)

    assert entries == [
        LibraryDriveInventoryEntry(
            serial_number="DRV-1",
            address=257,
            ordinal=0,
            library="T0-LIB0",
            location="F02C3R4",
            tech="LTO9",
        ),
        LibraryDriveInventoryEntry(
            serial_number="DRV-2",
            address=260,
            ordinal=3,
            library="T0-LIB0",
            location="F02C3R5",
            tech="LTO8",
        ),
    ]


def test_ibm_parser_pads_single_digit_frame(tmp_path: Path) -> None:
    inv = tmp_path / "IBM-03584-1234.json"
    inv.write_text(
        json.dumps(
            [
                {
                    "sn": "DRV-1",
                    "elementAddress": 257,
                    "logicalLibrary": "L",
                    "location": "drive_F3C2R1",
                    "mtm": "3588-F9C",
                }
            ]
        )
    )

    [entry] = IBMInventoryParser().parse(inv)

    assert entry.location == "F03C2R1"


def test_ibm_parser_raises_when_location_does_not_match(tmp_path: Path) -> None:
    inv = tmp_path / "IBM-03584-1234.json"
    inv.write_text(
        json.dumps(
            [
                {
                    "sn": "DRV-1",
                    "elementAddress": 257,
                    "logicalLibrary": "L",
                    "location": "totally-bogus",
                    "mtm": "3588-F9C",
                }
            ]
        )
    )

    with pytest.raises(TypeError, match="does not match expected format"):
        IBMInventoryParser().parse(inv)


def test_ibm_parser_raises_when_model_is_unknown(tmp_path: Path) -> None:
    inv = tmp_path / "IBM-03584-1234.json"
    inv.write_text(
        json.dumps(
            [
                {
                    "sn": "DRV-1",
                    "elementAddress": 257,
                    "logicalLibrary": "L",
                    "location": "drive_F1C1R1",
                    "mtm": "UNKNOWN-MODEL",
                }
            ]
        )
    )

    with pytest.raises(StopIteration):
        IBMInventoryParser().parse(inv)


def test_ibm_parser_accepts_custom_tech_model_map(tmp_path: Path) -> None:
    inv = tmp_path / "IBM-03584-1234.json"
    inv.write_text(
        json.dumps(
            [
                {
                    "sn": "DRV-1",
                    "elementAddress": 257,
                    "logicalLibrary": "L",
                    "location": "drive_F1C1R1",
                    "mtm": "vendor-future",
                }
            ]
        )
    )

    [entry] = IBMInventoryParser({"LTOXX": ["vendor-future"]}).parse(inv)

    assert entry.tech == "LTOXX"


# ---------------------------------------------------------------------------
# SpectraInventoryParser
# ---------------------------------------------------------------------------


_SPECTRA_XML_TEMPLATE = """<?xml version='1.0' encoding='UTF-8'?>
<driveList>
  <drive>
    <ID>FR1/DBA2/fLTO-DRV3</ID>
    <serialNumber>DRV-S1</serialNumber>
    <driveType>3588-F9C</driveType>
    <partitionDriveNumber>1</partitionDriveNumber>
    <partition>T0-SPEC0</partition>
  </drive>
  <drive>
    <ID>FR3/DBA1/fLTO-DRV5</ID>
    <serialNumber>DRV-S2</serialNumber>
    <driveType>3588-F8C</driveType>
    <partitionDriveNumber>4</partitionDriveNumber>
    <partition>T0-SPEC0</partition>
  </drive>
</driveList>
"""


def test_spectra_parser_returns_one_entry_per_drive(tmp_path: Path) -> None:
    inv = tmp_path / "SPECTRA-PYTHON-abc.xml"
    inv.write_text(_SPECTRA_XML_TEMPLATE)

    entries = SpectraInventoryParser().parse(inv)

    assert entries == [
        LibraryDriveInventoryEntry(
            serial_number="DRV-S1",
            address=256,
            ordinal=0,
            library="T0-SPEC0",
            location="F01B2S3",
            tech="LTO9",
        ),
        LibraryDriveInventoryEntry(
            serial_number="DRV-S2",
            address=259,
            ordinal=3,
            library="T0-SPEC0",
            location="F03B1S5",
            tech="LTO8",
        ),
    ]


def test_spectra_parser_raises_on_unknown_drive_type(tmp_path: Path) -> None:
    inv = tmp_path / "SPECTRA-PYTHON-abc.xml"
    inv.write_text(
        """<?xml version='1.0'?>
<driveList>
  <drive>
    <ID>FR1/DBA1/fLTO-DRV1</ID>
    <serialNumber>DRV-S1</serialNumber>
    <driveType>not-in-map</driveType>
    <partitionDriveNumber>1</partitionDriveNumber>
    <partition>P</partition>
  </drive>
</driveList>
"""
    )

    with pytest.raises(StopIteration):
        SpectraInventoryParser().parse(inv)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_config_defaults_when_no_file_present(tmp_path: Path) -> None:
    cfg = Config().load_from_file(tmp_path / "missing.ini")

    assert cfg.library_config_dir == Path("/etc/cta-ops/library_drive_config")
    assert "LTO9" in cfg.tech_model_drive_map
    assert cfg.vendor_map_suffix == {"SPECTRA": ".xml", "IBM": ".json"}
    assert cfg.exclude_vendor_logical_library == ["BACKUP"]


def test_config_load_from_file_overrides_every_section(tmp_path: Path) -> None:
    library_dir = tmp_path / "library_drive_config"
    library_dir.mkdir()
    ini = tmp_path / "ctaops-udev.ini"
    ini.write_text(
        f"""
[global]
library_config_dir = {library_dir}
exclude_vendor_logical_library = SKIP1, SKIP2

[tech_model_drive_map]
LTOXX = model-a, model-b

[vendor_map_suffix]
IBM = .ibm.json
"""
    )

    cfg = Config().load_from_file(ini)

    assert cfg.library_config_dir == library_dir
    assert cfg.tech_model_drive_map == {"LTOXX": ["model-a", "model-b"]}
    assert cfg.vendor_map_suffix == {"IBM": ".ibm.json"}
    assert cfg.exclude_vendor_logical_library == ["SKIP1", "SKIP2"]


def test_config_validate_raises_when_library_dir_does_not_exist(tmp_path: Path) -> None:
    cfg = Config(library_config_dir=tmp_path / "absent")

    with pytest.raises(ValueError, match="does not exist or is not a directory"):
        cfg.validate()


def test_config_validate_returns_self_when_dir_exists(tmp_path: Path) -> None:
    cfg = Config(library_config_dir=tmp_path)
    assert cfg.validate() is cfg


# ---------------------------------------------------------------------------
# Parser factory helpers
# ---------------------------------------------------------------------------


def test_get_parser_from_vendor_picks_ibm() -> None:
    assert isinstance(get_parser_from_vendor("IBM"), IBMInventoryParser)


def test_get_parser_from_vendor_picks_spectra() -> None:
    assert isinstance(get_parser_from_vendor("SPECTRA"), SpectraInventoryParser)


def test_get_parser_from_vendor_rejects_unknown_vendor() -> None:
    with pytest.raises(ValueError, match="Library vendor unknown"):
        get_parser_from_vendor("OTHER")


def test_get_parser_from_vendor_forwards_tech_model_map() -> None:
    tech_map = {"LTOXX": ["vendor-future"]}
    parser = get_parser_from_vendor("IBM", tech_map)
    assert isinstance(parser, IBMInventoryParser)
    assert parser.tech_model_drive_map == tech_map


def test_get_parser_from_file_suffix_picks_ibm() -> None:
    assert isinstance(get_parser_from_file_suffix(".json"), IBMInventoryParser)


def test_get_parser_from_file_suffix_picks_spectra() -> None:
    assert isinstance(get_parser_from_file_suffix(".xml"), SpectraInventoryParser)


def test_get_parser_from_file_suffix_rejects_unknown_suffix() -> None:
    with pytest.raises(ValueError, match="suffix unknown"):
        get_parser_from_file_suffix(".txt")


# ---------------------------------------------------------------------------
# MediaChangerPropertiesGenerator
# ---------------------------------------------------------------------------


def _set_ibm_changer_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_VENDOR", "IBM")
    monkeypatch.setenv("SCSI_MODEL", "03584L22")
    monkeypatch.setenv("ID_SCSI_SERIAL", "0000078AA6480401")


def test_media_changer_generator_cache_path_uses_vendor_suffix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_ibm_changer_env(monkeypatch)
    generator = MediaChangerPropertiesGenerator(Config(library_config_dir=tmp_path))

    assert generator._cache_path == tmp_path / "IBM-03584L22-0000078AA6480401.json"


def test_media_changer_generator_cache_path_spectra_uses_xml_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SCSI_VENDOR", "SPECTRA")
    monkeypatch.setenv("SCSI_MODEL", "PYTHON")
    monkeypatch.setenv("ID_SCSI_SERIAL", "abc")
    generator = MediaChangerPropertiesGenerator(Config(library_config_dir=tmp_path))

    assert generator._cache_path == tmp_path / "SPECTRA-PYTHON-abc.xml"


def test_media_changer_generator_get_name_skips_excluded_library(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_ibm_changer_env(monkeypatch)
    (tmp_path / "IBM-03584L22-0000078AA6480401.json").write_text(
        json.dumps(
            [
                {
                    "sn": "D1",
                    "elementAddress": 257,
                    "logicalLibrary": "BACKUP",
                    "location": "drive_F1C1R1",
                    "mtm": "3588-F9C",
                },
                {
                    "sn": "D2",
                    "elementAddress": 258,
                    "logicalLibrary": "T0-LIB0",
                    "location": "drive_F1C1R2",
                    "mtm": "3588-F9C",
                },
            ]
        )
    )
    generator = MediaChangerPropertiesGenerator(
        Config(library_config_dir=tmp_path, exclude_vendor_logical_library=["BACKUP"])
    )

    assert generator.get_name() == "T0-LIB0"
    assert generator.generate_udev_properties() == "CTAOPS_NAME=T0-LIB0"


def test_media_changer_generator_raises_if_all_libraries_excluded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_ibm_changer_env(monkeypatch)
    (tmp_path / "IBM-03584L22-0000078AA6480401.json").write_text(
        json.dumps(
            [
                {
                    "sn": "D1",
                    "elementAddress": 257,
                    "logicalLibrary": "BACKUP",
                    "location": "drive_F1C1R1",
                    "mtm": "3588-F9C",
                }
            ]
        )
    )
    generator = MediaChangerPropertiesGenerator(
        Config(library_config_dir=tmp_path, exclude_vendor_logical_library=["BACKUP"])
    )

    with pytest.raises(StopIteration):
        generator.get_name()


def test_media_changer_generator_rejects_unknown_vendor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_VENDOR", "OTHER")
    monkeypatch.setenv("SCSI_MODEL", "X")
    monkeypatch.setenv("ID_SCSI_SERIAL", "y")
    generator = MediaChangerPropertiesGenerator(
        Config(library_config_dir=tmp_path, vendor_map_suffix={"OTHER": ".json"})
    )

    with pytest.raises(ValueError, match="Library vendor unknown"):
        generator.get_name()


# ---------------------------------------------------------------------------
# DrivePropertiesGenerator
# ---------------------------------------------------------------------------


def _write_ibm_inventory(directory: Path, entries: list[IBMDriveEntry], name: str = "IBM-03584-1234.json") -> None:
    (directory / name).write_text(json.dumps(entries))


def test_drive_generator_emits_three_properties(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_ibm_inventory(
        tmp_path,
        [
            {
                "sn": "DRV-1",
                "elementAddress": 259,
                "logicalLibrary": "T0-LIB0",
                "location": "drive_F1C1R3",
                "mtm": "3588-F9C",
            }
        ],
    )
    monkeypatch.setenv("ID_SCSI_SERIAL", "DRV-1")
    generator = DrivePropertiesGenerator(Config(library_config_dir=tmp_path))

    output = generator.generate_udev_properties()

    assert output.splitlines() == [
        "CTAOPS_NAME=T0-LIB0-LTO9-F01C1R3",
        "CTAOPS_LIBRARY_ADDRESS=259",
        "CTAOPS_LIBRARY_ORDINAL=2",
    ]


def test_drive_generator_strips_leading_zeros_from_drive_serial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_ibm_inventory(
        tmp_path,
        [
            {
                "sn": "DRV-1",
                "elementAddress": 257,
                "logicalLibrary": "T0-LIB0",
                "location": "drive_F1C1R1",
                "mtm": "3588-F9C",
            }
        ],
    )
    monkeypatch.setenv("ID_SCSI_SERIAL", "000DRV-1")
    generator = DrivePropertiesGenerator(Config(library_config_dir=tmp_path))

    entry = generator.get_drive_inventory_entry()

    assert entry.serial_number == "DRV-1"


def test_drive_generator_searches_across_files_and_ignores_other_suffixes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_ibm_inventory(
        tmp_path,
        [
            {
                "sn": "DRV-IBM",
                "elementAddress": 257,
                "logicalLibrary": "T0-LIB0",
                "location": "drive_F1C1R1",
                "mtm": "3588-F9C",
            }
        ],
    )
    (tmp_path / "SPECTRA-PYTHON-abc.xml").write_text(_SPECTRA_XML_TEMPLATE)
    # A file with an unrelated suffix must be skipped rather than parsed.
    (tmp_path / "README.txt").write_text("not an inventory file")
    monkeypatch.setenv("ID_SCSI_SERIAL", "DRV-S2")
    generator = DrivePropertiesGenerator(Config(library_config_dir=tmp_path))

    entry = generator.get_drive_inventory_entry()

    assert entry.serial_number == "DRV-S2"
    assert entry.library == "T0-SPEC0"


def test_drive_generator_raises_when_drive_not_in_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_ibm_inventory(
        tmp_path,
        [
            {
                "sn": "DRV-1",
                "elementAddress": 257,
                "logicalLibrary": "T0-LIB0",
                "location": "drive_F1C1R1",
                "mtm": "3588-F9C",
            }
        ],
    )
    monkeypatch.setenv("ID_SCSI_SERIAL", "ABSENT")
    generator = DrivePropertiesGenerator(Config(library_config_dir=tmp_path))

    with pytest.raises(StopIteration):
        generator.get_drive_inventory_entry()


# ---------------------------------------------------------------------------
# get_udev_properties_generator
# ---------------------------------------------------------------------------


def test_get_udev_generator_returns_media_changer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_TYPE", "medium changer")
    generator = get_udev_properties_generator(Config(library_config_dir=tmp_path))

    assert isinstance(generator, MediaChangerPropertiesGenerator)


def test_get_udev_generator_returns_drive_for_nst_tape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_TYPE", "tape")
    monkeypatch.setenv("SUBSYSTEM", "scsi_tape")
    monkeypatch.setenv("DEVPATH", "/devices/pci0000:00/host0/target0:0:0/nst0")
    generator = get_udev_properties_generator(Config(library_config_dir=tmp_path))

    assert isinstance(generator, DrivePropertiesGenerator)


def test_get_udev_generator_rejects_non_nst_tape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_TYPE", "tape")
    monkeypatch.setenv("SUBSYSTEM", "scsi_tape")
    monkeypatch.setenv("DEVPATH", "/devices/pci0000:00/host0/target0:0:0/st0")

    with pytest.raises(ValueError, match="Unknown Device"):
        get_udev_properties_generator(Config(library_config_dir=tmp_path))


def test_get_udev_generator_rejects_unknown_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCSI_TYPE", "disk")
    monkeypatch.setenv("SUBSYSTEM", "block")
    monkeypatch.setenv("DEVPATH", "/devices/pci0000:00/host0/target0:0:0/sda")

    with pytest.raises(ValueError, match="Unknown Device"):
        get_udev_properties_generator(Config(library_config_dir=tmp_path))
