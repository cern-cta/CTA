# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import hashlib
import json
import shlex
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from system_tests.helpers.hosts import CtaCliHost, CtaRmcdHost, CtaTapedHost, EosClientHost

# =========================================================================
#  Helpers
# =========================================================================


@dataclass(frozen=True)
class ReadtpTapeFiles:
    # Tape location and expected payloads shared by the readtp tests
    vid: str
    fseqs: list[int]
    contents: dict[int, str]


@dataclass(frozen=True)
class ArchivedReadtpFile:
    # EOS path and original content before the catalogue assigns a tape fSeq
    path: Path
    content: str


@dataclass(frozen=True)
class ReadtpOutput:
    # Temporary output directory and the destination selected for each fSeq
    directory: Path
    files_by_fseq: dict[int, Path]


def smc_query(cta_rmcd: CtaRmcdHost, query_type: str, arguments: str = "") -> list[dict[str, Any]]:
    # Keep JSON parsing and the top-level response check consistent across SMC queries
    result = json.loads(cta_rmcd.exec_with_output(f"cta-smc -q {query_type} {arguments} --json"))
    assert isinstance(result, list)
    return result


def run_readtp(
    cta_taped: CtaTapedHost,
    tape_files: ReadtpTapeFiles,
    sequence: str,
    expected_fseqs: list[int],
) -> ReadtpOutput:
    # Give each requested fSeq its own destination so that payloads can be checked afterwards
    output_dir = Path(f"/tmp/readtp_output_{uuid.uuid4().hex}")
    destination_list = output_dir / "destinations"
    destinations = [output_dir / str(fseq) for fseq in expected_fseqs]
    destination_urls = "\n".join(f"file://{destination}" for destination in destinations) + "\n"
    cta_taped.exec(f"mkdir {output_dir}")
    try:
        # cta-readtp expects destination URLs in a separate file
        cta_taped.exec(f"printf %s {shlex.quote(destination_urls)} > {destination_list}")
        cta_taped.exec(
            f"cta-readtp {shlex.quote(tape_files.vid)} {shlex.quote(sequence)} --destination_files {destination_list}"
        )
    except Exception:
        # Do not leave partial output behind when cta-readtp fails
        cta_taped.exec(f"rm -rf {output_dir}")
        raise
    return ReadtpOutput(output_dir, dict(zip(expected_fseqs, destinations)))


def assert_readtp_output(cta_taped: CtaTapedHost, tape_files: ReadtpTapeFiles, output: ReadtpOutput) -> None:
    try:
        # Compare checksums because the taped image does not provide cmp
        for fseq, destination in output.files_by_fseq.items():
            expected_checksum = hashlib.sha256(tape_files.contents[fseq].encode()).hexdigest()
            actual_checksum = cta_taped.exec_with_output(f"sha256sum {destination}").split()[0]
            assert actual_checksum == expected_checksum
    finally:
        cta_taped.exec(f"rm -rf {output.directory}")


def find_tape_files(
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    disk_instance_name: str,
    archived_files: list[ArchivedReadtpFile],
) -> list[dict[str, Any]]:
    tape_files = []
    for archived_file in archived_files:
        # Resolve the EOS fxid before looking up its corresponding tape file
        file_info = json.loads(eos_client.file_info(disk_instance_name, archived_file.path, json_output=True))
        result = json.loads(
            cta_cli.exec_with_output(
                f"cta-admin --json tf ls --fxid {file_info['fxid']} -i {shlex.quote(disk_instance_name)}"
            )
        )
        assert len(result) == 1
        tape_files.append(result[0]["tf"])
    return tape_files


def describe_readtp_tape_files(
    archived_files: list[ArchivedReadtpFile], tape_files: list[dict[str, Any]]
) -> ReadtpTapeFiles:
    # All sequence forms are tested against one tape with consecutive fSeq values
    vids = {tape_file["vid"] for tape_file in tape_files}
    assert len(vids) == 1, "The files needed by cta-readtp must be archived on the same tape"

    fseqs = sorted(int(tape_file["fSeq"]) for tape_file in tape_files)
    assert fseqs == list(range(fseqs[0], fseqs[0] + len(fseqs))), "Expected consecutively archived tape files"

    contents = {
        int(tape_file["fSeq"]): archived_file.content for tape_file, archived_file in zip(tape_files, archived_files)
    }
    return ReadtpTapeFiles(vids.pop(), fseqs, contents)


# =========================================================================
#  Fixtures
# =========================================================================


@pytest.fixture(scope="module")
def readtp_tape_files(
    eos_client: EosClientHost,
    disk_instance_name: str,
    cta_cli: CtaCliHost,
    cta_taped: CtaTapedHost,
    test_dir: Path,
) -> Iterator[ReadtpTapeFiles]:
    cta_cli.set_all_drives_down()
    try:
        archived_files: list[ArchivedReadtpFile] = []
        # Queue files for archival
        for index in range(6):
            source_path = Path(f"/tmp/readtp_source_{uuid.uuid4().hex}")
            destination_path = test_dir / f"readtp_{index}_{uuid.uuid4().hex}"
            content = f"cta-readtp system test file {index}\n"
            eos_client.exec(f"printf %s {shlex.quote(content)} > {shlex.quote(str(source_path))}")
            eos_client.archive_file(disk_instance_name, destination_path, source_path, wait=False)
            eos_client.exec(f"rm {shlex.quote(str(source_path))}")
            archived_files.append(ArchivedReadtpFile(destination_path, content))
        # Put single drive up to let it consume everything to a single tape
        cta_cli.set_drive_up(cta_taped.drive_name)

        for archived_file in archived_files:
            eos_client.wait_for_file_archival(disk_instance_name, archived_file.path)
        cta_cli.wait_for_drives_to_stop_transferring()

        # Set drives down so that readtp does not compete with taped for the underlying device
        cta_cli.set_all_drives_down()

        # Find the tf entries for the files we just archived
        tape_files = find_tape_files(eos_client, cta_cli, disk_instance_name, archived_files)

        vids = {tape_file["vid"] for tape_file in tape_files}
        assert len(vids) == 1, "The files needed by cta-readtp must be archived on the same tape"

        fseqs = sorted(int(tape_file["fSeq"]) for tape_file in tape_files)
        assert fseqs == list(range(fseqs[0], fseqs[0] + len(fseqs))), "Expected consecutively archived tape files"

        # Pack them nicely so that we can use this later to compare the results
        contents = {
            int(tape_file["fSeq"]): archived_file.content
            for tape_file, archived_file in zip(tape_files, archived_files)
        }
        yield ReadtpTapeFiles(vids.pop(), fseqs, contents)
    finally:
        cta_cli.set_all_drives_up()


@pytest.fixture(scope="module")
def mounted_volume(cta_rmcd: CtaRmcdHost, cta_taped: CtaTapedHost) -> Iterator[tuple[int, str]]:
    # Use the taped fixture drive so that the test can unload its mechanism before dismounting
    drive_ordinal = cta_taped.drive_index
    drive = smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]
    assert drive["status"] == "free"

    # Select any cartridge in a storage slot without assuming a VID or library layout
    volume = next(volume for volume in smc_query(cta_rmcd, "V") if volume["elementType"] == "slot")
    vid = volume["vid"]
    cta_rmcd.exec(f"cta-smc -m -D {drive_ordinal} -V {shlex.quote(vid)}")
    try:
        yield drive_ordinal, vid
    finally:
        # A robot dismount requires the drive mechanism to unload the tape first
        if smc_query(cta_rmcd, "V", f"-V {shlex.quote(vid)}")[0]["elementType"] == "drive":
            drive = smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]
            if drive["status"] == "loaded":
                cta_taped.exec(f"mt -f {cta_taped.drive_device} offline")
            cta_rmcd.exec(f"cta-smc -d -D {drive_ordinal} -V {shlex.quote(vid)}")


@pytest.fixture(scope="module")
def ejected_volume(cta_rmcd: CtaRmcdHost) -> Iterator[str]:
    # Export a cartridge from a slot and import it during cleanup if the test did not do so
    volume = next(volume for volume in smc_query(cta_rmcd, "V") if volume["elementType"] == "slot")
    vid = volume["vid"]
    cta_rmcd.exec(f"cta-smc -e -V {shlex.quote(vid)}")
    try:
        yield vid
    finally:
        if smc_query(cta_rmcd, "V", f"-V {shlex.quote(vid)}")[0]["elementType"] == "export":
            cta_rmcd.exec(f"cta-smc -i -V {shlex.quote(vid)}")


# =========================================================================
#  Tests
# =========================================================================


def test_archive_files_for_readtp(readtp_tape_files: ReadtpTapeFiles) -> None:
    # Confirm that fixture setup produced the complete readtp data set
    assert len(readtp_tape_files.fseqs) == 6


def test_readtp_range(cta_taped: CtaTapedHost, readtp_tape_files: ReadtpTapeFiles) -> None:
    # Read one inclusive range and verify every returned payload
    fseqs = readtp_tape_files.fseqs[:3]
    output = run_readtp(cta_taped, readtp_tape_files, f"{fseqs[0]}-{fseqs[-1]}", fseqs)
    assert_readtp_output(cta_taped, readtp_tape_files, output)


def test_readtp_until_last_file(cta_taped: CtaTapedHost, readtp_tape_files: ReadtpTapeFiles) -> None:
    # Read to the end of the tape
    fseqs = readtp_tape_files.fseqs[-2:]
    output = run_readtp(cta_taped, readtp_tape_files, f"{fseqs[0]}-", fseqs)
    assert_readtp_output(cta_taped, readtp_tape_files, output)


def test_readtp_non_consecutive_file_ranges(cta_taped: CtaTapedHost, readtp_tape_files: ReadtpTapeFiles) -> None:
    # Combine individual fSeq values in one request
    fseqs = readtp_tape_files.fseqs
    expected_fseqs = [fseqs[0], fseqs[2], fseqs[3], fseqs[5]]
    output = run_readtp(
        cta_taped,
        readtp_tape_files,
        f"{fseqs[0]},{fseqs[2]}-{fseqs[3]},{fseqs[5]}",
        expected_fseqs,
    )
    assert_readtp_output(cta_taped, readtp_tape_files, output)


def test_smc_query_drives(cta_rmcd: CtaRmcdHost) -> None:
    # Check drive ordinals and addresses before comparing filtered queries
    drives = smc_query(cta_rmcd, "D")
    assert drives
    assert [drive["driveOrdinal"] for drive in drives] == list(range(len(drives)))
    assert len({drive["elementAddress"] for drive in drives}) == len(drives)
    assert all(drive["status"] in {"free", "loaded", "unloaded", "error"} for drive in drives)
    for drive in drives:
        assert smc_query(cta_rmcd, "D", f"-D {drive['driveOrdinal']}") == [drive]


def test_smc_query_library(cta_rmcd: CtaRmcdHost) -> None:
    # Validate the inquiry data and geometry returned for the library
    libraries = smc_query(cta_rmcd, "L")
    assert len(libraries) == 1
    library = libraries[0]
    assert set(library) == {"inquiry", "transport", "slot", "port", "device"}
    assert all(key in library["inquiry"] for key in ("vendor", "product", "revision"))
    assert all(
        isinstance(library[element][key], int)
        for element in ("transport", "slot", "port", "device")
        for key in ("count", "start")
    )


def test_smc_query_import_export_slots(cta_rmcd: CtaRmcdHost) -> None:
    # Validate the shape and possible states of every import/export port
    ports = smc_query(cta_rmcd, "P")
    assert all(set(port) == {"elementAddress", "vid", "state"} for port in ports)
    assert all(port["state"] in {"", "import", "export"} for port in ports)


def test_smc_query_status_slots(cta_rmcd: CtaRmcdHost) -> None:
    # Cross-check slot addresses against the library geometry
    library = smc_query(cta_rmcd, "L")[0]
    slots = smc_query(cta_rmcd, "S")
    assert len(slots) == library["slot"]["count"]
    assert [slot["elementAddress"] for slot in slots] == list(
        range(library["slot"]["start"], library["slot"]["start"] + library["slot"]["count"])
    )
    if slots:
        # Check that starting address and element count restrict the result
        assert smc_query(cta_rmcd, "S", f"-S {slots[0]['elementAddress']} -N 1") == slots[:1]


def test_smc_query_status_volumes(cta_rmcd: CtaRmcdHost) -> None:
    # Check unique VIDs and compare each filtered query with the full response
    volumes = smc_query(cta_rmcd, "V")
    assert len({volume["vid"] for volume in volumes}) == len(volumes)
    assert all(volume["vid"] and volume["elementType"] in {"slot", "drive", "import", "export"} for volume in volumes)
    for volume in volumes:
        assert smc_query(cta_rmcd, "V", f"-V {shlex.quote(volume['vid'])}") == [volume]


def test_smc_mount(cta_rmcd: CtaRmcdHost, mounted_volume: tuple[int, str]) -> None:
    # Confirm that the fixture moved the selected cartridge into the drive
    drive_ordinal, vid = mounted_volume
    mounted = smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]
    assert mounted["vid"] == vid
    assert mounted["status"] in {"loaded", "unloaded"}
    assert smc_query(cta_rmcd, "V", f"-V {shlex.quote(vid)}")[0]["elementType"] == "drive"


def test_smc_dismount(
    cta_rmcd: CtaRmcdHost,
    cta_taped: CtaTapedHost,
    mounted_volume: tuple[int, str],
) -> None:
    drive_ordinal, vid = mounted_volume
    # Unload the drive mechanism before asking the robot to return the cartridge
    drive = smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]
    if drive["status"] == "loaded":
        cta_taped.exec(f"mt -f {cta_taped.drive_device} offline")
    assert smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]["status"] == "unloaded"
    cta_rmcd.exec(f"cta-smc -d -D {drive_ordinal} -V {shlex.quote(vid)}")

    assert smc_query(cta_rmcd, "D", f"-D {drive_ordinal}")[0]["status"] == "free"
    assert smc_query(cta_rmcd, "V", f"-V {shlex.quote(vid)}")[0]["elementType"] == "slot"


def test_smc_eject(cta_rmcd: CtaRmcdHost, ejected_volume: str) -> None:
    # Confirm that the selected cartridge appears in an export port
    exported = smc_query(cta_rmcd, "V", f"-V {shlex.quote(ejected_volume)}")[0]
    assert exported["elementType"] == "export"
    assert any(port["vid"] == ejected_volume and port["state"] == "export" for port in smc_query(cta_rmcd, "P"))


def test_smc_inject(cta_rmcd: CtaRmcdHost, ejected_volume: str) -> None:
    # Return the exported cartridge to a storage slot
    cta_rmcd.exec(f"cta-smc -i -V {shlex.quote(ejected_volume)}")

    assert smc_query(cta_rmcd, "V", f"-V {shlex.quote(ejected_volume)}")[0]["elementType"] == "slot"
