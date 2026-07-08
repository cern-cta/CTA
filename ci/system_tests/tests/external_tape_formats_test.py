# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import uuid
import time

ENSTORE_LABEL_BLOCK_SIZE = 80
OSM_HEADER_BLOCK_SIZE = 32768
TAPE_PAYLOAD_BLOCK_SIZE = 262144

OSM_HEADER_BLOCK_COUNT = 2
OSM_FILE_BLOCK_COUNT = 202

ENSTORE_TAPE_SLOT = 2
ENSTORE_LARGE_TAPE_SLOT = 3

#####################################################################################################################
# Helpers
#####################################################################################################################


def wait_for_device_ready(host, drive_device: str, timeout_seconds: int = 60):
    deadline = time.monotonic() + timeout_seconds
    last_result = None

    while time.monotonic() < deadline:
        last_result = host.exec(f"mt -f {drive_device} status", capture_output=True, throw_on_failure=False)
        if last_result.success:
            return
        time.sleep(1)

    stderr = last_result.stderr.strip() if last_result else ""
    raise TimeoutError(f"Tape device {drive_device} still busy after {timeout_seconds}s. {stderr}")


def wait_for_drive_readable(cta_taped, drive_device: str, timeout_seconds: int = 30):
    deadline = time.monotonic() + timeout_seconds
    last_result = None

    while time.monotonic() < deadline:
        rewind_result = cta_taped.exec(f"mt -f {drive_device} rewind", capture_output=True, throw_on_failure=False)
        read_result = cta_taped.exec(
            f"dd if={drive_device} of=/dev/null bs={TAPE_PAYLOAD_BLOCK_SIZE} count=1",
            capture_output=True,
            throw_on_failure=False,
        )
        final_rewind_result = cta_taped.exec(
            f"mt -f {drive_device} rewind",
            capture_output=True,
            throw_on_failure=False,
        )
        last_result = read_result

        if rewind_result.success and read_result.success and final_rewind_result.success:
            return
        time.sleep(1)

    stderr = last_result.stderr.strip() if last_result else ""
    raise TimeoutError(f"Tape device {drive_device} still not readable after {timeout_seconds}s. {stderr}")


def write_tape_file(cta_rmcd, drive_device: str, input_path: str, block_size: int, description: str):
    wait_for_device_ready(cta_rmcd, drive_device)

    last_result = None
    for attempt in range(1, 6):
        last_result = cta_rmcd.exec(
            f"dd if={input_path} of={drive_device} bs={block_size}",
            throw_on_failure=False,
        )
        if last_result.success:
            time.sleep(2)
            wait_for_device_ready(cta_rmcd, drive_device)
            return

        print(f"Failed to write {description} to {drive_device} " f"(attempt {attempt}/5), waiting for mhvtl to settle")
        if last_result.stderr:
            print(last_result.stderr.rstrip())
        time.sleep(2)
        wait_for_device_ready(cta_rmcd, drive_device)

    stderr = last_result.stderr.strip() if last_result else ""
    raise RuntimeError(f"Failed to write {description} to {drive_device}. {stderr}")


def read_tape_file(cta_rmcd, drive_device: str, output_path: str, block_size: int, count: int, description: str):
    wait_for_device_ready(cta_rmcd, drive_device)

    last_result = None
    for attempt in range(1, 6):
        last_result = cta_rmcd.exec(
            f"dd if={drive_device} of={output_path} bs={block_size} count={count}",
            throw_on_failure=False,
        )
        if last_result.success:
            return

        print(
            f"Failed to read {description} from {drive_device} " f"(attempt {attempt}/5), waiting for mhvtl to settle"
        )
        if last_result.stderr:
            print(last_result.stderr.rstrip())
        time.sleep(2)
        wait_for_device_ready(cta_rmcd, drive_device)

    stderr = last_result.stderr.strip() if last_result else ""
    raise RuntimeError(f"Failed to read {description} from {drive_device}. {stderr}")


def space_filemarks_forward(cta_rmcd, drive_device: str, count: int, description: str):
    wait_for_device_ready(cta_rmcd, drive_device)

    last_result = None
    for attempt in range(1, 6):
        last_result = cta_rmcd.exec(
            f"mt -f {drive_device} fsf {count}",
            capture_output=True,
            throw_on_failure=False,
        )
        if last_result.success:
            time.sleep(2)
            wait_for_device_ready(cta_rmcd, drive_device)
            return

        print(
            f"Failed to position to {description} on {drive_device} "
            f"(attempt {attempt}/5), waiting for mhvtl to settle"
        )
        if last_result.stderr:
            print(last_result.stderr.rstrip())
        time.sleep(2)
        wait_for_device_ready(cta_rmcd, drive_device)

    stderr = last_result.stderr.strip() if last_result else ""
    raise RuntimeError(f"Failed to position to {description} on {drive_device}. {stderr}")


def remote_file_size(host, path: str) -> int:
    return int(host.exec_with_output(f"stat -c%s {path}"))


def remote_sha256(host, path: str) -> str:
    return host.exec_with_output(f"sha256sum {path}").split()[0]


def assert_remote_files_match(host, expected_path: str, actual_path: str):
    assert remote_file_size(host, actual_path) == remote_file_size(host, expected_path)
    assert remote_sha256(host, actual_path) == remote_sha256(host, expected_path)


def load_tape(cta_rmcd, slot: int, drive: int):
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec(f"mtx -f /dev/smc load {slot} {drive}")
    cta_rmcd.exec("mtx -f /dev/smc status")


def unload_tape(cta_rmcd, slot: int, drive: int):
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec(f"mtx -f /dev/smc unload {slot} {drive}")
    cta_rmcd.exec("mtx -f /dev/smc status")


def reload_tape(cta_rmcd, slot: int, drive: int):
    unload_tape(cta_rmcd, slot, drive)
    load_tape(cta_rmcd, slot, drive)
    time.sleep(2)


def clone_enstore_samples(cta_rmcd) -> str:
    sample_dir = "/ens_mhvtl_" + str(uuid.uuid4())[:8]
    cta_rmcd.exec("git lfs install --skip-repo")
    cta_rmcd.exec(f"git clone https://github.com/LTrestka/ens-mhvtl.git {sample_dir}")
    return sample_dir


#####################################################################################################################
# Tests
#####################################################################################################################


def test_install_required(cta_rmcd, cta_taped):
    cta_rmcd.exec("sudo microdnf install -y git git-lfs")
    # TODO: remove; do with kubectl debug using test image?
    cta_taped.exec("sudo microdnf -y install cta-integrationtests")


def test_load_tape(cta_rmcd):
    # Load tape in a tapedrive
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec("mtx -f /dev/smc load 1 0")
    cta_rmcd.exec("mtx -f /dev/smc status")


def test_read_osm_tape(cta_rmcd, cta_taped):
    drive_device = cta_taped.drive_device
    osm_dir = "/osm_mhvtl_" + str(uuid.uuid4())[:8]

    # Download OSM sample tape
    cta_rmcd.exec("git lfs install --skip-repo")
    cta_rmcd.exec(f"git clone https://gitlab.desy.de/mwai.karimi/osm-mhvtl.git {osm_dir}")

    # Get the device status where the tape is loaded and rewind it.
    cta_rmcd.exec(f"mt -f {drive_device} status")
    cta_rmcd.exec(f"mt -f {drive_device} rewind")

    # The header files have 4 more bytes in the git file
    cta_rmcd.exec(f"truncate -s -4 /{osm_dir}/L08033/L1")
    cta_rmcd.exec(f"touch /{osm_dir}/osm-tape.img")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/L1 of=/{osm_dir}/osm-tape.img bs={OSM_HEADER_BLOCK_SIZE}")
    cta_rmcd.exec(f"dd if=/{osm_dir}/L08033/L2 of=/{osm_dir}/osm-tape.img bs={OSM_HEADER_BLOCK_SIZE} seek=1")
    cta_rmcd.exec(
        f"dd if=/{osm_dir}/osm-tape.img of={drive_device} bs={OSM_HEADER_BLOCK_SIZE} count={OSM_HEADER_BLOCK_COUNT}"
    )
    cta_rmcd.exec(
        f"dd if=/{osm_dir}/L08033/file1 of={drive_device} bs={TAPE_PAYLOAD_BLOCK_SIZE} count={OSM_FILE_BLOCK_COUNT}"
    )
    cta_rmcd.exec(
        f"dd if=/{osm_dir}/L08033/file2 of={drive_device} bs={TAPE_PAYLOAD_BLOCK_SIZE} count={OSM_FILE_BLOCK_COUNT}"
    )

    # Clean up the clone
    cta_rmcd.exec(f"rm -rf {osm_dir}")

    cta_rmcd.exec(f"mt -f {drive_device} rewind")


def test_osm_reader(cta_taped):
    print(f"Using drive: {cta_taped.drive_name}, device: {cta_taped.drive_device}")
    cta_taped.exec(f"cta-osmReaderTest {cta_taped.drive_name} {cta_taped.drive_device}")


def test_unload_tape(cta_rmcd):
    # Unload the tape again
    cta_rmcd.exec("mtx -f /dev/smc status")
    cta_rmcd.exec("mtx -f /dev/smc unload 1 0")
    cta_rmcd.exec("mtx -f /dev/smc status")


def test_load_enstore_tape(cta_rmcd, cta_taped):
    load_tape(cta_rmcd, ENSTORE_TAPE_SLOT, cta_taped.drive_index)


def test_read_write_enstore_tape(cta_rmcd, cta_taped):
    drive_device = cta_taped.drive_device
    sample_dir = clone_enstore_samples(cta_rmcd)
    layout_dir = f"{sample_dir}/enstore/FL1212_f1"
    readback_dir = f"/enstore_readback_{str(uuid.uuid4())[:8]}"

    cta_rmcd.exec(f"mkdir -p {readback_dir}")
    try:
        cta_rmcd.exec(f"test -f {layout_dir}/vol1_FL1212.bin")
        cta_rmcd.exec(f"test -f {layout_dir}/fseq1_payload.bin")

        cta_rmcd.exec(f"mt -f {drive_device} status")
        wait_for_device_ready(cta_rmcd, drive_device)
        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        wait_for_device_ready(cta_rmcd, drive_device)

        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/vol1_FL1212.bin",
            ENSTORE_LABEL_BLOCK_SIZE,
            "Enstore VOL1 label",
        )
        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/fseq1_payload.bin",
            TAPE_PAYLOAD_BLOCK_SIZE,
            "Enstore payload",
        )

        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        wait_for_device_ready(cta_rmcd, drive_device)
        read_tape_file(
            cta_rmcd,
            drive_device,
            f"{readback_dir}/vol1.bin",
            ENSTORE_LABEL_BLOCK_SIZE,
            1,
            "Enstore VOL1 label",
        )
        assert_remote_files_match(cta_rmcd, f"{layout_dir}/vol1_FL1212.bin", f"{readback_dir}/vol1.bin")

        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        wait_for_device_ready(cta_rmcd, drive_device)
        space_filemarks_forward(cta_rmcd, drive_device, 1, "Enstore payload")

        payload_size = remote_file_size(cta_rmcd, f"{layout_dir}/fseq1_payload.bin")
        payload_blocks = (payload_size + TAPE_PAYLOAD_BLOCK_SIZE - 1) // TAPE_PAYLOAD_BLOCK_SIZE
        read_tape_file(
            cta_rmcd,
            drive_device,
            f"{readback_dir}/payload.bin",
            TAPE_PAYLOAD_BLOCK_SIZE,
            payload_blocks,
            "Enstore payload",
        )
        assert_remote_files_match(cta_rmcd, f"{layout_dir}/fseq1_payload.bin", f"{readback_dir}/payload.bin")

        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        wait_for_device_ready(cta_rmcd, drive_device)
    finally:
        cta_rmcd.exec(f"rm -rf {sample_dir} {readback_dir}")


def test_unload_enstore_tape(cta_rmcd, cta_taped):
    unload_tape(cta_rmcd, ENSTORE_TAPE_SLOT, cta_taped.drive_index)


def test_load_enstore_large_tape(cta_rmcd, cta_taped):
    load_tape(cta_rmcd, ENSTORE_LARGE_TAPE_SLOT, cta_taped.drive_index)


def test_write_enstore_large_tape(cta_rmcd, cta_taped):
    drive_device = cta_taped.drive_device
    sample_dir = clone_enstore_samples(cta_rmcd)
    layout_dir = f"{sample_dir}/enstorelarge/FL1587_f1"

    try:
        for segment in ["vol1_FL1587.bin", "fseq1_header.bin", "fseq1_payload.bin", "fseq1_trailer.bin"]:
            cta_rmcd.exec(f"test -f {layout_dir}/{segment}")

        cta_rmcd.exec(f"mt -f {drive_device} status")
        wait_for_device_ready(cta_rmcd, drive_device)
        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        wait_for_device_ready(cta_rmcd, drive_device)

        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/vol1_FL1587.bin",
            ENSTORE_LABEL_BLOCK_SIZE,
            "EnstoreLarge VOL1 label",
        )
        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/fseq1_header.bin",
            TAPE_PAYLOAD_BLOCK_SIZE,
            "EnstoreLarge file header",
        )
        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/fseq1_payload.bin",
            TAPE_PAYLOAD_BLOCK_SIZE,
            "EnstoreLarge payload",
        )
        write_tape_file(
            cta_rmcd,
            drive_device,
            f"{layout_dir}/fseq1_trailer.bin",
            TAPE_PAYLOAD_BLOCK_SIZE,
            "EnstoreLarge trailer",
        )

        cta_rmcd.exec(f"mt -f {drive_device} rewind")
        time.sleep(2)
        wait_for_device_ready(cta_rmcd, drive_device)
    finally:
        cta_rmcd.exec(f"rm -rf {sample_dir}")


def test_enstore_large_reader(cta_rmcd, cta_taped):
    reload_tape(cta_rmcd, ENSTORE_LARGE_TAPE_SLOT, cta_taped.drive_index)
    wait_for_drive_readable(cta_taped, cta_taped.drive_device)

    print(f"Using drive: {cta_taped.drive_name}, device: {cta_taped.drive_device}")
    cta_taped.exec(f"cta-enstoreLargeReaderTest {cta_taped.drive_name} {cta_taped.drive_device}")


def test_unload_enstore_large_tape(cta_rmcd, cta_taped):
    unload_tape(cta_rmcd, ENSTORE_LARGE_TAPE_SLOT, cta_taped.drive_index)
