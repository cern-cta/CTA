# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import time
import multiprocessing
import uuid
from dataclasses import dataclass
from pathlib import Path
from ..helpers.utils import Timeout
from typing import Optional

import pytest

#####################################################################################################################
# Helpers
#####################################################################################################################


@dataclass
class RepackParams:
    file_size_kb: int
    process_count: int


@pytest.fixture(scope="module")
def repack_params(request) -> RepackParams:
    client_config = request.config.test_config["tests"]["repack"]
    return RepackParams(
        file_size_kb=client_config["file_size_kb"],
        process_count=client_config["process_count"],
    )


@pytest.fixture(scope="module")
def repack_vo_name() -> str:
    return "vo_repack"


@pytest.fixture(scope="module")
def repack_mp_name() -> str:
    return "repack_ctasystest"


@pytest.fixture(scope="module")
def repack_base_dir() -> Path:
    return Path("/var") / "log"


@pytest.fixture(scope="function")
def repack_buffer_dir(eos_mgm, eos_client, disk_instance_name, request):
    # Needs to be a different dir than test_dir
    path = eos_mgm.base_dir_path / "repack" / f"{request.function.__name__}_{uuid.uuid4().hex[:6]}"
    eos_mgm.exec(f"eos mkdir -p {path}")
    eos_mgm.exec(f"eos chmod 1777 {path}")

    print(f"Testing the repack buffer URL at root://{disk_instance_name}/{path}")
    eos_client.exec(f"eos root://{disk_instance_name} ls -d {path} 1> /dev/null")
    print("\tTesting the insertion of a test file in the buffer URL")
    tmp_file_path = eos_client.exec_with_output("mktemp /tmp/repack_buffer_test_file.XXXX")
    tmp_file_name = Path(tmp_file_path).name
    eos_client.exec(f"xrdcp {tmp_file_path} root://{disk_instance_name}/{path}/{tmp_file_name}")
    print(f"\tFile {tmp_file_path} written in root://{disk_instance_name}/{path}/{tmp_file_name}")
    print("\tDeleting test file from the test directory")
    eos_client.exec(f"eos root://{disk_instance_name} rm {path}/{tmp_file_name}")
    print("Test repack buffer URL OK, returning the path")

    return path  # Technically we could yield and cleanup after, but keeping them around makes for easier debugging


def _get_first_vid_containing_files(cta_cli) -> str:
    tapes = json.loads(cta_cli.exec_with_output("cta-admin --json tape ls --all"))
    for tape in tapes:
        occupancy = tape.get("occupancy", 0)
        last_fseq = tape.get("lastFseq", 0)
        if occupancy not in (None, "0", 0) and last_fseq not in (None, "0", 0):
            return tape["vid"]
    pytest.fail("Failed to find a VID to repack")
    return None


def _get_number_of_files_on_tape(cta_cli, vid: str) -> int:
    tapefile_json = cta_cli.exec_with_output(f"cta-admin --json tf ls --vid {vid}")
    return len(json.loads(tapefile_json))


def _wait_for_tape_state(cta_cli, vid: str, expected_state: str, *, timeout_secs: int = 60) -> None:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        tape_json = json.loads(
            cta_cli.exec_with_output(f"cta-admin --json tape ls --state {expected_state} --vid {vid}")
        )
        if any(tape.get("vid") == vid for tape in tape_json):
            return
        time.sleep(1)
    raise TimeoutError(f"Tape {vid} did not reach state {expected_state} within {timeout_secs} seconds")


def _modify_tape_state(cta_cli, vid: str, state: str, reason: str = "Testing", wait=True) -> None:
    print(f"Marking tape {vid} as {state}")
    cta_cli.exec(f"cta-admin tape ch --state {state} --reason '{reason}' --vid {vid}")
    if wait:
        _wait_for_tape_state(cta_cli, vid, state)


def _remove_repack_request(cta_cli, vid: str, throw_on_failure: bool = True) -> None:
    print(f"Removing repack request for {vid}")
    cta_cli.exec(f"cta-admin repack rm --vid {vid}", throw_on_failure=throw_on_failure)


def _reclaim_tape(cta_cli, vid: str) -> None:
    print(f"Reclaiming tape {vid}")
    cta_cli.exec(f"cta-admin tape reclaim --vid {vid}")


def _has_repack_request(cta_cli, vid) -> bool:
    try:
        re_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
        return len(re_ls_json) > 0
    except json.JSONDecodeError:
        return False


def _retrieve_queue_empty(cta_cli, vid) -> int:
    queues = json.loads(cta_cli.exec_with_output("cta-admin --json showqueues"))
    return not any(q["vid"] == vid for q in queues)


def _count_files_in_queue(cta_cli, vid) -> int:
    queues = json.loads(cta_cli.exec_with_output("cta-admin --json showqueues"))
    for queue in queues:
        if queue["vid"] == vid:
            return int(queue["queuedFiles"])
    return 0


def _wait_for_repack_request_launch(cta_cli, vid, wait_timeout_secs=30) -> None:
    print(f"Waiting for the launch of the repack request on VID {vid}...")
    wait_timeout_secs = 20
    with Timeout(wait_timeout_secs) as t:
        while not _has_repack_request(cta_cli, vid) and not t.expired:
            time.sleep(1)
        if t.expired:
            raise TimeoutError(
                f"No repack request appeared for VID {vid} within timeout of {wait_timeout_secs} seconds"
            )
    print("Repack request launched")


def _wait_for_repack_request_expansion(cta_cli, vid, wait_timeout_secs=30) -> None:
    print(f"Waiting for repack request expansion on VID {vid}...")
    tape_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tape ls --vid {vid}"))
    lastFSeq = tape_json[0]["lastFseq"]
    wait_timeout_secs = 20
    with Timeout(wait_timeout_secs) as t:
        lastExpandedFSeq = 0
        while lastExpandedFSeq != lastFSeq and not t.expired:
            try:
                repack_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
                lastExpandedFSeq = repack_json[0]["lastExpandedFseq"]
            except json.JSONDecodeError:
                ...  # In this case re ls just didn't found anything
            time.sleep(1)
        if t.expired:
            raise TimeoutError(
                f"Repack request for VID {vid} failed to expand within timeout of {wait_timeout_secs} seconds. Last fSeq on tape: {lastFSeq}, last expanded fSeq: {lastExpandedFSeq}"
            )
    print("Repack request expanded")


def _wait_for_queue_to_empty(cta_cli, vid, wait_timeout_secs=30) -> None:
    print(f"Waiting for retrieve queue of {vid} to be empty...")
    wait_timeout_secs = 20
    with Timeout(wait_timeout_secs) as t:
        while not _retrieve_queue_empty(cta_cli, vid) and not t.expired:
            time.sleep(1)
        if t.expired:
            raise TimeoutError(
                f"Retrieve queue for VID {vid} failed to empty within timeout of {wait_timeout_secs} seconds."
            )
    print("Retrieve queue empty")


#####################################################################################################################
# Internal Test (called from the tests below)
#####################################################################################################################


def submit_repack_request(
    cta_cli,
    vid_to_repack: str,
    disk_instance_name: str,
    repack_buffer_dir: Path,
    mount_policy_name: str,
    add_copies_only: bool = False,
    move_only: bool = False,
    with_back_pressure: bool = False,
    no_recall: bool = False,
    max_files_to_select: Optional[int] = None,
):
    repack_timeout_secs = 120

    config_str = ", ".join(f"{k}={v}" for k, v in locals().items() if k != "cta_cli")
    print(f"\nTesting with the following options: {config_str}\n")

    assert not (add_copies_only and move_only)  # Mutually exclusive

    print(f"Deleting existing repack request for VID {vid_to_repack}")
    cta_cli.exec(f"cta-admin repack rm --vid {vid_to_repack}", throw_on_failure=False)

    print(f"Marking the tape {vid_to_repack} as full before Repacking it")
    cta_cli.exec(f"cta-admin tape ch --vid {vid_to_repack} --full true")

    if with_back_pressure:
        print("Backpressure test: setting too high free space requirements")
        ds_ls_json = json.loads(cta_cli.exec_with_output("cta-admin --json ds ls"))
        if not any(item.get("name") == "repackBuffer" for item in ds_ls_json):
            dis_name = "repackDiskInstanceSpace"
            cta_cli.exec(
                f"cta-admin dis add -n {dis_name} --di {disk_instance_name} -u 'eosSpace:default' -i 5 -m 'Repack test dis'"
            )
            cta_cli.exec(
                f"cta-admin ds add -n repackBuffer --di {disk_instance_name} --dis {dis_name} -r 'root://{disk_instance_name}/{repack_buffer_dir}' -f 111222333444555 -s 20 -m 'Repack test buffer ds'"
            )
        else:
            print("Disk system repackBuffer already defined. Ensuring too high free space requirements.")
            cta_cli.exec("cta-admin ds ch -n repackBuffer -f 111222333444555")

        cta_cli.exec("cta-admin ds ls")

    recycle_tape_file_json = json.loads(
        cta_cli.exec_with_output(f"cta-admin --json recycletf ls --vid {vid_to_repack}")
    )
    nb_recycle_tape_file_prev = len(recycle_tape_file_json)

    # Construct and submit repack request
    total_files_on_tape = _get_number_of_files_on_tape(cta_cli, vid_to_repack)
    print(f"Submitting repack request for VID {vid_to_repack}, buffer directory: {repack_buffer_dir}")
    print(f"Number of files currently on VID {vid_to_repack}: {total_files_on_tape}")

    extra_repack_options: list[str] = []
    if max_files_to_select is not None:
        assert max_files_to_select <= total_files_on_tape
        print(f"Partial repack covering {max_files_to_select}/{total_files_on_tape} files from tape {vid_to_repack}")
        extra_repack_options.append(f"--maxfilestoselect {max_files_to_select}")
    if move_only:
        extra_repack_options.append("-m")
    if add_copies_only:
        extra_repack_options.append("-a")
    if no_recall:
        extra_repack_options.append("--nr")

    full_buffer_url = f"root://{disk_instance_name}/{repack_buffer_dir}"
    repack_options_str: str = " ".join(extra_repack_options)
    cta_cli.exec(
        f"cta-admin repack add --mountpolicy {mount_policy_name} --vid {vid_to_repack} --bufferurl {full_buffer_url} {repack_options_str}"
    )

    # Now we wait
    if with_back_pressure:
        print("Backpressure test: waiting to see a report of sleeping retrieve queue.")

        with Timeout(repack_timeout_secs) as t:
            is_sleeping = False
            while not is_sleeping and not t.expired:
                try:
                    queues = json.loads(cta_cli.exec_with_output("cta-admin --json sq"))
                    # Check if our target VID is currently sleeping for space
                    is_sleeping = any(
                        q.get("vid") == vid_to_repack and q.get("sleepingForSpace") is True for q in queues
                    )
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass

                if not is_sleeping:
                    print(
                        f"\tWaiting for retrieve queue for tape {vid_to_repack} to be sleeping: Seconds passed = {int(time.time() - t.start)}"
                    )
                    time.sleep(1)

            if t.expired:
                raise TimeoutError(
                    f"Retrieve queue for tape {vid_to_repack} failed to sleep within {repack_timeout_secs} seconds"
                )

        print("Turning free space requirement to one byte (zero is not allowed).")
        cta_cli.exec_with_output("cta-admin ds ch -n repackBuffer -f 1")
        print(cta_cli.exec_with_output("cta-admin ds ls"))

    print("Waiting for repack to proceed.")
    with Timeout(repack_timeout_secs) as t:
        is_finished = False
        while not is_finished and not t.expired:
            try:
                repack_ls_json = json.loads(
                    cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}")
                )
                if repack_ls_json and len(repack_ls_json) > 0:
                    status = repack_ls_json[0].get("status")
                    is_finished = status in ["Complete", "Failed"]
            except (json.JSONDecodeError, IndexError, KeyError):
                pass

            if not is_finished:
                print(
                    f"\tWaiting for repack request on tape {vid_to_repack} to complete: Seconds passed = {int(time.time() - t.start)}"
                )
                time.sleep(1)

        if t.expired:
            print(f"Timed out after {repack_timeout_secs} seconds waiting for tape {vid_to_repack} to be repacked")
            print("Result of show queues")
            print(cta_cli.exec_with_output("cta-admin sq"))
            print("Result of dr ls")
            print(cta_cli.exec_with_output("cta-admin dr ls"))
            print("Result of Repack ls")
            print(cta_cli.exec_with_output(f"cta-admin repack ls --vid {vid_to_repack}"))

            try:
                repack_ls_json = json.loads(
                    cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}")
                )
                destination_infos = repack_ls_json[0].get("destinationInfos", [])
                if len(destination_infos) != 0:
                    print(f"{'DestinationVID':<15}\t{'NbFiles':<10}\t{'totalSize'}")
                    for dest in destination_infos:
                        print(f"{dest['vid']:<15}\t{dest['files']:<10}\t{dest['bytes']}")
            except Exception:
                pass  # Prevent parsing crash from hiding the main failure logs above

            raise TimeoutError(
                f"Retrieve queue for tape {vid_to_repack} failed to sleep within {repack_timeout_secs} seconds"
            )

    # Summary

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]

    recycletf_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json recycletf ls --vid {vid_to_repack}"))
    destination_infos = repack_request.get("destinationInfos", [])
    # Check for Repack Failure status
    if repack_request.get("status") == "Failed":
        print(f"Repack failed for tape {vid_to_repack}.")
        print(json.dumps(repack_ls_json, indent=4))
        # Print destination table if data exists
        if len(destination_infos) != 0:
            print(f"{'DestinationVID':<15}\t{'NbFiles':<10}\t{'totalSize'}")
            for dest in destination_infos:
                print(f"{dest['vid']:<15}\t{dest['files']:<10}\t{dest['bytes']}")

        raise SystemExit(1)

    # Check for Max Files Selection (Partial Repack Logic)
    if max_files_to_select is not None:
        # Fetch tape file list from cta-admin
        tf_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tf ls --vid {vid_to_repack}"))
        total_files_on_tape = len(tf_ls_json)

        all_files_selected = bool(repack_request.get("allFilesSelectedAtStart"))

        if total_files_on_tape > max_files_to_select:
            # Expecting partial selection (allFilesSelectedAtStart == False) AND tape is not completely empty
            assert not all_files_selected and total_files_on_tape != 0
        else:
            # Expecting full selection (allFilesSelectedAtStart == True) AND tape is empty
            assert all_files_selected and total_files_on_tape == 0

    if len(destination_infos) != 0:
        print(f"{'DestinationVID':<15}\t{'NbFiles':<10}\t{'totalSize'}")
        for dest in destination_infos:
            print(f"{dest['vid']:<15}\t{dest['files']:<10}\t{dest['bytes']}")

    nb_archived_files = int(repack_request["archivedFiles"])
    nb_files_to_retrieve = int(repack_request["totalFilesToRetrieve"])
    nb_recycle_tape_files_new = len(recycletf_ls_json)
    nb_recycle_tape_files = nb_recycle_tape_files_new - nb_recycle_tape_file_prev

    files_left_to_retrieve = int(repack_request["filesLeftToRetrieve"])
    files_left_to_archive = int(repack_request["filesLeftToArchive"])
    nb_destination_vids = len(destination_infos)
    nb_archived_destination_files = sum(int(dest["files"]) for dest in destination_infos)

    print(f"Number of archived files = {nb_archived_files} (spread over {nb_destination_vids} tapes)")
    print(f"Number of new recycled tape files = {nb_recycle_tape_files}")
    print(f"Number of files left to retrieve = {files_left_to_retrieve}")
    print(f"Number of files left to archive = {files_left_to_archive}")

    assert files_left_to_retrieve == 0
    assert files_left_to_archive == 0
    assert nb_archived_destination_files == nb_archived_files
    # assert nb_files_to_retrieve == nb_recycle_tape_files

    print(f"Repack request on VID {vid_to_repack} succeeded\n")


#####################################################################################################################
# Tests
#####################################################################################################################


def test_setup_client(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_setup.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_helper.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "cli_calls.sh"), "/tmp/", permissions="+x")


def test_create_repack_vo(cta_cli, repack_vo_name, disk_instance_name):
    cta_cli.exec(
        f'cta-admin virtualorganization add --vo {repack_vo_name} --readmaxdrives 1 --writemaxdrives 1 --diskinstance {disk_instance_name} --isrepackvo true --comment "vo for repack"'
    )


def test_create_repack_mount_policy(cta_cli, repack_mp_name):
    # This mount policy is for repack: IT MUST CONTAIN THE `repack` STRING IN IT TO ALLOW MOUNTING DISABLED TAPES
    cta_cli.exec(
        f'cta-admin mountpolicy add --name {repack_mp_name} --archivepriority 2 --minarchiverequestage 1 --retrievepriority 2 --minretrieverequestage 1 --comment "repack mountpolicy for ctasystest"'
    )


# Apparently the tests flip out if you have multiple drives doing work, so for now we keep it as before: only a single drive doing work
def test_set_single_drive_up(cta_cli, cta_taped):
    cta_cli.set_all_drives_down()
    cta_cli.set_drive_up(cta_taped.drive_name)


def test_archive_1_file(
    eos_client,
    remote_scripts_dir,
    repack_params,
    test_dir,
) -> None:
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_archive.sh"), "/tmp/", permissions="+x")
    file_count = 1
    eos_client.exec(
        f"/tmp/client_setup.sh -n {file_count} -s {repack_params.file_size_kb} -p {repack_params.process_count} -d {test_dir} -A"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_archive.sh")


def test_repack_move_only_with_backpressure(cta_cli, repack_buffer_dir, repack_mp_name, disk_instance_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")

    print(f'Launching the repack "move only" test on VID {vid_to_repack} (with backpressure)')
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
        with_back_pressure=True,
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_non_repacking_tape(cta_cli, repack_buffer_dir, repack_mp_name, disk_instance_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)

    # TODO: we may want to parameterize this test in the future to remove some of this duplication and improve test granularity
    _modify_tape_state(cta_cli, vid_to_repack, "DISABLED")
    print(f"Launching the repack request test on VID {vid_to_repack} with DISABLED state")
    with pytest.raises(RuntimeError):
        submit_repack_request(
            cta_cli,
            vid_to_repack=vid_to_repack,
            disk_instance_name=disk_instance_name,
            repack_buffer_dir=repack_buffer_dir,
            mount_policy_name=repack_mp_name,
        )
        print("The repack command should have failed as the tape is DISABLED")

    _modify_tape_state(cta_cli, vid_to_repack, "BROKEN")
    print(f"Launching the repack request test on VID {vid_to_repack} with BROKEN state")
    with pytest.raises(RuntimeError):
        submit_repack_request(
            cta_cli,
            vid_to_repack=vid_to_repack,
            disk_instance_name=disk_instance_name,
            repack_buffer_dir=repack_buffer_dir,
            mount_policy_name=repack_mp_name,
        )
        print("The repack command should have failed as the tape is BROKEN")

    _modify_tape_state(cta_cli, vid_to_repack, "EXPORTED")
    print(f"Launching the repack request test on VID {vid_to_repack} with EXPORTED state")
    with pytest.raises(RuntimeError):
        submit_repack_request(
            cta_cli,
            vid_to_repack=vid_to_repack,
            disk_instance_name=disk_instance_name,
            repack_buffer_dir=repack_buffer_dir,
            mount_policy_name=repack_mp_name,
        )
        print("The repack command should have failed as the tape is EXPORTED")

    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    print(f"Launching the repack request test on VID {vid_to_repack} with ACTIVE state")
    with pytest.raises(RuntimeError):
        submit_repack_request(
            cta_cli,
            vid_to_repack=vid_to_repack,
            disk_instance_name=disk_instance_name,
            repack_buffer_dir=repack_buffer_dir,
            mount_policy_name=repack_mp_name,
        )
        print("The repack command should have failed as the tape is ACTIVE")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching the repack request test on VID {vid_to_repack} with REPACKING state")
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_archive_1000_files(eos_client, repack_params, test_dir):
    file_count = 1000
    eos_client.exec(
        f"/tmp/client_setup.sh -n {file_count} -s {repack_params.file_size_kb} -p {repack_params.process_count} -d {test_dir} -A"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_archive.sh")


def test_repack_move_only_subset_of_files(cta_cli, repack_buffer_dir, repack_mp_name, disk_instance_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    total_files_on_tape = _get_number_of_files_on_tape(cta_cli, vid_to_repack)
    number_of_files_to_repack = int(total_files_on_tape / 2)  # Number that covers only some of the files on tape (half)

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(
        f'Launching the repack test "just move", with {number_of_files_to_repack}/{total_files_on_tape} files, on VID {vid_to_repack}'
    )
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
        max_files_to_select=number_of_files_to_repack,
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    with pytest.raises(RuntimeError):
        _reclaim_tape(cta_cli, vid_to_repack)
        print("The reclaim command should have failed because the tape should still contain files")

    total_files_on_tape = _get_number_of_files_on_tape(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(
        f'Launching the repack test "just move", with the remainder {total_files_on_tape} files, on VID {vid_to_repack}'
    )
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
        max_files_to_select=total_files_on_tape,
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


# Why 1152? No one knows...
def test_archive_1152_files(
    eos_client,
    repack_params,
    test_dir,
):
    file_count = 1152
    eos_client.exec(
        f"/tmp/client_setup.sh -n {file_count} -s {repack_params.file_size_kb} -p {repack_params.process_count} -d {test_dir} -A"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_archive.sh")


def test_repack_move_only(cta_cli, disk_instance_name, repack_buffer_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f'Launching the repack test "just move" on VID {vid_to_repack}')
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_tape_repair(cta_cli, eos_client, eos_mgm, repack_buffer_dir, repack_mp_name, disk_instance_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    number_of_files_to_inject = 10
    repack_sub_dir = f"{repack_buffer_dir}/{vid_to_repack}"
    eos_mgm.exec(f"eos mkdir -p {repack_sub_dir}")
    eos_mgm.exec(f"eos chmod 1777 {repack_sub_dir}")
    print(f"Will inject {number_of_files_to_inject} files into the repack buffer directory")

    assert number_of_files_to_inject < _get_number_of_files_on_tape(cta_cli, vid_to_repack)

    tape_file_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tapefile ls --vid {vid_to_repack}"))

    for tape_file in tape_file_ls_json[:number_of_files_to_inject]:
        disk_id = tape_file["df"]["diskId"]
        file_path_to_inject = eos_mgm.exec_with_output(f"eos fileinfo fid:{disk_id} --path | cut -d':' -f2 | tr -d ' '")
        eos_client.retrieve_file(disk_instance_name, file_path_to_inject)
        print(f"Copying the retrieved file {file_path_to_inject} into the repack buffer {repack_sub_dir}")
        file_fseq = int(tape_file["tf"]["fSeq"])
        eos_mgm.exec(f"eos cp {file_path_to_inject} {repack_sub_dir}/{file_fseq:09d}")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching a repack request on VID {vid_to_repack}")
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    user_provided_files = int(repack_request["userProvidedFiles"])
    archived_files = int(repack_request["archivedFiles"])
    retrieved_files = int(repack_request["retrievedFiles"])
    total_files_to_retrieve = int(repack_request["totalFilesToRetrieve"])
    total_files_to_archive = int(repack_request["totalFilesToArchive"])

    assert total_files_to_retrieve == total_files_to_archive - user_provided_files
    assert retrieved_files == total_files_to_retrieve
    assert archived_files == total_files_to_archive

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_just_add_copies(cta_cli, disk_instance_name, repack_buffer_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f'Launching the repack test "just add copies" on VID {vid_to_repack} with all copies already on CTA')
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        add_copies_only=True,
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    archived_files = int(repack_request["archivedFiles"])
    retrieved_files = int(repack_request["retrievedFiles"])

    assert archived_files == 0
    assert retrieved_files == 0

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")


def test_repack_cancellation(
    cta_cli, disk_instance_name, repack_buffer_dir, repack_mp_name, postgres_scheduler_enabled
):
    cta_cli.set_all_drives_down()

    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching the repack request test on VID {vid_to_repack}")
    repack_process = multiprocessing.Process(
        target=submit_repack_request,
        args=(cta_cli, vid_to_repack, disk_instance_name, repack_buffer_dir, repack_mp_name),
        kwargs={"move_only": True},
    )
    repack_process.start()

    _wait_for_repack_request_launch(cta_cli, vid_to_repack)
    _wait_for_repack_request_expansion(cta_cli, vid_to_repack)

    nb_files_in_queue = _count_files_in_queue(cta_cli, vid_to_repack)
    print(f"Expansion finished with the following number of files in the retrieve queue: {nb_files_in_queue}")
    if postgres_scheduler_enabled:
        assert nb_files_in_queue > 0

    repack_process.terminate()
    repack_process.join()
    print("Background process safely terminated.")
    _remove_repack_request(cta_cli, vid_to_repack)

    print(
        f"Checking if the Retrieve queue of the VID {vid_to_repack} contains the Retrieve Requests created from the Repack Request expansion"
    )
    nb_files_on_tape_to_repack = len(
        json.loads(cta_cli.exec_with_output(f"cta-admin --json tapefile ls --vid {vid_to_repack}"))
    )
    print(f"Number of files on tape {vid_to_repack}: {nb_files_on_tape_to_repack}")

    nb_files_in_queue = _count_files_in_queue(cta_cli, vid_to_repack)
    print(f"After repack request deletion, number of files in the retrieve queue: {nb_files_in_queue}")
    if postgres_scheduler_enabled:
        assert nb_files_in_queue == 0
    else:
        # In case of the objectstore we expect the retrieve queue to still have jobs
        assert nb_files_in_queue == nb_files_on_tape_to_repack

    cta_cli.set_all_drives_up(wait=False)
    _wait_for_queue_to_empty(cta_cli, vid_to_repack)
    _remove_repack_request(cta_cli, vid_to_repack, throw_on_failure=False)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")


def test_repack_tape_repair_no_recall(
    cta_cli, eos_client, eos_mgm, repack_buffer_dir, repack_mp_name, disk_instance_name
):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    number_of_files_to_inject = 10
    print(f"Will inject {number_of_files_to_inject} files into the repack buffer directory")
    repack_sub_dir = f"{repack_buffer_dir}/{vid_to_repack}"
    eos_mgm.exec(f"eos mkdir -p {repack_sub_dir}")
    eos_mgm.exec(f"eos chmod 1777 {repack_sub_dir}")

    tape_file_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tapefile ls --vid {vid_to_repack}"))

    assert number_of_files_to_inject < _get_number_of_files_on_tape(cta_cli, vid_to_repack)

    for i in range(number_of_files_to_inject):
        tape_file = tape_file_ls_json[i + 1]
        disk_id = tape_file["df"]["diskId"]
        file_path_to_inject = eos_mgm.exec_with_output(f"eos fileinfo fid:{disk_id} --path | cut -d':' -f2 | tr -d ' '")
        eos_client.retrieve_file(disk_instance_name, file_path_to_inject)
        print(f"Copying the retrieved file {file_path_to_inject} into the repack buffer {repack_sub_dir}")
        file_fseq = int(tape_file["tf"]["fSeq"])
        eos_mgm.exec(f"eos cp {file_path_to_inject} {repack_sub_dir}/{file_fseq:09d}")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching a repack request on VID {vid_to_repack}")
    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
        move_only=True,
        no_recall=True,
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    user_provided_files = int(repack_request["userProvidedFiles"])
    archived_files = int(repack_request["archivedFiles"])
    retrieved_files = int(repack_request["retrievedFiles"])
    total_files_to_retrieve = int(repack_request["totalFilesToRetrieve"])
    total_files_to_archive = int(repack_request["totalFilesToArchive"])

    assert total_files_to_retrieve == total_files_to_archive - user_provided_files
    assert retrieved_files == total_files_to_retrieve
    assert archived_files == total_files_to_archive
    assert archived_files == user_provided_files

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")


# Keep this test for last - it adds new tapepools and archive routes
def test_repack_move_and_add_copies(
    cta_cli,
    disk_instance_name,
    repack_buffer_dir,
    repack_mp_name,
    postgres_scheduler_enabled,
    cta_storage_class,
    cta_default_tape_pool,
):
    if not postgres_scheduler_enabled:
        #   - The `repackMoveAndAddCopies` test has been problematic (frequent CI failures) due to a difficult-to-debug bug in the object store scheduler:
        #     - Despite the repack completing successfully (original tape is empty and all files have been moved to the new tapes),
        #       CTA fails to update the `repack` object, which causes the test to fail.
        #     - This should be fixed once we deprecate the object store backend and move to the new scheduler.
        #   - For more info check:
        #     - https://gitlab.cern.ch/cta/CTA/-/issues/990
        #     - https://gitlab.cern.ch/cta/CTA/-/issues/1114#note_9462287

        pytest.skip("Test does not work reliably with the objectstore")

    tp_dest1_default = "systest2_default"
    tp_dest2_default = "systest3_default"
    tp_dest2_repack = "systest3_repack"

    print(f"Creating 2 destination tapepools: {tp_dest1_default} and {tp_dest2_default}")
    cta_cli.exec(
        f"cta-cli -- cta-admin tapepool add --name {tp_dest1_default} --vo vo --partialtapesnumber 2 --comment 'Temp {tp_dest1_default} tapepool'"
    )
    cta_cli.exec(
        f"cta-cli -- cta-admin tapepool add --name {tp_dest2_default} --vo vo --partialtapesnumber 2 --comment 'Temp {tp_dest2_default} tapepool'"
    )
    print(f"Creating 1 destination tapepool for repack: {tp_dest2_repack} (will override {tp_dest2_default})")
    cta_cli.exec(
        f"cta-cli -- cta-admin tapepool add --name {tp_dest2_repack} --vo vo --partialtapesnumber 2 --comment 'Temp {tp_dest2_repack} repack tapepool'"
    )

    print("Creating archive routes for adding two copies of the file")
    cta_cli.exec(
        f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 2 --tapepool {tp_dest1_default} --comment 'ArchiveRoute2_default'"
    )
    cta_cli.exec(
        f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 3 --tapepool --archiveroutetype DEFAULT {tp_dest2_default} --comment 'ArchiveRoute3_default'"
    )
    cta_cli.exec(
        f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 3 --tapepool --archiveroutetype REPACK {tp_dest2_repack} --comment 'ArchiveRoute3_repack'"
    )

    print("Changing the tapepool of tapes")
    all_tapes = json.loads(cta_cli.exec_with_output("cta-cli -- cta-admin --json tape ls --all"))
    all_vids = [tape.vid for tape in all_tapes]
    assert len(all_vids) > 0

    all_tape_pools = json.loads(cta_cli.exec_with_output("cta-cli -- cta-admin --json tapepool ls --all"))
    assert len(all_tape_pools) > 0

    nb_tapes_per_tape_pool = int(len(all_vids) / len(all_tape_pools))

    print("Redistributing tapes across remaining tape pools, keeping the first tape pool unchanged.")
    count_changing = 0
    tape_pool_index = 1
    start = nb_tapes_per_tape_pool + (len(all_vids) % len(all_tape_pools))
    for vid in all_vids[start:]:
        tape_pool = all_tape_pools[tape_pool_index].name
        cta_cli.exec(f"cta-cli -- cta-admin tape ch --vid {vid} --tapepool {tape_pool}")

        count_changing += 1
        if count_changing % nb_tapes_per_tape_pool == 0:
            tape_pool_index += 1

    print(f"Changing the storage class {cta_storage_class} nb copies")
    cta_cli.exec(f"cta-admin storageclass ch --name {cta_storage_class} --numberofcopies 3")

    cta_cli.set_all_drives_up(wait=False)
    vid_to_repack = _get_first_vid_containing_files(cta_cli)

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f'Launching the repack "Move and add copies" test on VID {vid_to_repack}')
    # Note this process starts in the background. We cannot use exec_async because futures are not cancellable when running

    submit_repack_request(
        cta_cli,
        vid_to_repack=vid_to_repack,
        disk_instance_name=disk_instance_name,
        repack_buffer_dir=repack_buffer_dir,
        mount_policy_name=repack_mp_name,
    )
    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    archived_files = int(repack_request["archivedFiles"])
    retrieved_files = int(repack_request["retrievedFiles"])
    total_files_to_retrieve = int(repack_request["totalFilesToRetrieve"])
    total_files_to_archive = int(repack_request["totalFilesToArchive"])

    assert retrieved_files == total_files_to_retrieve
    assert archived_files == total_files_to_archive

    print(
        "Checking that 2 copies were to the default tape pool (archive route 1 and 2) and 1 copy to repack tape pool (archive route 3)"
    )

    repack_destinations = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    tape_pool_list = []

    for destination in repack_destinations:
        for dest_info in destination["destinationInfos"]:
            tape = json.loads(cta_cli.exec_with_output(f"cta-admin --json tape ls --vid {dest_info['vid']}"))
            tape_pool_list.extend(t["tapepool"] for t in tape)

    assert cta_default_tape_pool in tape_pool_list
    assert tp_dest1_default in tape_pool_list
    assert tp_dest2_repack in tape_pool_list

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_add_errors_to_whitelist(error_whitelist):
    error_whitelist.add("Aborting recall mount startup: empty mount")
    error_whitelist.add("Notified client of end session with error")
    error_whitelist.add("In OStoreDB::RepackArchiveReportBatch::report(): async job update failed.")
    # Rest of the error is "root://ctaeos//eos/ctaeos/repack/ULT101/ directory" However, we just do a partial match to prevent having to specify the exact dir
    error_whitelist.add("In OStoreDB::RepackArchiveReportBatch::report(): failed to remove the root://")
