# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from ..helpers.utils import Timeout

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
def repack_report_dir(request, repack_base_dir):
    return repack_base_dir / f"{request.function.__name__}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="module")
def repack_buffer_url(eos_mgm) -> str:
    path = eos_mgm.base_dir_path / f"repack_{uuid.uuid4().hex[:6]}"
    eos_mgm.exec(f"eos mkdir -p {path}")
    eos_mgm.exec(f"eos chmod 1777 {path}")
    return path


def _get_first_vid_containing_files(cta_cli):
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


def _remove_repack_request(cta_cli, vid: str) -> None:
    print(f"Removing repack request for {vid}")
    cta_cli.exec(f"cta-admin repack rm --vid {vid}")


def _reclaim_tape(cta_cli, vid: str) -> None:
    print(f"Reclaiming tape {vid}")
    cta_cli.exec(f"cta-admin tape reclaim --vid {vid}")


def _has_repack_request(cta_cli, vid) -> bool:
    re_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
    return len(re_ls_json) > 0


def _retrieve_queue_empty(cta_cli, vid) -> int:
    return (
        int(cta_cli.exec_with_output(f'cta-admin --json showqueues | jq -r \'. [] | select(.vid == "{vid}") | wc -l'))
        == 0
    )


def _count_files_in_queue(cta_cli, vid) -> int:
    return cta_cli.exec_with_output(
        f"cta-admin --json showqueues | jq -r '. [] | select(.vid == \"{vid}\") | .queuedFiles'"
    )


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
        repack_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
        lastExpandedFSeq = repack_json[0]["lastExpandedFseq"]
        while lastExpandedFSeq != lastFSeq and not t.expired:
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
# Tests
#####################################################################################################################


def test_setup_client(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_setup.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_helper.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "cli_calls.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_repack.sh"), "/tmp/", permissions="+x")


def test_create_repack_vo(cta_cli, repack_vo_name, disk_instance_name):
    cta_cli.exec(
        f'cta-admin virtualorganization add --vo {repack_vo_name} --readmaxdrives 1 --writemaxdrives 1 --diskinstance {disk_instance_name} --isrepackvo true --comment "vo for repack"'
    )


def test_create_repack_mount_policy(cta_cli, repack_mp_name):
    # This mount policy is for repack: IT MUST CONTAIN THE `repack` STRING IN IT TO ALLOW MOUNTING DISABLED TAPES
    cta_cli.exec(
        f'cta-admin mountpolicy add --name {repack_mp_name} --archivepriority 2 --minarchiverequestage 1 --retrievepriority 2 --minretrieverequestage 1 --comment "repack mountpolicy for ctasystest"'
    )


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


def test_roundtrip_repack_with_backpressure(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")

    print(f'Launching the repack "just move" test on VID {vid_to_repack} (with backpressure)')
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -p -n {repack_mp_name}"
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_roundtrip_repack_no_backpressure(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")

    print(f'Launching the repack "just move" test on VID {vid_to_repack}')
    # Note the missing -p flag
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name}"
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_non_repacking_tape(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)

    # TODO: we may want to parameterize this test in the future to remove some of this duplication and improve test granularity
    _modify_tape_state(cta_cli, vid_to_repack, "DISABLED")
    print(f"Launching the repack request test on VID {vid_to_repack} with DISABLED state")
    with pytest.raises(RuntimeError):
        eos_client.exec(
            f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
        )
        print("The repack command should have failed as the tape is DISABLED")

    _modify_tape_state(cta_cli, vid_to_repack, "BROKEN")
    print(f"Launching the repack request test on VID {vid_to_repack} with BROKEN state")
    with pytest.raises(RuntimeError):
        eos_client.exec(
            f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
        )
        print("The repack command should have failed as the tape is BROKEN")

    _modify_tape_state(cta_cli, vid_to_repack, "EXPORTED")
    print(f"Launching the repack request test on VID {vid_to_repack} with EXPORTED state")
    with pytest.raises(RuntimeError):
        eos_client.exec(
            f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
        )
        print("The repack command should have failed as the tape is EXPORTED")

    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    print(f"Launching the repack request test on VID {vid_to_repack} with ACTIVE state")
    with pytest.raises(RuntimeError):
        eos_client.exec(
            f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
        )
        print("The repack command should have failed as the tape is ACTIVE")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching the repack request test on VID {vid_to_repack} with REPACKING state")
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
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


def test_repack_just_move_with_max_files(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    total_files_on_tape = _get_number_of_files_on_tape(cta_cli, vid_to_repack)
    number_of_files_to_repack = int(total_files_on_tape / 2)  # Number that covers only some of the files on tape (half)

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(
        f'Launching the repack test "just move", with {number_of_files_to_repack}/{total_files_on_tape} files, on VID {vid_to_repack}'
    )
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name} -f {number_of_files_to_repack}"
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    with pytest.raises(RuntimeError):
        _reclaim_tape(cta_cli, vid_to_repack)
        print("The reclaim command should have failed because the tape should still contain files")

    files_left_on_tape = _get_number_of_files_on_tape(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(
        f'Launching the repack test "just move", with the remainder {files_left_on_tape} files, on VID {vid_to_repack}'
    )
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name} -f {files_left_on_tape}"
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


def test_repack_just_move(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f'Launching the repack test "just move" on VID {vid_to_repack}')
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name}"
    )

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_tape_repair(
    cta_cli, eos_client, eos_mgm, request, repack_buffer_url, repack_report_dir, repack_mp_name, disk_instance_name
):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    number_of_files_to_inject = 10
    print(f"Will inject {number_of_files_to_inject} files into the repack buffer directory")
    buffer_dir = repack_buffer_url / f"{request.function.__name__}_{uuid.uuid4().hex[:6]}"
    eos_mgm.exec(f"eos mkdir -p {buffer_dir}")
    eos_mgm.exec(f"eos chmod 1777 {buffer_dir}")

    tape_file_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tapefile ls --vid {vid_to_repack}"))

    # TODO: in parallel?
    for i in range(number_of_files_to_inject):
        disk_id = tape_file_ls_json[i]["df"]["diskId"]
        file_path_to_inject = eos_mgm.exec_with_output(f"eos fileinfo fid:{disk_id} --path | cut -d':' -f2 | tr -d ' '")
        eos_client.retrieve_file(disk_instance_name, file_path_to_inject)
        print(f"Copying the retrieved file {file_path_to_inject} into the repack buffer {buffer_dir}")
        file_fseq = int(tape_file_ls_json[i]["tf"]["fSeq"])
        eos_mgm.exec(f"eos cp {file_path_to_inject} {buffer_dir}/{file_fseq:09d}")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching a repack request on VID {vid_to_repack}")
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name}"
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    user_provided_files = repack_request["userProvidedFiles"]
    archived_files = repack_request["archivedFiles"]
    retrieved_files = repack_request["retrievedFiles"]
    total_files_to_retrieve = repack_request["totalFilesToRetrieve"]
    total_files_to_archive = repack_request["totalFilesToArchive"]

    assert total_files_to_retrieve == total_files_to_archive - user_provided_files
    assert retrieved_files == total_files_to_retrieve
    assert archived_files == total_files_to_archive

    # For now manually remove the files in EOS; they were copied there by root so it seems that the tape daemon is not allowed to remove them even though the permissions are set 1777
    eos_mgm.force_remove_directory(buffer_dir)
    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


def test_repack_just_add_copies(cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f'Launching the repack test "just add copiese" on VID {vid_to_repack} with all copies already on CTA')
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -a -r {repack_report_dir} -n {repack_mp_name}"
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    archived_files = repack_request["archivedFiles"]
    retrieved_files = repack_request["retrieved_files"]

    assert archived_files == 0
    assert retrieved_files == 0

    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")


def test_repack_cancellation(
    cta_cli, eos_client, repack_buffer_url, repack_report_dir, repack_mp_name, postgres_scheduler_enabled
):
    cta_cli.set_all_drives_down()

    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching the repack request test on VID {vid_to_repack}")
    # Note this process starts in the background. We cannot use exec_async because futures are not cancellable when running
    background_repack_test_pid = eos_client.exec_with_output(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name} & echo $!"
    )

    _wait_for_repack_request_launch(cta_cli, vid_to_repack)
    _wait_for_repack_request_expansion(cta_cli, vid_to_repack)

    nb_files_in_queue = _count_files_in_queue(cta_cli, vid_to_repack)
    print(f"Expansion finished with the following number of files in the retrieve queue: {nb_files_in_queue}")
    if postgres_scheduler_enabled:
        assert nb_files_in_queue > 0

    eos_client.exec(f"kill {background_repack_test_pid}")
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
    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")


def test_repack_tape_repair_no_recall(
    cta_cli, eos_client, eos_mgm, request, repack_buffer_url, repack_report_dir, repack_mp_name, disk_instance_name
):
    vid_to_repack = _get_first_vid_containing_files(cta_cli)
    number_of_files_to_inject = 10
    print(f"Will inject {number_of_files_to_inject} files into the repack buffer directory")
    buffer_dir = repack_buffer_url / f"{request.function.__name__}_{uuid.uuid4().hex[:6]}"
    eos_mgm.exec(f"eos mkdir -p {buffer_dir}")
    eos_mgm.exec(f"eos chmod 1777 {buffer_dir}")

    tape_file_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json tapefile ls --vid {vid_to_repack}"))

    # TODO: in parallel?
    for i in range(number_of_files_to_inject):
        disk_id = tape_file_ls_json[i]["df"]["diskId"]
        file_path_to_inject = eos_mgm.exec_with_output(f"eos fileinfo fid:{disk_id} --path | cut -d':' -f2 | tr -d ' '")
        eos_client.retrieve_file(disk_instance_name, file_path_to_inject)
        print(f"Copying the retrieved file {file_path_to_inject} into the repack buffer {buffer_dir}")
        file_fseq = int(tape_file_ls_json[i]["tf"]["fSeq"])
        eos_mgm.exec(f"eos cp {file_path_to_inject} {buffer_dir}/{file_fseq:09d}")

    _modify_tape_state(cta_cli, vid_to_repack, "REPACKING")
    print(f"Launching a repack request on VID {vid_to_repack}")
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -m -r {repack_report_dir} -n {repack_mp_name} -u"
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    user_provided_files = repack_request["userProvidedFiles"]
    archived_files = repack_request["archivedFiles"]
    retrieved_files = repack_request["retrievedFiles"]
    total_files_to_retrieve = repack_request["totalFilesToRetrieve"]
    total_files_to_archive = repack_request["totalFilesToArchive"]

    assert total_files_to_retrieve == total_files_to_archive - user_provided_files
    assert retrieved_files == total_files_to_retrieve
    assert archived_files == total_files_to_archive
    assert archived_files == user_provided_files

    # For now manually remove the files in EOS; they were copied there by root so it seems that the tape daemon is not allowed to remove them even though the permissions are set 1777
    eos_mgm.force_remove_directory(buffer_dir)
    _remove_repack_request(cta_cli, vid_to_repack)
    _modify_tape_state(cta_cli, vid_to_repack, "ACTIVE")
    _reclaim_tape(cta_cli, vid_to_repack)


# Keep this test for last - it adds new tapepools and archive routes
def test_repack_move_and_add_copies(
    cta_cli,
    eos_client,
    repack_buffer_url,
    repack_report_dir,
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
    tp_dest2_default = "systest2_default"
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
        f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 3 --tapepool --archiveroutetype DEFAULT {tp_dest1_default} --comment 'ArchiveRoute3_default'"
    )
    cta_cli.exec(
        f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 3 --tapepool --archiveroutetype REPACK {tp_dest2_default} --comment 'ArchiveRoute3_repack'"
    )
    cta_cli.exec(
        f"cta-cli -- cta-admin tapepool add --name {tp_dest2_default} --vo vo --partialtapesnumber 2 --comment 'Temp {tp_dest2_repack} tapepool'"
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
    eos_client.exec(
        f"/tmp/test_repack.sh -v {vid_to_repack} -b {repack_buffer_url} -r {repack_report_dir} -n {repack_mp_name}"
    )

    repack_ls_json = json.loads(cta_cli.exec_with_output(f"cta-admin --json repack ls --vid {vid_to_repack}"))
    repack_request = repack_ls_json[0]
    archived_files = repack_request["archivedFiles"]
    retrieved_files = repack_request["retrievedFiles"]
    total_files_to_retrieve = repack_request["totalFilesToRetrieve"]
    total_files_to_archive = repack_request["totalFilesToArchive"]

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
