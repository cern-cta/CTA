# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import re
import shlex
import sys
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Union, cast
from _pytest.fixtures import SubRequest

import fastjsonschema
import pytest

from system_tests.helpers.hosts import CtaCliHost, CtaMaintdHost, CtaTapedHost, EosClientHost, EosMgmHost
from system_tests.helpers.test_config import TestConfig as SystemTestConfig
from system_tests.helpers.test_env import TestEnv

# =========================================================================
#  Helpers
# =========================================================================


@dataclass
class ClientParams:
    file_count: int
    file_size_kb: int
    process_count: int


@dataclass
class PrepareTestPaths:
    tape_file: Path
    no_prepare_dir: Path
    missing_dir: Path


def _response_for_path(response: dict[str, Any], path: Path) -> dict[str, Any]:
    """Find the per-path result in a multi-file PREPARE response."""
    return next(item for item in response["responses"] if item["path"] == str(path))


def _run_eosdf_test(eos_client: EosClientHost, cta_cli: CtaCliHost, disk_instance_name: str, test_dir: Path) -> None:
    disk_system = "eosdfBuffer"
    disk_instance_space = "eosdfDiskInstanceSpace"
    file_path = test_dir / "testfile1_eosdf"

    # Remove resources just in case. They normally do not exist, so both commands may fail as expected
    print("The following cleanup commands may fail because the EOSDF resources are normally absent")
    cta_cli.exec(f"cta-admin ds rm -n '{disk_system}'", throw_on_failure=False)
    cta_cli.exec(f"cta-admin dis rm -n '{disk_instance_space}' --di '{disk_instance_name}'", throw_on_failure=False)

    try:
        cta_cli.exec(
            f"cta-admin dis add -n '{disk_instance_space}' --di '{disk_instance_name}' "
            "-u 'eosSpace:default' -i 1 -m 'EOSDF test'"
        )
        cta_cli.exec(
            f"cta-admin ds add -n '{disk_system}' --di '{disk_instance_name}' --dis '{disk_instance_space}' "
            "-r '.*/eos/.*' -f 1 -s 20 -m 'EOSDF test'"
        )

        print("The following file lookup may fail as expected if the EOSDF test file has not been created yet")
        file_info = eos_client.exec(f"eos root://{disk_instance_name} fileinfo '{file_path}'", throw_on_failure=False)
        if not file_info.success:
            source_path = Path("/tmp/testfile1_eosdf")
            eos_client.exec(f"printf foo > '{source_path}'")
            eos_client.archive_file(disk_instance_name, file_path, source_path, wait=True)

        eos_client.retrieve_file(disk_instance_name, file_path)
        result = eos_client.evict_prepare(disk_instance_name, [file_path])
        assert result.success, result.stderr
    finally:
        cta_cli.exec(f"cta-admin ds rm -n '{disk_system}'", throw_on_failure=False)
        cta_cli.exec(f"cta-admin dis rm -n '{disk_instance_space}' --di '{disk_instance_name}'", throw_on_failure=False)


@pytest.fixture(scope="module")
def client_params(test_config: SystemTestConfig) -> ClientParams:
    client_config = test_config["tests"]["client"]
    return ClientParams(
        file_count=client_config["file_count"],
        file_size_kb=client_config["file_size_kb"],
        process_count=client_config["process_count"],
    )


@pytest.fixture(scope="class")
def prepare_test_paths(
    eos_client: EosClientHost,
    eos_mgm: EosMgmHost,
    cta_cli: CtaCliHost,
    disk_instance_name: str,
    test_dir: Path,
) -> PrepareTestPaths:
    no_prepare_dir = test_dir / "no_prepare"
    missing_dir = test_dir / "none"
    eos_mgm.exec(f"eos mkdir -p '{no_prepare_dir}'")
    eos_mgm.exec(
        f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d '{no_prepare_dir}'"
    )
    cta_cli.set_all_drives_up()
    tape_file = eos_client.generate_and_archive_file(
        disk_instance_name, test_dir / "idempotent_prepare", append_uid=True
    )
    return PrepareTestPaths(tape_file, no_prepare_dir, missing_dir)


# =========================================================================
#  Tests
# =========================================================================

# Some scripts probably deserve to be their own test module instead of cramming everything in this file


def test_setup_client(
    eos_client: EosClientHost, client_params: ClientParams, test_dir: Path, remote_scripts_dir: Path
) -> None:
    # Install the shared client scripts before generating the archive/retrieve test data
    # This should eventually be removed once all those scripts have been migrated to python
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "client_setup.sh", Path("/tmp"), permissions="+x")
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "client_helper.sh", Path("/tmp"), permissions="+x")
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "cli_calls.sh", Path("/tmp"), permissions="+x")
    eos_client.exec(
        f"/tmp/client_setup.sh -n {client_params.file_count} -s {client_params.file_size_kb} -p "
        f"{client_params.process_count} -d {test_dir} -r -c xrd"
    )


###
# TPC capabilities
#
# if the installed xrootd version is not the same as the one used to compile eos
# Third Party Copy can be broken
#
# Valid check:
# [ ~]# xrdfs
# root://pps-ngtztag6p5ht-minion-2.cern.ch:1101 query config tpc
# 1
#
# invalid check:
# [ ~]# xrdfs root://ctaeos.toto.svc.cluster.local:1095 query config tpc
# tpc
def test_eos_xrootd_third_party_copy_capabilities(eos_mgm: EosMgmHost, disk_instance_name: str) -> None:
    """Verifies that all online EOS FST nodes have xrootd TPC capabilities enabled."""
    del disk_instance_name  # This fixture ensures that a disk instance is available.

    # Fetch nodes
    node_ls_raw = eos_mgm.exec_with_output("eos -j root://localhost node ls")
    node_envelope = json.loads(node_ls_raw)
    node_data = node_envelope.get("result", [])

    # Filter for online nodes and extract their hostport addresses
    online_hostports = [node["hostport"] for node in node_data if node.get("status") == "online" and "hostport" in node]

    assert online_hostports, "No online FST nodes were found to test!"

    failed_nodes = []
    print(f"Checking xrootd TPC capabilities on {len(online_hostports)} online FSTs...")

    for hostport in online_hostports:
        # Querying the node config for tpc capability (returns '1' if enabled)
        tpc_query_cmd = f"xrdfs root://{hostport} query config tpc"

        result = eos_mgm.exec_with_output(tpc_query_cmd, throw_on_failure=False)
        if "1" in result:
            print(f"{hostport}: OK")
        else:
            print(f"{hostport}: KO (Result: {result.strip()})")
            failed_nodes.append(hostport)

    assert not failed_nodes, f"TPC capabilities validation FAILED for the following nodes: {failed_nodes}"


def test_eos_xrootd_api_fts_compliance(eos_mgm: EosMgmHost) -> None:
    """Verifies that xrdfs query prepare preserves the exact requested sequence order and duplicates.

    Write 3 files and xrdfs query them in reverse order with duplicates.
    `xrdfs query prepare 3 2 1 3` must answer 3 2 1 3.
    """
    tmp_dir = eos_mgm.base_dir_path / f"tmp_xrd_fts_compliance_{str(uuid.uuid4())[:8]}"

    eos_mgm.exec(f"eos mkdir -p {tmp_dir}")
    eos_mgm.exec(f"eos chmod 777 {tmp_dir}")

    # Define the exact tracking order payload (including intentional duplicate sequence)
    file_sequence = ["3", "2", "1", "3"]
    input_paths = [f"{tmp_dir}/{name}" for name in file_sequence]

    unique_paths = set(input_paths)
    for path in unique_paths:
        eos_mgm.exec(f"eos touch {path}")

    # Request prepare configuration via xrdfs in the original sequence layout
    paths_payload = " ".join(input_paths)
    print(f"Checking xrootd API FTS compliance querying: {paths_payload}")

    # xrdfs query prepare 0 <paths...> returns JSON detailing the responses arrays
    query_output = eos_mgm.exec_with_output(f"xrdfs root://localhost query prepare 0 {paths_payload}")

    response_data = json.loads(query_output)
    # Safely extract paths from the JSON response items mapping
    output_paths = [item["path"] for item in response_data.get("responses", [])]

    # Both the order and elements must match exactly
    assert input_paths == output_paths, (
        "FTS Compliance Failed! The xrdfs query prepare did not maintain the original request sequence."
    )

    print("xrootd_API capabilities: SUCCESS")

    eos_mgm.force_remove_directory(tmp_dir)


def test_simple_archive_retrieve(
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    test_dir: Path,
    disk_instance_name: str,
) -> None:
    # Archive a fresh file and inspect its initial tape-backed state
    cta_cli.set_all_drives_up()
    file_path = test_dir / "test_simple_archive_retrieve"
    file_path = eos_client.generate_and_archive_file(
        disk_instance_name, destination_path=file_path, wait=True, append_uid=True
    )
    print("Information about the archived testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    # Recall the file to disk, then evict its disk replica again.
    eos_client.retrieve_file(disk_instance_name, file_path, wait=True)
    print("Information about the retrieved testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    # The original test attempts to evict as poweruser1, but this does not work
    eos_client.evict_file(disk_instance_name, file_path, user="eosadmin1", wait=True)
    print("Information about the evicted testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    # Remove the namespace entry once the full lifecycle has been exercised
    eos_client.delete_file(disk_instance_name, file_path)


def test_archive(eos_client: EosClientHost, remote_scripts_dir: Path) -> None:
    # Run a bulk archive
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "test_archive.sh", Path("/tmp"), permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_archive.sh")
    # TODO: replace by something more deterministic. Is this even necessary?
    print("Sleeping 5 seconds to allow MGM-FST communication to settle after disk copy deletion.")
    time.sleep(5)


def test_retrieve(eos_client: EosClientHost, remote_scripts_dir: Path) -> None:
    # Recall the files created by test_archive
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "test_retrieve.sh", Path("/tmp"), permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_retrieve.sh")


def test_evict(eos_client: EosClientHost, remote_scripts_dir: Path) -> None:
    # Evict the recalled files from test_retrieve
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "test_evict.sh", Path("/tmp"), permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_evict.sh")


def test_abort_prepare(
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    client_params: ClientParams,
    disk_instance_name: str,
    test_dir: Path,
) -> None:
    # This test assumes the archive directory contains only files that have been evicted from disk. This works because
    # test_archive and test_evict run first.
    # Once those scripts are migrated, this test should use a dedicated directory.
    archive_directories = eos_client.exec_with_output(f"eos root://{disk_instance_name} ls '{test_dir}'").splitlines()
    assert len(archive_directories) == 1, f"Expected one archive directory below {test_dir}: {archive_directories}"
    archive_directory = test_dir / archive_directories[0]
    # Set all drives down to ensure requests stay in the queue and can be aborted
    cta_cli.wait_for_drives_to_stop_transferring()
    cta_cli.set_all_drives_down()

    # Submit retrieve requests
    requests = eos_client.retrieve_directory(
        disk_instance_name,
        archive_directory,
        wait=False,
        activity="T0Reprocess",
        parallelism=client_params.process_count,
    )
    assert requests, f"No archived files found below {archive_directory}"

    # Wait for them to queue
    deadline = time.monotonic() + 20
    while cta_cli.retrieve_queue_file_count() != len(requests) and time.monotonic() < deadline:
        time.sleep(1)
    assert cta_cli.retrieve_queue_file_count() == len(requests), "Not all retrieve requests were queued"

    # Abort every queued PREPARE request
    eos_client.abort_files(disk_instance_name, requests, parallelism=client_params.process_count)

    # CTA clears the cancelled requests when the retrieve queues are processed. Do not wait for UP here because the
    # drives can immediately transition to TRANSFERING while consuming the queues.
    cta_cli.set_all_drives_up(wait=False)
    cta_cli.wait_for_queue_to_empty(wait_timeout_secs=20)

    # Ensure that the files were not actually retrieved on EOS
    retrieved_file = eos_client.exec(
        f"eos root://{shlex.quote(disk_instance_name)} find -f {shlex.quote(str(archive_directory))} | "
        f"xargs -r -P {client_params.process_count} -I{{}} "
        f"eos root://{shlex.quote(disk_instance_name)} ls -y '{{}}' | "
        "grep -qE '^d[1-9][0-9]*::t1'",
        throw_on_failure=False,
    )
    assert not retrieved_file.success, "Some files were retrieved despite cancellation"


def test_multiple_retrieve(
    cta_cli: CtaCliHost, eos_client: EosClientHost, test_dir: Path, remote_scripts_dir: Path
) -> None:
    # Restore the drives before doing concurrent retrieve
    # Assumes test_evict ran before this as this is the directory it's operating on
    cta_cli.set_all_drives_up()
    eos_client.copy_to(remote_scripts_dir / "eos_client" / "test_multiple_retrieve.sh", Path("/tmp"), permissions="+x")
    eos_client.exec(f". /tmp/client_env && /tmp/test_multiple_retrieve.sh {test_dir}")


class TestPrepare:
    """Verify that PREPARE handles every path independently and idempotently.

    A failing path must not affect valid paths in the same stage request, although stage returns an error when every
    path fails. Query responses must identify failed paths through error_text. Abort and evict operate on every path but
    return an error if any individual path fails.
    """

    def test_prepare_existing_file(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
    ) -> None:
        # Keep the request in the queue so its query response can be inspected deterministically
        cta_cli.set_all_drives_down()
        result = eos_client.prepare_files(disk_instance_name, [prepare_test_paths.tape_file])
        assert result.success, result.stderr
        # A valid tape file must be represented as an active request without an error
        response = _response_for_path(
            eos_client.query_prepare(disk_instance_name, result.stdout.strip(), [prepare_test_paths.tape_file]),
            prepare_test_paths.tape_file,
        )
        assert response == {
            **response,
            "path_exists": True,
            "requested": True,
            "has_reqid": True,
            "error_text": "",
        }

    def test_prepare_missing_file_fails(
        self, eos_client: EosClientHost, disk_instance_name: str, prepare_test_paths: PrepareTestPaths
    ) -> None:
        # A request containing only a nonexistent path must fail immediately
        missing_file = prepare_test_paths.missing_dir / str(uuid.uuid4())
        print("The following PREPARE request is expected to fail because the file does not exist")
        result = eos_client.prepare_files(disk_instance_name, [missing_file])
        assert not result.success

    @pytest.mark.parametrize("all_forbidden", [False, True], ids=["one-of-two", "all"])
    def test_prepare_without_permission(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
        all_forbidden: bool,
    ) -> None:
        # Create files whose ACL permits access but explicitly denies PREPARE requests
        if not all_forbidden:
            cta_cli.set_all_drives_down()
        forbidden_files = [
            prepare_test_paths.no_prepare_dir / str(uuid.uuid4()) for _ in range(2 if all_forbidden else 1)
        ]
        for path in forbidden_files:
            eos_client.exec(f"KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp /etc/group root://{disk_instance_name}/{path}")
        # A mixed request succeeds overall, while a request with no valid path fails
        paths = forbidden_files if all_forbidden else [prepare_test_paths.tape_file, forbidden_files[0]]
        if all_forbidden:
            print("The following PREPARE request is expected to fail because every path is forbidden")
        result = eos_client.prepare_files(disk_instance_name, paths)
        assert result.success is not all_forbidden
        # Query the mixed request to ensure the forbidden path did not affect the valid one
        if not all_forbidden:
            response = eos_client.query_prepare(disk_instance_name, result.stdout.strip(), paths)
            failed = _response_for_path(response, forbidden_files[0])
            valid = _response_for_path(response, prepare_test_paths.tape_file)
            assert failed["path_exists"] is True
            assert failed["requested"] is False
            assert failed["has_reqid"] is False
            assert failed["error_text"]
            assert valid["error_text"] == ""

    def test_prepare_with_valid_and_missing_file(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
    ) -> None:
        # Combine a valid tape file with a missing path and keep the valid request queued for inspection
        cta_cli.set_all_drives_down()
        missing_file = prepare_test_paths.missing_dir / str(uuid.uuid4())
        paths = [prepare_test_paths.tape_file, missing_file]
        result = eos_client.prepare_files(disk_instance_name, paths)
        assert result.success

        # The missing path reports its own failure without affecting the valid path
        response = eos_client.query_prepare(disk_instance_name, result.stdout.strip(), paths)
        failed = _response_for_path(response, missing_file)
        valid = _response_for_path(response, prepare_test_paths.tape_file)
        assert failed["path_exists"] is False
        assert failed["requested"] is False
        assert failed["has_reqid"] is False
        assert failed["error_text"]
        assert valid["error_text"] == ""

    def test_prepare_with_only_missing_files(
        self,
        eos_client: EosClientHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
    ) -> None:
        # A PREPARE request containing no valid paths must fail outright
        missing_files = [prepare_test_paths.missing_dir / str(uuid.uuid4()) for _ in range(2)]
        print("The following PREPARE request is expected to fail because every file is missing")
        result = eos_client.prepare_files(disk_instance_name, missing_files)
        assert not result.success

    def test_prepare_multiple_file_response(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
    ) -> None:
        # Create enough tape, forbidden, and missing files to exercise a heterogeneous response
        tape_files = [prepare_test_paths.tape_file]
        cta_cli.set_all_drives_up()
        tape_files.extend(
            [
                eos_client.generate_and_archive_file(
                    disk_instance_name,
                    prepare_test_paths.tape_file.parent / "prepare_multi",
                    append_uid=True,
                    wait_timeout_secs=90,
                )
                for _ in range(3)
            ]
        )
        cta_cli.set_all_drives_down()
        forbidden_files = [prepare_test_paths.no_prepare_dir / str(uuid.uuid4()) for _ in range(4)]
        missing_files = [prepare_test_paths.missing_dir / str(uuid.uuid4()) for _ in range(4)]
        for path in forbidden_files:
            eos_client.exec(f"KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp /etc/group root://{disk_instance_name}/{path}")
        paths = tape_files + forbidden_files + missing_files
        # Submit all path categories together and validate every response entry
        result = eos_client.prepare_files(disk_instance_name, paths)
        assert result.success
        assert result.stdout.strip()
        assert not result.stdout.strip().isspace()
        response = eos_client.query_prepare(disk_instance_name, result.stdout.strip(), paths)
        assert len(response["responses"]) == len(paths)
        assert all(_response_for_path(response, path)["path_exists"] for path in tape_files + forbidden_files)
        assert all(_response_for_path(response, path)["error_text"] == "" for path in tape_files)
        assert all(_response_for_path(response, path)["error_text"] for path in forbidden_files + missing_files)
        assert all(not _response_for_path(response, path)["path_exists"] for path in missing_files)

    @pytest.mark.parametrize("include_missing", [False, True], ids=["success", "partial-failure"])
    def test_prepare_abort(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        prepare_test_paths: PrepareTestPaths,
        include_missing: bool,
    ) -> None:
        # Queue a valid PREPARE request, optionally aborting it alongside a missing path
        cta_cli.set_all_drives_down()
        prepare = eos_client.prepare_files(disk_instance_name, [prepare_test_paths.tape_file])
        assert prepare.success
        paths = [prepare_test_paths.tape_file]
        if include_missing:
            paths.append(prepare_test_paths.missing_dir / str(uuid.uuid4()))
            print("The following PREPARE abort is expected to fail for the missing path")
        abort = eos_client.abort_prepare(
            disk_instance_name,
            prepare.stdout.strip(),
            paths,
        )
        assert abort.success is not include_missing
        # The valid path must be cancelled even when another path makes the command fail
        response = _response_for_path(
            eos_client.query_prepare(disk_instance_name, prepare.stdout.strip(), [prepare_test_paths.tape_file]),
            prepare_test_paths.tape_file,
        )
        assert response["path_exists"] is True
        assert response["requested"] is False
        assert response["has_reqid"] is False
        assert response["error_text"] == ""

    @pytest.mark.parametrize("include_missing", [False, True], ids=["success", "partial-failure"])
    def test_prepare_evict(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        test_dir: Path,
        include_missing: bool,
    ) -> None:
        # Put a freshly archived file on disk before requesting its eviction
        cta_cli.set_all_drives_up()
        file_path = eos_client.generate_and_archive_file(
            disk_instance_name, test_dir / "prepare_evict", append_uid=True
        )
        eos_client.retrieve_file(disk_instance_name, file_path)
        paths = [file_path]
        if include_missing:
            paths.append(test_dir / "none" / str(uuid.uuid4()))
            print("The following PREPARE eviction is expected to fail for the missing path")
        # A missing companion path changes the command status but must not block the valid eviction
        result = eos_client.evict_prepare(disk_instance_name, paths)
        assert result.success is not include_missing
        eos_client.wait_for_file_eviction(disk_instance_name, file_path)


def test_delete_on_closew_error(eos_client: EosClientHost, disk_instance_name: str, test_dir: Path) -> None:
    # An archival failure before the archive request is queued triggers delete-on-close. EOS must remove every replica
    # and the namespace entry without ever queueing an archive request.
    test_directory = test_dir / "fail_on_closew_test"
    test_file = test_directory / str(uuid.uuid4())
    eos_admin = f"KRB5CCNAME=/tmp/eosadmin1/krb5cc_0 eos -r 0 0 root://{disk_instance_name}"

    # Configure a storage class that makes the CLOSEW workflow fail before CTA queues an archive request
    print("The following cleanup command may fail because the test directory is normally absent")
    eos_client.exec(f"{eos_admin} rm -rf '{test_directory}'", throw_on_failure=False)
    eos_client.exec(f"{eos_admin} mkdir '{test_directory}'")
    try:
        eos_client.exec(f"{eos_admin} attr set sys.archive.storage_class=fail_on_closew_test '{test_directory}'")

        # Writing the file must fail and trigger EOS's delete-on-close handling
        print("The following archive is expected to fail and trigger delete-on-close handling")
        copy_result = eos_client.exec(
            f"KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp /etc/group root://{disk_instance_name}/{test_file}",
            capture_output=True,
            throw_on_failure=False,
        )
        assert not copy_result.success, "xrdcp succeeded despite the CLOSEW workflow error"

        # Exit status 2 specifically means that EOS removed the namespace entry; other failures must not pass the test
        file_info_status = eos_client.exec_with_output(
            f"eos root://{disk_instance_name} fileinfo '{test_file}' >/dev/null 2>&1; printf '%s' $?"
        )
        assert file_info_status == "2", (
            f"Expected EOS fileinfo to report a missing namespace entry (exit 2), got {file_info_status}"
        )
    finally:
        # Keep reruns independent even when an assertion above fails
        eos_client.exec(f"{eos_admin} rm -rf '{test_directory}'", throw_on_failure=False)


def test_archive_zero_length_file(eos_client: EosClientHost, disk_instance_name: str, test_dir: Path) -> None:
    # Create an empty source and attempt to archive it
    source_path = Path(f"/tmp/empty_file_{uuid.uuid4().hex}")
    destination_path = test_dir / source_path.name
    eos_client.exec(f"truncate -s 0 '{source_path}'")
    print("The following archive is expected to fail because zero-length files are forbidden")
    result = eos_client.exec(
        f"KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp '{source_path}' root://{disk_instance_name}/{destination_path}",
        capture_output=True,
        throw_on_failure=False,
    )
    # Verify both that the copy failed and that it reached the expected zero-length validation
    assert not result.success, "Archiving a zero-length file unexpectedly succeeded"
    error_output = result.stdout + result.stderr
    assert "0-length" in error_output.lower(), f"Unexpected xrdcp error: {error_output}"


class TestEosEvict:
    def test_eos_evict_counter_and_missing_tape_copy(
        self, eos_client: EosClientHost, cta_cli: CtaCliHost, disk_instance_name: str, test_dir: Path
    ) -> None:
        # Put the destination tape drives down so no tape copy can be written
        file_path = test_dir / f"eos_evict_{uuid.uuid4().hex}"
        cta_cli.set_all_drives_down()

        # Write a file for archival and discover the FSID of its disk replica
        eos_client.archive_file(disk_instance_name, file_path, Path("/etc/group"), wait=False)
        file_info = json.loads(
            eos_client.exec_with_output(
                "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 "
                f"eos --json root://{disk_instance_name} info '{file_path}'"
            )
        )
        disk_fsid = next(
            location["fsid"] for location in file_info["locations"] if location["schedgroup"].startswith("default")
        )

        # A normal eviction must fail while no tape replica exists
        print("The following eviction is expected to fail because the file has no tape replica")
        result = eos_client.exec(
            "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"eos root://{disk_instance_name} evict '{file_path}'",
            throw_on_failure=False,
        )
        assert not result.success

        # Selecting the disk FSID and bypassing the counter must still fail without a tape replica
        print("The following forced eviction is expected to fail because the file has no tape replica")
        result = eos_client.exec(
            "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"eos root://{disk_instance_name} evict --ignore-evict-counter --fsid {disk_fsid} '{file_path}'",
            throw_on_failure=False,
        )
        assert not result.success

        # Failed eviction attempts must leave the disk replica intact
        assert eos_client.is_file_on_disk(disk_instance_name, file_path)

        # Allow archival to finish, then verify the disk copy is removed and the tape copy exists
        cta_cli.set_all_drives_up(wait=False)
        eos_client.wait_for_file_archival(disk_instance_name, file_path)
        eos_client.wait_for_file_eviction(disk_instance_name, file_path)

        # Three retrieve requests should produce one disk replica with an eviction counter of three
        for _ in range(3):
            result = eos_client.prepare_files(disk_instance_name, [file_path])
            assert result.success
        eos_client.wait_for_file_retrieval(disk_instance_name, file_path)

        # Each normal evict decrements the counter and preserves the disk replica until it reaches zero
        for expected_counter in range(3, 0, -1):
            attributes = json.loads(
                eos_client.exec_with_output(
                    "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 "
                    f"eos --json root://{disk_instance_name} attr get sys.retrieve.evict_counter '{file_path}'"
                )
            )
            assert int(attributes["attr"]["get"][0]["sys"]["retrieve"]["evict_counter"]) == expected_counter
            eos_client.exec(
                "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 "
                f"eos root://{disk_instance_name} evict '{file_path}'"
            )

        # The disk replica is removed after the final counter reaches 0
        eos_client.wait_for_file_eviction(disk_instance_name, file_path)

    def test_eos_evict_explicit_fsid(
        self,
        cta_cli: CtaCliHost,
        eos_client: EosClientHost,
        disk_instance_name: str,
        test_dir: Path,
    ) -> None:
        cta_cli.set_all_drives_up()
        dummy_file_systems = [(101, "dummy_1"), (102, "dummy_2"), (103, "dummy_3")]
        tape_fsid = 65535
        missing_fsid = 200
        file_path = eos_client.generate_and_archive_file(
            disk_instance_name, test_dir / "eos_evict_fsid", append_uid=True
        )
        eos_admin = f"KRB5CCNAME=/tmp/eosadmin1/krb5cc_0 XrdSecPROTOCOL=krb5 eos -r 0 0 root://{disk_instance_name}"
        eos_power = "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5 eos"

        try:
            # Add three dummy filesystems and advertise replicas on them in the namespace
            for fsid, name in dummy_file_systems:
                eos_client.exec(f"{eos_admin} space define '{name}'")
                eos_client.exec(f"{eos_admin} fs add -m {fsid} '{name}' localhost:1234 /does_not_exist_{fsid} '{name}'")
                eos_client.exec(f"{eos_admin} file tag '{file_path}' +{fsid}")

            def replica_count() -> int:
                info = json.loads(
                    eos_client.exec_with_output(f"{eos_power} --json root://{disk_instance_name} info '{file_path}'")
                )
                return len(info["locations"])

            # EOS should now advertise the tape replica and three dummy disk replicas
            assert replica_count() == 4

            # Eviction must reject the tape FSID, a nonexistent FSID, and incomplete option combinations
            failing_cases = [
                ("tape replica", f"--ignore-evict-counter --fsid {tape_fsid}"),
                ("nonexistent replica", f"--ignore-evict-counter --fsid {missing_fsid}"),
                ("counter not bypassed", "--fsid 101"),
                ("missing FSID", "--ignore-removal-on-fst"),
                ("counter not bypassed with removal ignored", "--ignore-removal-on-fst --fsid 101"),
            ]
            for case, options in failing_cases:
                print(f"The following eviction is expected to fail: {case}")
                result = eos_client.exec(
                    f"{eos_power} root://{disk_instance_name} evict {options} '{file_path}'", throw_on_failure=False
                )
                assert not result.success, f"Eviction unexpectedly succeeded for {case}"
            assert replica_count() == 4

            # Remove one selected replica while preserving all others
            eos_client.exec(
                f"{eos_power} root://{disk_instance_name} evict --ignore-evict-counter --fsid 101 '{file_path}'"
            )
            assert replica_count() == 3

            # Removing namespace metadata can succeed even if removal on the dummy FST is skipped
            eos_client.exec(
                f"{eos_power} root://{disk_instance_name} evict "
                f"--ignore-removal-on-fst --ignore-evict-counter --fsid 102 '{file_path}'"
            )
            assert replica_count() == 2

            # Remove all remaining disk replicas while preserving the tape replica.
            eos_client.exec(f"{eos_power} root://{disk_instance_name} evict --ignore-evict-counter '{file_path}'")
            assert replica_count() == 1
            assert eos_client.is_file_on_tape_only(disk_instance_name, file_path)
        finally:
            # Clean up
            for fsid, _ in dummy_file_systems:
                eos_client.exec(f"{eos_admin} fs config {fsid} configstatus=empty", throw_on_failure=False)
                eos_client.exec(f"{eos_admin} fs rm {fsid}", throw_on_failure=False)


def test_eos_immutable_file(eos_client: EosClientHost, eos_mgm: EosMgmHost, test_dir: Path) -> None:
    eos_client.exec(
        f". /tmp/client_env && echo yes | cta-immutable-file-test root://{eos_mgm.instance_name}/{test_dir}/immutable_file"
    )


def test_eos_timestamps_correctness(eos_client: EosClientHost, disk_instance_name: str, test_dir: Path) -> None:
    def persistent_timestamps(file_info: str) -> tuple[str, str]:
        timestamps = {}
        for name in ("Modify", "Birth"):
            match = re.search(rf"^\s*{name}:.*?Timestamp:\s*(.+)$", file_info, re.MULTILINE)
            assert match is not None, f"Could not find {name} timestamp in EOS file info:\n{file_info}"
            timestamps[name] = match.group(1).strip()
        return timestamps["Modify"], timestamps["Birth"]

    # Modify and birth timestamps must remain stable across tape operations. The change timestamp is allowed to change
    file_path = test_dir / f"test_eos_timestamps_{uuid.uuid4().hex}"
    eos_client.archive_file(disk_instance_name, file_path, Path("/etc/group"), wait=False)
    timestamps_before_archive = persistent_timestamps(eos_client.file_info(disk_instance_name, file_path))

    # Compare the persistent timestamps after archive, retrieve, and eviction transitions
    eos_client.wait_for_file_archival(disk_instance_name, file_path)
    assert persistent_timestamps(eos_client.file_info(disk_instance_name, file_path)) == timestamps_before_archive

    eos_client.retrieve_file(disk_instance_name, file_path)
    assert persistent_timestamps(eos_client.file_info(disk_instance_name, file_path)) == timestamps_before_archive

    eos_client.evict_file(disk_instance_name, file_path)
    assert persistent_timestamps(eos_client.file_info(disk_instance_name, file_path)) == timestamps_before_archive


# Tests for eosdf


class TestEosdf:
    """Verify EOS free-space probing and its deliberately non-fatal error handling during retrieval.

    Retrieval must continue when cta-eosdf.sh is absent, non-executable, or cannot contact its EOS instance. Those
    failures are reported in the taped log instead of failing the tape session.
    """

    def test_eosdf(
        self,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        test_dir: Path,
        cta_taped: CtaTapedHost,
    ) -> None:
        # Restrict work to a single known drive to ensure we can reliably inspect its logs
        cta_cli.set_all_drives_down()
        cta_cli.set_drive_up(cta_taped.drive_name)
        _run_eosdf_test(eos_client, cta_cli, disk_instance_name, test_dir)

    ## The idea is that we run it once without script, and once without executable permission on the script
    ## Both times we should get a success, because when the script is the problem, we allow staging to continue

    def test_eosdf_with_nonexistent_script(
        self,
        cta_taped: CtaTapedHost,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        test_dir: Path,
    ) -> None:
        # Temporarily hide the probe script, then verify retrieval continues and the failure is logged
        cta_taped.exec("sudo mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh")
        try:
            _run_eosdf_test(eos_client, cta_cli, disk_instance_name, test_dir)
            cta_taped.exec(f"grep -q 'No such file or directory' {cta_taped.log_file_path}")
        finally:
            cta_taped.exec("sudo mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh")

    def test_eosdf_without_executable_permissions(
        self,
        cta_taped: CtaTapedHost,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        test_dir: Path,
    ) -> None:
        # Make the probe non-executable, then verify retrieval still succeeds with a diagnostic
        cta_taped.exec("chmod -x /usr/bin/cta-eosdf.sh")
        try:
            _run_eosdf_test(eos_client, cta_cli, disk_instance_name, test_dir)
            cta_taped.exec(f"grep -q 'Permission denied' {cta_taped.log_file_path}")
        finally:
            cta_taped.exec("chmod +x /usr/bin/cta-eosdf.sh")

    # Test what happens when the EOS client returns an error.
    # The script uses a nonexistent instance name to make it unreachable.
    # grep for 'could not be used to get the FreeSpace'

    def test_eosdf_with_script_that_throws_exception(
        self,
        cta_taped: CtaTapedHost,
        eos_client: EosClientHost,
        cta_cli: CtaCliHost,
        disk_instance_name: str,
        test_dir: Path,
    ) -> None:
        # Point the probe at an unreachable EOS instance to exercise its runtime-error fallback
        cta_taped.exec("sudo sed -i 's|root://$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh")
        try:
            _run_eosdf_test(eos_client, cta_cli, disk_instance_name, test_dir)
            cta_taped.exec(f"grep -q 'could not be used to get the FreeSpace' {cta_taped.log_file_path}")
        finally:
            cta_taped.exec("sudo sed -i 's|root://nonexistentinstance|root://$diskInstance|g' /usr/bin/cta-eosdf.sh")
        # Done with the eosdf tests; set all drives up again
        cta_cli.set_all_drives_up()


# This test screws with the tape pools and archive routes, so it should be the last one in the suite that tests anything
# to do with the tape workflow
# We can't use the Temp* resources directly, because they clean themselves up. However, after archiving we have some
# files archived, which prevent the cleanup


def test_retrieve_queue_cleanup(
    eos_mgm: EosMgmHost,
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    test_dir: Path,
    cta_storage_class: str,
    remote_scripts_dir: Path,
) -> None:
    eos_client.copy_to(
        remote_scripts_dir / "eos_client" / "test_retrieve_queue_cleanup.sh", Path("/tmp"), permissions="+x"
    )
    nb_copies = 3
    non_full_tapes = cta_cli.list_writable_tapes()
    assert len(non_full_tapes) >= 3
    vo_name = "vo"  # get this from somewhere?

    # Build storage classes with one, two, and three tape copies and matching archive routes.
    tp_names = [f"tp_{i + 1}_copy" for i in range(nb_copies)]
    for i, tp_name in enumerate(tp_names):
        copynb = i + 1
        copy_dir = test_dir / f"dir_{copynb}_copy"
        sc_name = f"{cta_storage_class}_{copynb}_copy"
        eos_mgm.exec(f"eos mkdir -p {copy_dir}")
        eos_mgm.exec(f"eos attr set sys.archive.storage_class={sc_name} {copy_dir}")
        print(f"Creating TP {tp_name}")
        cta_cli.exec(f"cta-admin tp add -n '{tp_name}' --vo {vo_name} -p 0 -m 'Add temp tape pool'")
        cta_cli.exec(f"cta-admin tape ch --vid {non_full_tapes[i]['vid']} --tapepool {tp_name}")
        print(f"Creating SC {sc_name}, {sc_name}, {copynb}")
        cta_cli.exec(f"cta-admin sc add -n {sc_name} -c {copynb} --vo {vo_name} -m 'Add temp storage class'")

        for j in range(copynb):
            print(f"Creating AR {sc_name}, {tp_names[j]}, {j + 1}")
            cta_cli.exec(
                f"cta-admin archiveroute add --storageclass '{sc_name}' --tapepool {tp_names[j]} --copynb "
                f"{j + 1} -m 'Add temp archive route'"
            )

    # Exercise cleanup of the retrieve queues
    eos_client.exec(f". /tmp/client_env && /tmp/test_retrieve_queue_cleanup.sh {test_dir}")


# Tests for correct runtime behaviour w.r.t. logs, config files, etc


class TestRuntimeDeployment:
    def test_taped_config_dr_ls_consistency(self, cta_cli: CtaCliHost, cta_taped: CtaTapedHost) -> None:
        # Load the on-disk taped configuration and its catalogue representation
        taped_config = cta_taped.exec_with_output("cat /etc/cta/cta-taped.conf")
        drive_json = cta_cli.exec_with_output("cta-admin --json dr ls")
        entries = [e for e in json.loads(drive_json) if e.get("driveName") == cta_taped.drive_name]
        assert entries, "Drive not found"
        config_json = entries[0].get("driveConfig")
        assert config_json, "driveconfig missing"
        indexed = {(e["category"], e["key"], e["value"]) for e in config_json}

        # Because our config files are badly structured, some options end up differently in the catalogue
        # For now just skip them
        key_skip_list = ["MountCriteria"]

        # Every relevant configuration entry must be reflected in the registered drive
        for line in taped_config.splitlines():
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith("#"):
                continue
            parts = stripped_line.split(None, 3)
            if len(parts) < 3:
                continue
            cat, key, val = parts[0], parts[1], parts[2]
            if key in key_skip_list:
                continue
            assert (cat, key, val) in indexed

    # The following are standard for all services using the CTA runtime library
    # For now only cta_maintd is supported, but eventually the frontend and taped should be added here

    @pytest.mark.parametrize(
        ("daemon_fixture", "expected_config_files"),
        [
            pytest.param("cta_maintd", ("catalogue", "telemetry"), id="maintd"),
            pytest.param("cta_rmcd", (), id="rmcd"),
        ],
    )
    def test_runtime_directory_correctness(
        self,
        request: SubRequest,
        daemon_fixture: str,
        expected_config_files: tuple[str, ...],
        postgres_scheduler_enabled: bool,
    ) -> None:
        # Compare deployed inputs with their runtime copies and verify the generated service metadata
        daemon = request.getfixturevalue(daemon_fixture)

        # Files present for every service
        daemon.exec(f"cmp /etc/cta/cta-{daemon.process_name}.toml /run/cta/config.toml")
        daemon.exec(f"jq -e -r '.service == \"cta-{daemon.process_name}\"' /run/cta/version.json >/dev/null")
        daemon.exec("cmp /etc/cta/cta-logging.schema.json /run/cta/cta-logging.schema.json")

        # Determine which files we should actually check, because not every service exposes all of them
        config_files = {
            "catalogue": ("/etc/cta/cta-catalogue.conf", "/run/cta/catalogue.config_file"),
            "scheduler": ("/etc/cta/cta-scheduler.conf", "/run/cta/scheduler.config_file"),
            "telemetry": ("/etc/cta/cta-otel.yaml", "/run/cta/telemetry.config_file"),
        }
        files_to_check = list(expected_config_files)
        if daemon_fixture == "cta_maintd" and postgres_scheduler_enabled:
            files_to_check.append("scheduler")

        # Now compare
        for config_name in files_to_check:
            deployed_path, runtime_path = config_files[config_name]
            daemon.exec(f"cmp {deployed_path} {runtime_path}")

    @pytest.mark.parametrize(
        "daemon_fixture",
        [
            pytest.param("cta_maintd", id="maintd"),
            pytest.param("cta_rmcd", id="rmcd"),
        ],
    )
    def test_reopens_logfile_on_sighup(self, request: SubRequest, daemon_fixture: str):
        # Record the daemon's current log descriptor before simulating log rotation
        daemon = request.getfixturevalue(daemon_fixture)

        log_file = daemon.log_file_path
        pid = daemon.exec_with_output(f"pgrep -u cta {daemon.process_name}")

        fd = daemon.exec_with_output(f"find /proc/{pid}/fd -maxdepth 1 -lname '{log_file}' -printf '%f\n'")
        assert fd

        rotated = f"{log_file}.pytest"

        # Move the log file; the CTA daemon should keep writing to this moved location
        # as the file descriptor has not been refreshed yet
        daemon.exec(f"sudo mv {log_file} {rotated}")
        # Create a new log file in the original location
        daemon.exec(f"sudo install -o cta -g tape -m 0644 /dev/null {log_file}")
        new_inode = daemon.exec_with_output(f"stat -Lc '%d:%i' {log_file}")

        # Send signal to refresh file descriptor
        daemon.exec(f"pkill -SIGHUP -u cta {daemon.process_name}")
        # Wait until it starts writing to the new file
        current_inode = None

        max_iter = 50
        sleep_time_sec = 0.1
        for _ in range(max_iter):
            current_inode = daemon.exec_with_output(f"stat -Lc '%d:%i' /proc/{pid}/fd/{fd}")
            if current_inode == new_inode:
                break
            time.sleep(sleep_time_sec)

        # The descriptor must eventually refer to the replacement file's inode
        assert current_inode == new_inode

    # Should be deleted once taped uses the new runtime library
    def test_log_rotation_taped(self, cta_taped: CtaTapedHost, remote_scripts_dir: Path) -> None:
        cta_taped.copy_to(remote_scripts_dir / "cta_taped" / "test_refresh_log_fd.sh", Path("/tmp"), permissions="+x")
        cta_taped.exec("sudo bash /tmp/test_refresh_log_fd.sh")

    def test_log_schema_correctness(self, env: TestEnv, tmp_path: Path, cta_maintd: CtaMaintdHost) -> None:
        # Collect the schema and logs from every CTA service that participates in this deployment
        hosts = [*env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped]
        logging_schema_path = tmp_path / "cta-logging.schema.json"
        logging_paths = [(host, tmp_path / f"{host.name}.log") for host in hosts]

        # Copying from Kubernetes has significant fixed overhead, so fetch all inputs concurrently
        with ThreadPoolExecutor(max_workers=len(logging_paths) + 1) as pool:
            copies = [
                pool.submit(
                    cta_maintd.copy_from,
                    Path("/run/cta/cta-logging.schema.json"),
                    logging_schema_path,
                )
            ]
            copies.extend(pool.submit(host.copy_from, host.log_file_path, path) for host, path in logging_paths)
            for copy in copies:
                copy.result()

        fail_fast = True

        def load_schema(path: Path) -> dict[str, Any]:
            with path.open(encoding="utf-8") as f:
                return json.load(f)

        def iter_lines(path: Union[Path, str]) -> Iterator[str]:
            if path == "-":
                for line in sys.stdin:
                    yield line
            else:
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        yield line

        def extract_expected_events(schema: dict[str, Any]) -> set[str]:
            expected_events = set()
            try:
                enum_events = schema["properties"]["event_name"]["enum"]
                expected_events.update(enum_events)
            except KeyError:
                pass

            # Not all events may occur in the output logs of test
            ignore_events = ["program_exiting"]
            return expected_events - set(ignore_events)

        print("Verifying log schema")

        schema = load_schema(logging_schema_path)
        validate = cast(Callable[[dict[str, Any]], Any], fastjsonschema.compile(schema))

        expected_events = extract_expected_events(schema)
        observed_events = set()

        errors = 0
        i = 0

        # Validate each JSON log record while collecting the event names observed across all hosts
        for host, current_logging_path in logging_paths:
            print(f"Checking logs for {host.name}")
            for i, line in enumerate(iter_lines(current_logging_path), start=1):
                stripped_line = line.strip()
                if not stripped_line:
                    continue

                try:
                    obj = json.loads(stripped_line)
                except json.JSONDecodeError:
                    print(f"ERROR: Invalid JSON found on line {i}")
                    print(f"  * Contents: {stripped_line}")
                    errors += 1
                    if fail_fast:
                        sys.exit(1)
                    continue

                if "event_name" in obj:
                    observed_events.add(obj["event_name"])

                try:
                    validate(obj)
                except fastjsonschema.JsonSchemaException as violation:
                    errors += 1
                    print(f"ERROR: Schema violation found on line {i}")
                    print(f"  * Contents: {line}")
                    print(f"  * Violation: {violation}")
                    if fail_fast:
                        sys.exit(1)

        # Require coverage of every schema event that is expected during the system suite
        missing_events = expected_events - observed_events
        if missing_events:
            print("\nERROR: Test coverage incomplete!")
            print("The schema expects coverage for these events, but they were missing from the input logs:")
            for missing in sorted(missing_events):
                print(f"  - {missing}")
            errors += 1

        if errors:
            print(f"Total errors found: {errors}")
            sys.exit(1)

        if i == 0:
            print("ERROR: No JSON objects found")
            sys.exit(1)

        print(f"SUCCESS: Verification passed. {i} lines checked.")
        print(f"Coverage complete: All {len(expected_events)} defined event types were tested.")


def test_add_errors_to_whitelist(error_whitelist: set[str]) -> None:
    # Register errors deliberately produced by the destructive and failure-path scenarios above
    error_whitelist.add("Task failed: counting another error for this session")
    error_whitelist.add("In RecallReportPacker::ReportError::execute(): failing retrieve job after exception.")
    error_whitelist.add("File writing to disk failed")
    error_whitelist.add(
        "Received an exception when trying to get archive file by id. Ignoring request to delete archive file."
    )
    error_whitelist.add("In Scheduler::reportRetrieveJobsBatch(): failed to report.")
    error_whitelist.add("In Scheduler::reportArchiveJobsBatch(): failed to report.")
    error_whitelist.add(
        "In RetrieveRequest::garbageCollect() [queue cleanup]: No VID available to requeue the "
        "request. Failing all jobs."
    )
    error_whitelist.add("End of recall session with error(s)")
    error_whitelist.add(
        "In RetrieveMount::releaseDiskSpace(): reservation release request failed, driveName, "
        "diskSystem and mountId do not match."
    )
    error_whitelist.add("In OStoreDB::RepackArchiveReportBatch::report(): async job update failed.")
    error_whitelist.add("In Agent::deleteAndUnregisterSelf: agent still owns objects. Here is a part of the list.")
