# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import pytest

import json
import uuid
import base64
from datetime import datetime, timedelta
import time
from dataclasses import dataclass
from ..helpers.utils import find_line
import sys

from jsonschema import Draft202012Validator

#####################################################################################################################
# Helpers
#####################################################################################################################


@dataclass
class ClientParams:
    file_count: int
    file_size_kb: int
    directory_count: int
    process_count: int


@pytest.fixture(scope="module")
def client_params(request) -> ClientParams:
    client_config = request.config.test_config["tests"]["client"]
    return ClientParams(
        file_count=client_config["file_count"],
        file_size_kb=client_config["file_size_kb"],
        directory_count=client_config["directory_count"],
        process_count=client_config["process_count"],
    )


#####################################################################################################################
# Tests
#####################################################################################################################

# For now only the "glue" has been migrated to Python. Most of the scripts invoked in the tests below still need to be migrated at a later point in time
# Some scripts probably deserve to be their own test module instead of cramming everything in this file


def test_setup_client(eos_client, client_params, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_setup.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_helper.sh"), "/tmp/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "cli_calls.sh"), "/tmp/", permissions="+x")
    eos_client.exec(
        f"/tmp/client_setup.sh -n {client_params.file_count} -s {client_params.file_size_kb} -p {client_params.process_count} -d {test_dir} -r -c xrd"
    )


###
# TPC capabilities
#
# if the installed xrootd version is not the same as the one used to compile eos
# Third Party Copy can be broken
#
# Valid check:
# [root@eosctafst0017 ~]# xrdfs
# root://pps-ngtztag6p5ht-minion-2.cern.ch:1101 query config tpc
# 1
#
# invalid check:
# [root@ctaeos /]# xrdfs root://ctaeos.toto.svc.cluster.local:1095 query config tpc
# tpc


def test_eos_xrootd_third_party_copy_capabilities(eos_mgm, disk_instance_name):
    """Verifies that all online EOS FST nodes have xrootd TPC capabilities enabled."""

    # Fetch nodes
    node_ls_raw = eos_mgm.exec_with_output("eos -j root://localhost node ls")
    node_envelope = json.loads(node_ls_raw)
    node_data = node_envelope.get("result", [])

    # Filter for online nodes and extract their hostport addresses
    online_hostports = []
    for node in node_data:
        if node.get("status") == "online" and "hostport" in node:
            online_hostports.append(node["hostport"])

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


def test_eos_xrootd_api_fts_compliance(eos_mgm):
    """Verifies that xrdfs query prepare preserves the exact requested sequence order and duplicates.
    Write 3 files and xrdfs query them in reverse order with duplicates.
    `xrdfs query prepare 3 2 1 3` must answer 3 2 1 3
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

    try:
        response_data = json.loads(query_output)
        # Safely extract paths from the JSON response items mapping
        output_paths = [item["path"] for item in response_data.get("responses", [])]
    except (json.JSONDecodeError, KeyError) as e:
        pytest.fail(f"Failed to parse compliant JSON out of xrdfs output. Error: {e}. Output: {query_output}")

    # Both the order and elements must match exactly
    assert (
        input_paths == output_paths
    ), "FTS Compliance Failed! The xrdfs query prepare did not maintain the original request sequence."

    print("xrootd_API capabilities: SUCCESS")

    eos_mgm.force_remove_directory(tmp_dir)


def test_simple_archive_retrieve(eos_client, test_dir, disk_instance_name):
    file_path = test_dir / "test_simple_archive_retrieve"
    file_path = eos_client.generate_and_archive_file(
        disk_instance_name, destination_path=file_path, wait=True, append_uid=True
    )
    print("Information about the archived testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    eos_client.retrieve_file(disk_instance_name, file_path, wait=True)
    print("Information about the retrieved testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    # The original test attempts to evict as poweruser1, but this does not work
    eos_client.evict_file(disk_instance_name, file_path, user="eosadmin1", wait=True)
    print("Information about the evicted testing file:")
    print(eos_client.file_info(disk_instance_name, file_path))

    eos_client.delete_file(disk_instance_name, file_path)


def test_archive(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_archive.sh"), "/tmp/", permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_archive.sh")
    # TODO: replace by something more deterministic. Is this even necessary?
    print("Sleeping 5 seconds to allow MGM-FST communication to settle after disk copy deletion.")
    time.sleep(5)


def test_retrieve(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_retrieve.sh"), "/tmp/", permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_retrieve.sh")


def test_evict(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_evict.sh"), "/tmp/", permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_evict.sh")


def test_abort_prepare(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_abort_prepare.sh"), "/tmp/", permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_abort_prepare.sh")


def test_multiple_retrieve(eos_client, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_multiple_retrieve.sh"), "/tmp/", permissions="+x")
    eos_client.exec(f". /tmp/client_env && /tmp/test_multiple_retrieve.sh {test_dir}")


def test_idempotent_prepare(eos_client, eos_mgm, cta_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_idempotent_prepare.sh"), "/tmp/", permissions="+x")

    no_prepare_dir = cta_dir / "no_prepare"
    # no_prepare_dir must be writable by eosusers and powerusers
    # but not allow prepare requests
    # this is achieved through the ACLs
    eos_mgm.exec(f"eos mkdir -p {no_prepare_dir}")
    eos_mgm.exec(
        f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d {no_prepare_dir}"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_idempotent_prepare.sh")
    eos_mgm.force_remove_directory(no_prepare_dir)


def test_delete_on_closew_error(eos_client, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_delete_on_closew_error.sh"), "/tmp/", permissions="+x"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_delete_on_closew_error.sh")


def test_archive_zero_length_file(eos_client, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_archive_zero_length_file.sh"), "/tmp/", permissions="+x"
    )
    eos_client.exec(". /tmp/client_env && /tmp/test_archive_zero_length_file.sh")


def test_eos_evict(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eos_evict.sh"), "/tmp/", permissions="+x")
    eos_client.exec(". /tmp/client_env && /tmp/test_eos_evict.sh")


# tmp_path is a fixture provided by pytest itself. See: https://docs.pytest.org/en/6.2.x/tmpdir.html


def test_eos_http_rest_api(eos_client, eos_mgm, tmp_path, remote_scripts_dir, test_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_rest_api.sh"), "/tmp/", permissions="+x")
    # Copy over CA certificates
    eos_mgm.copy_from("etc/grid-security/certificates/", tmp_path)
    eos_client.copy_to(tmp_path, "/etc/grid-security")
    eos_client.exec(f". /tmp/client_env && /tmp/test_rest_api.sh {test_dir}")


def test_eos_immutable_file(eos_client, eos_mgm, test_dir):
    eos_client.exec(
        f". /tmp/client_env && echo yes | cta-immutable-file-test root://{eos_mgm.instance_name}/{test_dir}/immutable_file"
    )


def test_eos_timestamps_correctness(eos_client, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eos_timestamps.sh"), "/tmp/", permissions="+x")
    eos_client.exec(f". /tmp/client_env && /tmp/test_eos_timestamps.sh {test_dir}")


# Note that this test simply tests whether the base64 encoded string ends up in the eos report logs verbatim


def test_eos_archive_metadata_ends_up_in_eos_report(eos_client, eos_mgm, test_dir):
    disk_instance_name = eos_mgm.instance_name
    file_loc = test_dir / "archive_metadata_file"

    archive_metadata = {"scheduling_hints": f"test_{str(uuid.uuid4())[:8]}"}
    archive_metadata_b64 = base64.b64encode(json.dumps(archive_metadata, separators=(",", ":")).encode()).decode()

    now = datetime.now()
    later_timestamp = int((now + timedelta(days=1)).timestamp())

    EOSADMIN_USER = "eosadmin1"
    token_eosuser1 = eos_client.exec_with_output(
        f"XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/{EOSADMIN_USER}/krb5cc_0 eos -r 0 0 root://{disk_instance_name} token --tree --path '/eos/ctaeos/://:/api/' --expires {later_timestamp} --owner user1 --group eosusers --permission rwx"
    )

    print("Printing eosuser token dump")
    eos_client.exec(f'eos root://"{disk_instance_name}" token --token "{token_eosuser1}" | jq .')

    print("Archiving file with archive metadata")
    eos_client.exec(
        f'curl -X PUT -L --insecure -H "Accept: application/json" -H "ArchiveMetadata: {archive_metadata_b64}" -H "Authorization: Bearer {token_eosuser1}" "https://{disk_instance_name}:8443/{file_loc}" --upload-file "/etc/group"'
    )

    # Check the archive metadata appears in the mgm log file
    report_file = eos_mgm.get_report_file_path()
    print(f"Report file: {report_file}")
    content = eos_mgm.exec_with_output(f"cat {report_file}")

    xrd_line = find_line(content, f"archivemetadata={archive_metadata_b64}")

    assert xrd_line, f'Missing EOS report entry for Archive Metadata string: "{archive_metadata_b64}"'


# Tests for eosdf


def test_eosdf(eos_client, cta_cli, test_dir, remote_scripts_dir, cta_taped):
    # Ensure that whatever drive we are checking is the one doing the archiving
    cta_cli.set_all_drives_down()
    cta_cli.set_drive_up(cta_taped.drive_name)
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eosdf.sh"), "/tmp/", permissions="+x")
    eos_client.exec(f". /tmp/client_env && /tmp/test_eosdf.sh {test_dir}")


## The idea is that we run it once without script, and once without executable permission on the script
## Both times we should get a success, because when the script is the problem, we allow staging to continue


def test_eosdf_with_nonexistent_script(cta_taped, eos_client, test_dir):
    cta_taped.exec("mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh")
    try:
        eos_client.exec(f". /tmp/client_env && /tmp/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'No such file or directory' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh")


def test_eosdf_without_executable_permissions(cta_taped, eos_client, test_dir):
    cta_taped.exec("chmod -x /usr/bin/cta-eosdf.sh")
    try:
        eos_client.exec(f". /tmp/client_env && /tmp/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'Permission denied' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("chmod +x /usr/bin/cta-eosdf.sh")


# Test what happens when we get an error from the eos client (fake instance not reachable by specifying a nonexistent instance name in the script)
# grep for 'could not be used to get the FreeSpace'


def test_eosdf_with_script_that_throws_exception(cta_taped, eos_client, cta_cli, test_dir):
    cta_taped.exec("sed -i 's|root://$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh")
    try:
        eos_client.exec(f". /tmp/client_env && /tmp/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'could not be used to get the FreeSpace' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("sed -i 's|root://nonexistentinstance|root://$diskInstance|g' /usr/bin/cta-eosdf.sh")
    # Done with the eosdf tests; set all drives up again
    cta_cli.set_all_drives_up()


# This test screws with the tape pools and archive routes, so it should be the last one in the suite that tests anything to do with the tape workflow
# We can't use the Temp* resources directly, because they clean themselves up. However, after archiving we have some files archived, which prevent the cleanup


def test_retrieve_queue_cleanup(eos_mgm, eos_client, cta_cli, test_dir, cta_storage_class, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_retrieve_queue_cleanup.sh"), "/tmp/", permissions="+x"
    )
    nb_copies = 3

    non_full_tapes = cta_cli.writable_tapes()
    assert len(non_full_tapes) >= 3
    vo_name = "vo"  # get this from somewhere?

    tp_names = [f"tp_{i + 1}_copy" for i in range(nb_copies)]
    for i, tp_name in enumerate(tp_names):
        copynb = i + 1
        dir = test_dir / f"dir_{copynb}_copy"
        sc_name = f"{cta_storage_class}_{copynb}_copy"
        eos_mgm.exec(f"eos mkdir -p {dir}")
        eos_mgm.exec(f"eos attr set sys.archive.storage_class={sc_name} {dir}")
        print(f"Creating TP {tp_name}")
        cta_cli.exec(f"cta-admin tp add -n '{tp_name}' --vo {vo_name} -p 0 -m 'Add temp tape pool'")
        cta_cli.exec(f"cta-admin tape ch --vid {non_full_tapes[i]['vid']} --tapepool {tp_name}")
        print(f"Creating SC {sc_name}, {sc_name}, {copynb}")
        cta_cli.exec(f"cta-admin sc add -n {sc_name} -c {copynb} --vo {vo_name} -m 'Add temp storage class'")

        for j in range(copynb):
            print(f"Creating AR {sc_name}, {tp_names[j]}, {j+1}")
            cta_cli.exec(
                f"cta-admin archiveroute add --storageclass '{sc_name}' --tapepool {tp_names[j]} --copynb {j+1} -m 'Add temp archive route'"
            )

    # The actual test
    eos_client.exec(f". /tmp/client_env && /tmp/test_retrieve_queue_cleanup.sh {test_dir}")


# Tests for correct runtime behaviour w.r.t. logs, config files, etc


def test_taped_config_dr_ls_consistency(cta_cli, cta_taped):

    taped_config = cta_taped.exec_with_output("cat /etc/cta/cta-taped.conf")
    drive_json = cta_cli.exec_with_output("cta-admin --json dr ls")
    entries = [e for e in json.loads(drive_json) if e.get("driveName") == cta_taped.drive_name]
    assert entries, "Drive not found"
    config_json = entries[0].get("driveConfig")
    assert config_json, "driveconfig missing"
    indexed = {(e["category"], e["key"], e["value"]) for e in config_json}

    # Because our config files are badly structured, some options end up differently in the catalogue
    # For now just skip them
    KEY_SKIP_LIST = ["MountCriteria"]

    for line in taped_config.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        cat, key, val = parts[0], parts[1], parts[2]
        if key in KEY_SKIP_LIST:
            continue
        assert (cat, key, val) in indexed


def test_example_config_file_correctness_maintd(cta_maintd):
    cta_maintd.exec("cta-maintd --config-strict --config /etc/cta/cta-maintd.example.toml --config-check")


def test_runtime_directory_correctness_maintd(cta_maintd):
    cta_maintd.exec("comm /etc/cta/cta-maintd.toml /run/cta/config.toml -3")
    cta_maintd.exec("comm /etc/cta/cta-catalogue.conf /run/cta/catalogue.config_file -3")
    cta_maintd.exec("comm /etc/cta/cta-otel.yaml /run/cta/telemetry.config_file -3")
    cta_maintd.exec("jq -e -r '.service == \"cta-maintd\"' /run/cta/version.json >/dev/null")


def test_log_rotation_maintd(cta_maintd, remote_scripts_dir):
    cta_maintd.copy_to(str(remote_scripts_dir / "cta_maintd" / "test_refresh_log_fd.sh"), "/tmp/", permissions="+x")
    cta_maintd.exec("bash /tmp/test_refresh_log_fd.sh")


def test_log_rotation_taped(cta_taped, remote_scripts_dir):
    cta_taped.copy_to(str(remote_scripts_dir / "cta_taped" / "test_refresh_log_fd.sh"), "/tmp/", permissions="+x")
    cta_taped.exec("bash /tmp/test_refresh_log_fd.sh")


def test_log_schema_correctness(env, tmp_path, cta_maintd):
    hosts = env.cta_admin_api + env.cta_workflow_api + env.cta_taped
    logging_schema_path = tmp_path / "cta-logging.schema.json"
    # Maintd already populates the logging schema in the runtime directory
    cta_maintd.copy_from("/run/cta/cta-logging.schema.json", logging_schema_path)

    fail_fast = True

    def load_schema(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def iter_lines(path):
        if path == "-":
            for line in sys.stdin:
                yield line
        else:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    yield line

    def extract_expected_events(schema):
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
    validator = Draft202012Validator(schema)

    expected_events = extract_expected_events(schema)
    observed_events = set()

    errors = 0
    i = 0

    for host in hosts:
        print(f"Checking logs for {host.name}")
        current_logging_path = tmp_path / f"{host.name}.log"
        host.copy_from(host.log_file_path, current_logging_path)
        for i, line in enumerate(iter_lines(current_logging_path), start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"ERROR: Invalid JSON found on line {i}")
                print(f"  * Contents: {line}")
                errors += 1
                if fail_fast:
                    sys.exit(1)
                continue

            if "event_name" in obj:
                observed_events.add(obj["event_name"])

            violations = sorted(validator.iter_errors(obj), key=lambda e: e.path)

            if violations:
                errors += 1
                print(f"ERROR: Schema violation found on line {i}")
                print(f"  * Contents: {line}")
                print("  * Violations:")
                for v in violations:
                    path = ".".join(map(str, v.path))
                    if path:
                        print(f"      {path}: {v.message}")
                    else:
                        print(f"      {v.message}")
                    if fail_fast:
                        sys.exit(1)

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


def test_add_errors_to_whitelist(error_whitelist):
    error_whitelist.add("Task failed: counting another error for this session")
    error_whitelist.add("In RecallReportPacker::ReportError::execute(): failing retrieve job after exception.")
    error_whitelist.add("File writing to disk failed")
    error_whitelist.add(
        "Received an exception when trying to get archive file by id. Ignoring request to delete archive file."
    )
    error_whitelist.add("In Scheduler::reportRetrieveJobsBatch(): failed to report.")
    error_whitelist.add("In Scheduler::reportArchiveJobsBatch(): failed to report.")
    error_whitelist.add(
        "In RetrieveRequest::garbageCollect() [queue cleanup]: No VID available to requeue the request. Failing all jobs."
    )
    error_whitelist.add("End of recall session with error(s)")
    error_whitelist.add(
        "In RetrieveMount::releaseDiskSpace(): reservation release request failed, driveName, diskSystem and mountId do not match."
    )
    error_whitelist.add("In OStoreDB::RepackArchiveReportBatch::report(): async job update failed.")
    error_whitelist.add("In Agent::deleteAndUnregisterSelf: agent still owns objects. Here is a part of the list.")
