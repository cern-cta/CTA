# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import pytest

import json
import time
from dataclasses import dataclass

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

# workflow tests


@pytest.mark.eos
def test_setup_archive_directory(eos_client, eos_mgm, test_dir):
    eos_client.exec(f"eos root://{eos_mgm.instance_name} mkdir -p {test_dir}")


@pytest.mark.eos
def test_setup_client(eos_client, client_params, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_setup.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "client_helper.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "cli_calls.sh"), "/root/", permissions="+x")
    eos_client.exec(
        f"/root/client_setup.sh -n {client_params.file_count} -s {client_params.file_size_kb} -p {client_params.process_count} -d {test_dir} -r -c xrd"
    )


@pytest.mark.eos
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


@pytest.mark.eos
def test_archive(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_archive.sh"), "/root/", permissions="+x")
    eos_client.exec(". /root/client_env && /root/test_archive.sh")
    # TODO: replace by something more deterministic. Is this even necessary?
    print("Sleeping 5 seconds to allow MGM-FST communication to settle after disk copy deletion.")
    time.sleep(5)


@pytest.mark.eos
def test_retrieve(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_retrieve.sh"), "/root/", permissions="+x")
    eos_client.exec(". /root/client_env && /root/test_retrieve.sh")


@pytest.mark.eos
def test_evict(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_evict.sh"), "/root/", permissions="+x")
    eos_client.exec(". /root/client_env && /root/test_evict.sh")


@pytest.mark.eos
def test_abort_prepare(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_abort_prepare.sh"), "/root/", permissions="+x")
    eos_client.exec(". /root/client_env && /root/test_abort_prepare.sh")


@pytest.mark.eos
def test_multiple_retrieve(eos_client, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_multiple_retrieve.sh"), "/root/", permissions="+x")
    eos_client.exec(f". /root/client_env && /root/test_multiple_retrieve.sh {test_dir}")


@pytest.mark.eos
def test_idempotent_prepare(eos_client, eos_mgm, cta_dir, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_idempotent_prepare.sh"), "/root/", permissions="+x"
    )

    no_prepare_dir = cta_dir / "no_prepare"
    # no_prepare_dir must be writable by eosusers and powerusers
    # but not allow prepare requests
    # this is achieved through the ACLs
    eos_mgm.exec(f"eos mkdir -p {no_prepare_dir}")
    eos_mgm.exec(
        f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d {no_prepare_dir}"
    )
    eos_client.exec(". /root/client_env && /root/test_idempotent_prepare.sh")
    eos_mgm.force_remove_directory(no_prepare_dir)


@pytest.mark.eos
def test_delete_on_closew_error(eos_client, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_delete_on_closew_error.sh"), "/root/", permissions="+x"
    )
    eos_client.exec(". /root/client_env && /root/test_delete_on_closew_error.sh")


@pytest.mark.eos
def test_archive_zero_length_file(eos_client, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_archive_zero_length_file.sh"), "/root/", permissions="+x"
    )
    eos_client.exec(". /root/client_env && /root/test_archive_zero_length_file.sh")


@pytest.mark.eos
def test_eos_evict(eos_client, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eos_evict.sh"), "/root/", permissions="+x")
    eos_client.exec(". /root/client_env && /root/test_eos_evict.sh")


# tmp_path is a fixture provided by pytest itself. See: https://docs.pytest.org/en/6.2.x/tmpdir.html
@pytest.mark.eos
def test_eos_http_rest_api(eos_client, eos_mgm, tmp_path, remote_scripts_dir, test_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_rest_api.sh"), "/root/", permissions="+x")
    # Copy over CA certificates
    eos_mgm.copy_from("etc/grid-security/certificates/", tmp_path)
    eos_client.copy_to(tmp_path, "/etc/grid-security")
    eos_client.exec(f". /root/client_env && /root/test_rest_api.sh {test_dir}")


@pytest.mark.eos
def test_eos_immutable_file(eos_client, eos_mgm, test_dir):
    eos_client.exec(
        f". /root/client_env && echo yes | cta-immutable-file-test root://{eos_mgm.instance_name}/{test_dir}/immutable_file"
    )


@pytest.mark.eos
def test_eos_timestamps_correctness(eos_client, test_dir, remote_scripts_dir):
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eos_timestamps.sh"), "/root/", permissions="+x")
    eos_client.exec(f". /root/client_env && /root/test_eos_timestamps.sh {test_dir}")


# Tests for eosdf


@pytest.mark.eos
def test_eosdf(eos_client, cta_cli, test_dir, remote_scripts_dir, cta_taped):
    # Ensure that whatever drive we are checking is the one doing the archiving
    cta_cli.set_all_drives_down()
    cta_cli.set_drive_up(cta_taped.drive_name)
    eos_client.copy_to(str(remote_scripts_dir / "eos_client" / "test_eosdf.sh"), "/root/", permissions="+x")
    eos_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")


## The idea is that we run it once without script, and once without executable permission on the script
## Both times we should get a success, because when the script is the problem, we allow staging to continue
@pytest.mark.eos
def test_eosdf_with_nonexistent_script(cta_taped, eos_client, test_dir):
    cta_taped.exec("mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh")
    try:
        eos_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'No such file or directory' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh")


@pytest.mark.eos
def test_eosdf_without_executable_permissions(cta_taped, eos_client, test_dir):
    cta_taped.exec("chmod -x /usr/bin/cta-eosdf.sh")
    try:
        eos_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'Permission denied' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("chmod +x /usr/bin/cta-eosdf.sh")


# Test what happens when we get an error from the eos client (fake instance not reachable by specifying a nonexistent instance name in the script)
# grep for 'could not be used to get the FreeSpace'
@pytest.mark.eos
def test_eosdf_with_script_that_throws_exception(cta_taped, eos_client, cta_cli, test_dir):
    cta_taped.exec("sed -i 's|root://$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh")
    try:
        eos_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'could not be used to get the FreeSpace' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("sed -i 's|root://nonexistentinstance|root://$diskInstance|g' /usr/bin/cta-eosdf.sh")
    # Done with the eosdf tests; set all drives up again
    cta_cli.set_all_drives_up()


# This test screws with the tape pools and archive routes, so it should be the last one in the suite that tests anything to do with the tape workflow
# We can't use the Temp* resources directly, because they clean themselves up. However, after archiving we have some files archived, which prevent the cleanup
@pytest.mark.eos
def test_retrieve_queue_cleanup(eos_mgm, eos_client, cta_cli, test_dir, cta_storage_class, remote_scripts_dir):
    eos_client.copy_to(
        str(remote_scripts_dir / "eos_client" / "test_retrieve_queue_cleanup.sh"), "/root/", permissions="+x"
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
    eos_client.exec(f". /root/client_env && /root/test_retrieve_queue_cleanup.sh {test_dir}")


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
    cta_maintd.copy_to(str(remote_scripts_dir / "cta_maintd" / "test_refresh_log_fd.sh"), "/root/", permissions="+x")
    cta_maintd.exec("bash /root/test_refresh_log_fd.sh")


def test_log_rotation_taped(cta_taped, remote_scripts_dir):
    cta_taped.copy_to(str(remote_scripts_dir / "cta_taped" / "test_refresh_log_fd.sh"), "/root/", permissions="+x")
    cta_taped.exec("bash /root/test_refresh_log_fd.sh")


# Maybe it makes sense to run this after all tests?
def test_install_jsonschema(cta_maintd, cta_frontend, cta_taped):
    cta_maintd.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")
    cta_frontend.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")
    cta_taped.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")


def test_log_schema_correctness_maintd(cta_maintd, remote_scripts_dir):
    cta_maintd.copy_to(str(remote_scripts_dir / "common" / "verify_log_schema.py"), "/root/", permissions="+x")
    cta_maintd.exec(
        "python3 /root/verify_log_schema.py --schema /run/cta/cta-logging.schema.json --input /var/log/cta/cta-maintd.log --fail-fast"
    )


def test_log_schema_correctness_frontend(cta_frontend, remote_scripts_dir):
    cta_frontend.copy_to(str(remote_scripts_dir / "common" / "verify_log_schema.py"), "/root/", permissions="+x")
    cta_frontend.exec(
        "python3 /root/verify_log_schema.py --schema /etc/cta/cta-logging.schema.json --input /var/log/cta/cta-frontend.log --fail-fast"
    )


def test_log_schema_correctness_taped(cta_taped, remote_scripts_dir):
    cta_taped.copy_to(str(remote_scripts_dir / "common" / "verify_log_schema.py"), "/root/", permissions="+x")
    cta_taped.exec(
        "python3 /root/verify_log_schema.py --schema /etc/cta/cta-logging.schema.json --input /var/log/cta/cta-taped.log --fail-fast"
    )


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
