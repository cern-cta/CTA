# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import pytest

import json
from dataclasses import dataclass
from pathlib import Path

# TODO: this test should be split up into multiple suits

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


def test_hosts_present_client(env):
    assert len(env.cta_taped) > 0
    assert len(env.cta_frontend) > 0
    assert len(env.cta_cli) > 0
    assert len(env.eos_client) > 0
    assert len(env.eos_mgm) > 0


def test_copy_scripts(eos_client):
    remote_scripts_dir = Path(__file__).parent / "remote_scripts"
    eos_client_script_dir = remote_scripts_dir / "eos_client"
    eos_client.copy_to(str(eos_client_script_dir / "client_setup.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "client_helper.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "cli_calls.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_archive.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_retrieve.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_evict.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_delete.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_abort_prepare.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_multiple_retrieve.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_idempotent_prepare.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_delete_on_closew_error.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_archive_zero_length_file.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_eos_evict.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_retrieve_queue_cleanup.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_rest_api.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_eos_timestamps.sh"), "/root/", permissions="+x")
    eos_client.copy_to(str(eos_client_script_dir / "test_eosdf.sh"), "/root/", permissions="+x")


# For now only the "glue" has been migrated to Python. Most of the scripts invoked in the tests below still need to be migrated at a later point in time
# Some scripts probably deserve to be their own test module instead of cramming everything in this file


def test_kinit_client(eos_client):
    eos_client.exec("mkdir -p /tmp/ctaadmin2")
    eos_client.exec("mkdir -p /tmp/eosadmin1")
    eos_client.exec("mkdir -p /tmp/poweruser1")
    eos_client.exec(". /root/client_helper.sh && admin_kinit")


# workflow tests


@pytest.mark.eos
def test_setup_archive_directory(eos_client, eos_mgm, test_dir):
    eos_client.exec(f"eos root://{eos_mgm.instance_name} mkdir -p {test_dir}")


@pytest.mark.eos
def test_setup_client(eos_client, client_params, test_dir):
    eos_client.exec(
        f"/root/client_setup.sh -n {client_params.file_count} -s {client_params.file_size_kb} -p {client_params.process_count} -d {test_dir} -r -c xrd"
    )


# @pytest.mark.eos
# def test_simple_archive_retrieve(eos_client, test_dir, disk_instance_name):
#     file_path = test_dir / "test_simple_archive_retrieve"
#     file_path = eos_client.generate_and_archive_file(
#         disk_instance_name, destination_path=file_path, wait=True, append_uid=True
#     )
#     print("Information about the archived testing file:")
#     print(eos_client.file_info(disk_instance_name, file_path))

#     eos_client.retrieve_file(disk_instance_name, file_path, wait=True)
#     print("Information about the retrieved testing file:")
#     print(eos_client.file_info(disk_instance_name, file_path))

#     eos_client.evict_file(disk_instance_name, file_path, wait=True)
#     print("Information about the evicted testing file:")
#     print(eos_client.file_info(disk_instance_name, file_path))

#     eos_client.delete_file(disk_instance_name, file_path)


# @pytest.mark.eos
# def test_archive(eos_client):
#     eos_client.exec(". /root/client_env && /root/test_archive.sh")
#     # TODO: replace by something more deterministic. Is this even necessary?
#     print("Sleeping 5 seconds to allow MGM-FST communication to settle after disk copy deletion.")
#     time.sleep(5)


# @pytest.mark.eos
# def test_retrieve(eos_client):
#     eos_client.exec(". /root/client_env && /root/test_retrieve.sh")


# @pytest.mark.eos
# def test_evict(eos_client):
#     eos_client.exec(". /root/client_env && /root/test_evict.sh")


# @pytest.mark.eos
# def client_abort_prepare(eos_client):
#     eos_client.exec(". /root/client_env && /root/client_abort_prepare.sh")


@pytest.mark.eos
def test_multiple_retrieve(eos_client, test_dir):
    eos_client.exec(f". /root/client_env && /root/test_multiple_retrieve.sh {test_dir}")


@pytest.mark.eos
def test_idempotent_prepare(eos_client, eos_mgm, cta_dir):

    no_prepare_dir = cta_dir / "no_prepare"
    # no_prepare_dir must be writable by eosusers and powerusers
    # but not allow prepare requests
    # this is achieved through the ACLs
    eos_mgm.exec(f"eos mkdir {no_prepare_dir}")
    eos_mgm.exec(
        f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d {no_prepare_dir}"
    )
    eos_client.exec(". /root/client_env && /root/test_idempotent_prepare.sh")
    eos_mgm.force_remove_directory(no_prepare_dir)


@pytest.mark.eos
def test_delete_on_closew_error(eos_client):
    eos_client.exec(". /root/client_env && /root/test_delete_on_closew_error.sh")


@pytest.mark.eos
def test_archive_zero_length_file(eos_client):
    eos_client.exec(". /root/client_env && /root/test_archive_zero_length_file.sh")


@pytest.mark.eos
def test_eos_evict(eos_client):
    eos_client.exec(". /root/client_env && /root/test_eos_evict.sh")


@pytest.mark.eos
def test_retrieve_queue_cleanup(eos_client):
    eos_client.exec(". /root/client_env && /root/test_retrieve_queue_cleanup.sh")


# tmp_path is a fixture provided by pytest itself. See: https://docs.pytest.org/en/6.2.x/tmpdir.html
@pytest.mark.eos
def test_eos_http_rest_api(eos_client, eos_mgm, tmp_path):
    # Copy over CA certificates
    eos_mgm.copy_from("etc/grid-security/certificates/", tmp_path)
    eos_client.copy_to(tmp_path, "/etc/grid-security")
    eos_client.exec("bash /root/test_rest_api.sh")


@pytest.mark.eos
def test_eos_immutable_file(eos_client, eos_mgm, test_dir):
    eos_client.exec(
        f". /root/client_env && echo yes | cta-immutable-file-test root://{eos_mgm.instance_name}/{test_dir}/immutable_file"
    )


@pytest.mark.eos
def test_eos_timestamps_correctness(eos_client, test_dir):
    archive_directory = test_dir / "test_eos_timestamps_correctness"
    eos_client.exec(f". /root/client_env && /root/test_eos_timestamps.sh {archive_directory}")


# Tests for eosdf


@pytest.mark.eos
def test_eosdf_create_archive_directory(eos_client, eos_mgm, test_dir):
    archive_directory = test_dir / "eosdf"
    eos_client.exec(f"eos root://{eos_mgm.instance_name} mkdir -p {archive_directory}")


@pytest.mark.eos
def test_eosdf(cta_client, test_dir):
    cta_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")


## The idea is that we run it once without script, and once without executable permission on the script
## Both times we should get a success, because when the script is the problem, we allow staging to continue
@pytest.mark.eos
def test_eosdf_with_nonexistent_script(cta_taped, cta_client, test_dir):
    cta_taped.exec("mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh")
    try:
        cta_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'No such file or directory' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh")


@pytest.mark.eos
def test_eosdf_without_executable_permissions(cta_taped, cta_client, test_dir):
    cta_taped.exec("chmod -x /usr/bin/cta-eosdf.sh")
    try:
        cta_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'Permission denied' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("chmod +x /usr/bin/cta-eosdf.sh")


# Test what happens when we get an error from the eos client (fake instance not reachable by specifying a nonexistent instance name in the script)
# grep for 'could not be used to get the FreeSpace'
@pytest.mark.eos
def test_eosdf_with_script_that_throws_exception(cta_taped, cta_client, test_dir):
    cta_taped.exec("sed -i 's|root://\$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh")
    try:
        cta_client.exec(f". /root/client_env && /root/test_eosdf.sh {test_dir}")
        cta_taped.exec(f"grep -q 'could not be used to get the FreeSpace' {cta_taped.log_file_path}")
    finally:
        cta_taped.exec("sed -i 's|root://nonexistentinstance|root://\$diskInstance|g' /usr/bin/cta-eosdf.sh")


# Tests for correct runtime behaviour w.r.t. logs, config files, etc


def test_taped_config_dr_ls_consistency(cta_cli, cta_taped):

    taped_config = cta_taped.execWithOutput("cat /etc/cta/cta-taped.conf")
    drive_json = cta_cli.execWithOutput("cta-admin --json dr ls")
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


def test_log_rotation_maintd(cta_maintd):
    cta_maintd.exec("bash /root/refresh_log_fd.sh")


def test_log_rotation_taped(cta_taped):
    cta_taped.exec("bash /root/refresh_log_fd.sh")


# Maybe it makes sense to run this after all tests?
def test_install_jsonschema(cta_maintd, cta_frontend, cta_taped):
    cta_maintd.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")
    cta_frontend.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")
    cta_taped.exec("dnf install -y python3-pip && python3 -m pip install jsonschema")


def test_log_schema_correctness_maintd(cta_maintd):
    cta_maintd.exec(
        "python3 /root/verify_log_schema.py --schema /run/cta/cta-logging.schema.json --input /var/log/cta/cta-maintd.log --fail-fast"
    )


def test_log_schema_correctness_frontend(cta_frontend):
    cta_frontend.exec(
        "python3 /root/verify_log_schema.py --schema /run/cta/cta-logging.schema.json --input /var/log/cta/cta-maintd.log --fail-fast"
    )


def test_log_schema_correctness_taped(cta_taped):
    cta_taped.exec(
        "python3 /root/verify_log_schema.py --schema /run/cta/cta-logging.schema.json --input /var/log/cta/cta-maintd.log --fail-fast"
    )
