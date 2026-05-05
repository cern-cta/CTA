# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import pytest

import datetime
import json
import time
from dataclasses import dataclass
from pathlib import Path

from ..helpers.hosts import EosClientHost, EosMgmHost

#####################################################################################################################
# Helpers
#####################################################################################################################


@dataclass
class GfalParams:
    file_count: int
    file_size_kb: int
    process_count: int


@pytest.fixture(scope="class")
def gfal_params(request) -> GfalParams:
    gfal_config = request.config.test_config["tests"]["gfal"]
    return GfalParams(
        file_count=gfal_config["file_count"],
        file_size_kb=gfal_config["file_size_kb"],
        process_count=gfal_config["process_count"],
    )


@pytest.fixture
def eos_client(env) -> EosClientHost:
    return env.eos_client[0]


@pytest.fixture
def eos_mgm(env) -> EosMgmHost:
    return env.eos_mgm[0]


def _get_report_file_path() -> Path:
    now = datetime.datetime.now()
    base_path = Path("/var") / "eos" / "report" / f"{now:%Y}" / f"{now:%m}"
    file_name = f"{now:%Y}{now:%m}{now:%d}.eosreport"
    return base_path / file_name


def _find_line(content, *filters) -> str:
    return next((line for line in content.splitlines() if all(f in line for f in filters)), "")


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_client_gfal(env):
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


# For now only the "glue" has been migrated to Python. All the scripts invoked in the tests below still need to be migrated at a later point in time


def test_kinit_client(eos_client):
    eos_client.exec("mkdir -p /tmp/ctaadmin2")
    eos_client.exec("mkdir -p /tmp/eosadmin1")
    eos_client.exec("mkdir -p /tmp/poweruser1")
    eos_client.exec(". /root/client_helper.sh && admin_kinit")


@pytest.mark.eos
def test_setup_archive_directory(eos_client, eos_mgm):
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    print(f"Cleaning up previous archive directory: {archive_directory}")
    eos_mgm.force_remove_directory(archive_directory)
    eos_client.exec(f"eos root://{eos_mgm.instance_name} mkdir {archive_directory}")


@pytest.mark.eos
def test_setup_client_gfal_xrootd(eos_client, eos_mgm, gfal_params):
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    eos_client.exec("dnf install -y python3-gfal2-util gfal2-plugin-xrootd")
    eos_client.exec(
        f"/root/client_setup.sh -n {gfal_params.file_count} -s {gfal_params.file_size_kb} -p {gfal_params.process_count} -d {archive_directory} -r -c gfal2 -Z root"
    )


@pytest.mark.eos
def test_archive_gfal_xrootd(eos_client):
    eos_client.exec(". /root/client_env && /root/test_archive.sh")
    # TODO: replace by something more deterministic. Is this even necessary?
    print("Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion.")
    time.sleep(10)


@pytest.mark.eos
def test_retrieve_gfal_xrootd(eos_client):
    eos_client.exec(". /root/client_env && /root/test_retrieve.sh")


@pytest.mark.eos
def test_evict_gfal_xrootd(eos_client):
    eos_client.exec(". /root/client_env && /root/test_evict.sh")


@pytest.mark.eos
def test_delete_gfal_xrootd(eos_client):
    eos_client.exec(". /root/client_env && /root/test_delete.sh")


@pytest.mark.eos
def test_setup_client_gfal_https(eos_client, eos_mgm, gfal_params):
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    eos_client.exec("dnf install -y gfal2-plugin-http")
    eos_client.exec(
        f"/root/client_setup.sh -n {gfal_params.file_count} -s {gfal_params.file_size_kb} -p {gfal_params.process_count} -d {archive_directory} -r -c gfal2 -Z https"
    )
    # Enable insecure certs for gfal2
    eos_client.exec("sed -i 's/INSECURE=false/INSECURE=true/g' /etc/gfal2.d/http_plugin.conf")


@pytest.mark.eos
def test_archive_gfal_https(eos_client):
    eos_client.exec(". /root/client_env && /root/test_archive.sh")
    # TODO: replace by something more deterministic. Is this even necessary?
    print("Sleeping 10 seconds to allow MGM-FST communication to settle after disk copy deletion.")
    time.sleep(10)


@pytest.mark.eos
def test_retrieve_gfal_https(eos_client):
    eos_client.exec(". /root/client_env && /root/test_retrieve.sh")


@pytest.mark.eos
def test_evict_gfal_https(eos_client):
    eos_client.exec(". /root/client_env && /root/test_evict.sh")


@pytest.mark.eos
def test_delete_gfal_https(eos_client):
    eos_client.exec(". /root/client_env && /root/test_delete.sh")


@pytest.mark.eos
def test_gfal_activity(eos_client, eos_mgm):
    disk_instance_name = eos_mgm.instance_name
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    valid_instance_file = archive_directory / "test_gfal_activity_valid_instance"
    invalid_instance_file = archive_directory / "test_gfal_activity_invalid_instance"
    valid_instance_file = eos_client.generate_and_archive_file(
        disk_instance_name, valid_instance_file, wait=False, append_uid=True
    )
    invalid_instance_file = eos_client.generate_and_archive_file(
        disk_instance_name, invalid_instance_file, wait=False, append_uid=True
    )
    eos_client.wait_for_file_archival(disk_instance_name, valid_instance_file)
    eos_client.wait_for_file_archival(disk_instance_name, invalid_instance_file)

    # Query .well-known tape rest api endpoint to get the sitename
    site_name = eos_client.exec_with_output(
        f"curl --insecure https://{disk_instance_name}:8443/.well-known/wlcg-tape-rest-api 2>/dev/null | jq -r .sitename"
    )
    print(f"Site name: {site_name}")

    # Get token
    TOKEN_TIMEOUT = 604800
    now = int(time.time())
    later = now + TOKEN_TIMEOUT
    token_eospower1 = eos_client.exec_with_output(
        f". /root/client_env && eosadmin_eos root://{disk_instance_name} token --tree --path '/eos/ctaeos/://:/api/' --expires \"{later}\" --owner poweruser1 --group powerusers --permission prwx"
    )

    # Generate metadata string with correct site name
    valid_metadata = json.dumps({site_name: {"activity": "CTA-Test-HTTP-CI-TEST-activity-passing"}})
    print(f"Valid metadata created: {valid_metadata}")

    # Bring online with valid activity
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-bringonline --staging-metadata '{valid_metadata}' davs://{disk_instance_name}:8443/{valid_instance_file}"
    )

    # Evict
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-evict https://{disk_instance_name}:8443/{valid_instance_file}"
    )

    # Bring online for different instance: No activity should be logged in xrdmgm.log or eosreport
    invalid_metadata = json.dumps({"NotTheSiteName": {"activity": "CTA-Test-HTTP-CI-TEST-activity-passing"}})
    print(f"Invalid metadata created: {invalid_metadata}")

    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-bringonline --staging-metadata '{invalid_metadata}' davs://{disk_instance_name}:8443/{invalid_instance_file}"
    )

    # Evict the retrieved file
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-evict https://{disk_instance_name}:8443/{invalid_instance_file}"
    )

    report_file = _get_report_file_path()
    print(f"Report file: {report_file}")

    # Arguably not the most efficient. If this becomes a problem, just replace by grep
    content = eos_mgm.exec_with_output(f"cat {report_file}")

    # Check that activity is set for staging of file with valid instance name
    valid_line = _find_line(content, "event=stage", valid_instance_file)

    assert valid_line, f"Missing log line for {valid_instance_file}"

    assert (
        "&activity=CTA-Test-HTTP-CI-TEST-activity-passing&" in valid_line
    ), f"Activity not set correctly for valid instance: {valid_line}"

    # Check that activity is NOT set for staging of file with invalid instance name
    invalid_line = _find_line(content, "event=stage", invalid_instance_file)

    assert invalid_line, f"Missing log line for {invalid_instance_file}"

    assert "&activity=&" in invalid_line, f"Activity unexpectedly set for invalid instance: {invalid_line}"


@pytest.mark.eos
def test_xrootd_activity(eos_client, eos_mgm):
    disk_instance_name = eos_mgm.instance_name
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    test_file = archive_directory / "test_xrootd_activity"
    test_file = eos_client.generate_and_archive_file(disk_instance_name, test_file, wait=True)

    # TODO: eventually move definitions like this to a central place
    EOSPOWER_USER = "poweruser1"

    # Retrieve with activity
    eos_client.exec(
        f"KRB5CCNAME=/tmp/{EOSPOWER_USER}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs {disk_instance_name} prepare -s {test_file}?activity=XRootD_Act"
    )

    # Evict
    eos_client.exec(
        f"XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/{EOSPOWER_USER}/krb5cc_0 xrdfs {disk_instance_name} prepare -e {test_file}"
    )

    # Check activity is set for XRootD staging request through xrdfs
    report_file = _get_report_file_path()
    print(f"Report file: {report_file}")
    content = eos_mgm.exec_with_output(f"cat {report_file}")

    xrd_line = _find_line(content, "event=stage", "XRootD_Act")

    assert xrd_line, "Missing log line for XRootD activity"

    assert "&activity=XRootD_Act&" in xrd_line, f"Activity not set correctly for XRootD: {xrd_line}"


@pytest.mark.eos
def test_cleanup_archive_directory(eos_mgm):
    archive_directory = eos_mgm.base_dir_path / "cta" / "gfal"
    print(f"Cleaning up previous archive directory: {archive_directory}")
    eos_mgm.force_remove_directory(archive_directory)
