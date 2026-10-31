# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import json
import re
import shlex
import uuid
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from system_tests.helpers.hosts import EosClientHost, EosMgmHost
from system_tests.helpers.test_config import TestConfig

from system_tests.helpers.utils import find_line

# =========================================================================
#  Helpers
# =========================================================================


@dataclass
class GfalParams:
    file_count: int
    file_size_kb: int
    process_count: int


@pytest.fixture(scope="module")
def gfal_params(test_config: TestConfig) -> GfalParams:
    gfal_config = test_config["tests"]["gfal"]
    return GfalParams(
        file_count=gfal_config["file_count"],
        file_size_kb=gfal_config["file_size_kb"],
        process_count=gfal_config["process_count"],
    )


@dataclass
class GfalBatch:
    protocol: str
    directory: Path
    endpoint: str
    writer_auth: str = field(repr=False)
    power_auth: str = field(repr=False)


@pytest.fixture(scope="module")
def gfal_xrootd(test_dir: Path, disk_instance_name: str) -> GfalBatch:
    auth = "KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5"
    return GfalBatch("root", test_dir / "gfal_xrootd", f"root://{disk_instance_name}", auth, auth)


@pytest.fixture(scope="module")
def gfal_https(eos_client: EosClientHost, test_dir: Path, disk_instance_name: str) -> GfalBatch:
    writer = eos_client.generate_token(disk_instance_name, owner="user1", group="eosusers", permission="rwx")
    power = eos_client.generate_token(disk_instance_name, owner="poweruser1", group="powerusers", permission="prwx")
    return GfalBatch(
        "https",
        test_dir / "gfal_https",
        f"https://{disk_instance_name}:8443",
        f"BEARER_TOKEN={shlex.quote(writer)}",
        f"BEARER_TOKEN={shlex.quote(power)}",
    )


def _run_gfal_batch(eos_client: EosClientHost, batch: GfalBatch, params: GfalParams, operation: str) -> None:
    if params.file_count < 1 or params.process_count < 1:
        raise ValueError("GFAL file count and parallelism must be positive")
    started = time.monotonic()
    print(
        f"{operation}: {params.file_count} files in {batch.directory} via {batch.protocol}, "
        f"{params.process_count} workers as poweruser1",
        flush=True,
    )
    prefix = shlex.quote(f"{batch.endpoint}/{batch.directory}/0/file_")
    # Suppress per-file results and staging tokens; keep stderr and exit status.
    worker = f'{batch.power_auth} {operation} {prefix}"$1" >/dev/null'
    eos_client.exec(
        f"set -o pipefail; seq 0 {params.file_count - 1} | "
        f"xargs -P {params.process_count} -n 1 sh -c {shlex.quote(worker)} _"
    )
    print(f"{operation}: all commands succeeded in {time.monotonic() - started:.1f}s", flush=True)


def _check_evicted(
    eos_client: EosClientHost, instance: str, batch: GfalBatch, params: GfalParams, timeout: int
) -> None:
    eos_client.wait_for_directory_eviction(instance, batch.directory, wait_timeout_secs=timeout)
    total, remaining = eos_client.file_eviction_counts(instance, batch.directory)
    assert total == params.file_count, f"Expected {params.file_count} files in {batch.directory}, found {total}"
    assert remaining == 0, f"{remaining} files are not on tape only in {batch.directory}"
    print(f"{batch.protocol}: {total}/{params.file_count} files archived and fully evicted", flush=True)


def _retrieve_gfal(eos_client: EosClientHost, instance: str, batch: GfalBatch, params: GfalParams) -> None:
    _run_gfal_batch(eos_client, batch, params, "gfal-bringonline")
    timeout = 40 + params.file_count // 5
    started = time.monotonic()
    previous = -1
    last_report = started - 10
    while time.monotonic() - started < timeout:
        listing = eos_client.exec_with_output(
            f"eos {shlex.quote(f'root://{instance}')} ls -y {shlex.quote(str(batch.directory / '0'))}"
        )
        # Require a disk replica and a tape replica for every expected file.
        count = sum(bool(re.match(r"^d[1-9][0-9]*::t1\b", line)) for line in listing.splitlines())
        now = time.monotonic()
        if count != previous or now - last_report >= 10:
            print(
                f"{batch.protocol}: {count}/{params.file_count} retrieved after {now - started:.1f}s "
                f"(timeout {timeout}s)",
                flush=True,
            )
            previous, last_report = count, now
        if count == params.file_count:
            return
        time.sleep(1)
    raise TimeoutError(f"{batch.protocol}: only {previous}/{params.file_count} files retrieved in {batch.directory}")


def _evict_gfal(eos_client: EosClientHost, instance: str, batch: GfalBatch, params: GfalParams) -> None:
    _run_gfal_batch(eos_client, batch, params, "gfal-evict")
    _check_evicted(eos_client, instance, batch, params, 20)


def _delete_gfal(eos_client: EosClientHost, instance: str, batch: GfalBatch, params: GfalParams) -> None:
    # HTTPS deletion uses user1; XRootD deletion uses poweruser1, matching the original tests.
    print(f"Deleting {params.file_count} files recursively from {batch.directory} via {batch.protocol}", flush=True)
    timeout = 5 + params.file_count // 9
    deadline = time.monotonic() + timeout
    # Bound the command itself, including a forced kill if GFAL ignores termination.
    eos_client.exec(
        f"{batch.writer_auth} timeout --kill-after=5s {timeout}s "
        f"gfal-rm -r {shlex.quote(f'{batch.endpoint}/{batch.directory}')} >/dev/null"
    )
    # List the surviving parent so an unrelated command failure cannot masquerade as successful deletion.
    while time.monotonic() < deadline:
        entries = eos_client.exec_with_output(
            f"eos {shlex.quote(f'root://{instance}')} ls {shlex.quote(str(batch.directory.parent))}"
        ).splitlines()
        if batch.directory.name not in [entry.rstrip("/") for entry in entries]:
            print(f"{batch.protocol}: batch directory and all files deleted", flush=True)
            return
        time.sleep(1)
    raise TimeoutError(f"GFAL deletion did not remove {batch.directory}")


# =========================================================================
#  Tests
# =========================================================================


def test_setup_client_gfal_xrootd(eos_client: EosClientHost) -> None:
    eos_client.exec("dnf install -y python3-gfal2-util gfal2-plugin-xrootd")


def test_archive_gfal_xrootd(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_xrootd: GfalBatch
) -> None:
    # GFAL/root has always used xrdcp for archival; GFAL coverage starts at bringonline.
    eos_client.generate_and_archive_files(
        disk_instance_name,
        gfal_xrootd.directory,
        file_count=gfal_params.file_count,
        file_size_kb=gfal_params.file_size_kb,
        parallelism=gfal_params.process_count,
    )


def test_retrieve_gfal_xrootd(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_xrootd: GfalBatch
) -> None:
    _retrieve_gfal(eos_client, disk_instance_name, gfal_xrootd, gfal_params)


def test_evict_gfal_xrootd(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_xrootd: GfalBatch
) -> None:
    _evict_gfal(eos_client, disk_instance_name, gfal_xrootd, gfal_params)


def test_delete_gfal_xrootd(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_xrootd: GfalBatch
) -> None:
    _delete_gfal(eos_client, disk_instance_name, gfal_xrootd, gfal_params)


def test_setup_client_gfal_https(eos_client: EosClientHost) -> None:
    eos_client.exec("dnf install -y python3-gfal2-util gfal2-plugin-http")
    eos_client.exec("sed -i 's/INSECURE=false/INSECURE=true/g' /etc/gfal2.d/http_plugin.conf")


def test_archive_gfal_https(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_https: GfalBatch
) -> None:
    if gfal_params.file_count < 1 or gfal_params.process_count < 1:
        raise ValueError("GFAL file count and parallelism must be positive")
    batch = gfal_https
    source = Path(f"/tmp/gfal_archive_{uuid.uuid4().hex}")
    # Preserve the original HTTPS workload: a shared 15 KiB zero-filled source, independent of configured file size.
    print(f"Archiving {gfal_params.file_count} files of 15 KiB via HTTPS to {batch.directory} as user1", flush=True)
    eos_client.exec(f"eos root://{disk_instance_name} mkdir -p {shlex.quote(str(batch.directory / '0'))}")
    started = time.monotonic()
    try:
        eos_client.exec(f"dd if=/dev/zero of={source} bs=1K count=15 status=none")
        prefix = shlex.quote(f"{batch.endpoint}/{batch.directory}/0/file_")
        worker = f'{batch.writer_auth} gfal-copy {source} {prefix}"$1" >/dev/null'
        eos_client.exec(
            f"set -o pipefail; seq 0 {gfal_params.file_count - 1} | "
            f"xargs -P {gfal_params.process_count} -n 1 sh -c {shlex.quote(worker)} _"
        )
    finally:
        eos_client.exec(f"rm -f {source}", throw_on_failure=False)
    print(f"HTTPS copies completed in {time.monotonic() - started:.1f}s; waiting for tape archival", flush=True)
    _check_evicted(eos_client, disk_instance_name, batch, gfal_params, 4 * (40 + gfal_params.file_count // 5))


def test_retrieve_gfal_https(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_https: GfalBatch
) -> None:
    _retrieve_gfal(eos_client, disk_instance_name, gfal_https, gfal_params)


def test_evict_gfal_https(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_https: GfalBatch
) -> None:
    _evict_gfal(eos_client, disk_instance_name, gfal_https, gfal_params)


def test_delete_gfal_https(
    eos_client: EosClientHost, disk_instance_name: str, gfal_params: GfalParams, gfal_https: GfalBatch
) -> None:
    _delete_gfal(eos_client, disk_instance_name, gfal_https, gfal_params)


def test_gfal_activity_ends_up_in_eos_report(
    eos_client: EosClientHost, eos_mgm: EosMgmHost, disk_instance_name: str, test_dir: Path
) -> None:
    valid_instance_file = test_dir / "test_gfal_activity_valid_instance"
    invalid_instance_file = test_dir / "test_gfal_activity_invalid_instance"
    valid_instance_file = eos_client.generate_and_archive_file(
        disk_instance_name, valid_instance_file, wait=False, append_uid=True
    )
    invalid_instance_file = eos_client.generate_and_archive_file(
        disk_instance_name, invalid_instance_file, wait=False, append_uid=True
    )
    eos_client.wait_for_file_archival(disk_instance_name, valid_instance_file)
    eos_client.wait_for_file_archival(disk_instance_name, invalid_instance_file)
    eos_client.wait_for_file_eviction(disk_instance_name, valid_instance_file)
    eos_client.wait_for_file_eviction(disk_instance_name, invalid_instance_file)

    # Query .well-known tape rest api endpoint to get the sitename
    site_name = eos_client.exec_with_output(
        f"curl --insecure https://{disk_instance_name}:8443/.well-known/wlcg-tape-rest-api "
        f"2>/dev/null | jq -r .sitename"
    )
    print(f"Site name: {site_name}")

    token_eospower1 = eos_client.generate_token(
        disk_instance_name, owner="poweruser1", group="powerusers", permission="prwx"
    )

    # Generate metadata string with correct site name
    valid_metadata = json.dumps({site_name: {"activity": "CTA-Test-HTTP-CI-TEST-activity-passing"}})
    print(f"Valid metadata created: {valid_metadata}")

    # Bring online with valid activity
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-bringonline --staging-metadata '{valid_metadata}' "
        f"davs://{disk_instance_name}:8443/{valid_instance_file} >/dev/null"
    )

    # Evict
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-evict https://{disk_instance_name}:8443/{valid_instance_file} >/dev/null"
    )

    # Bring online for different instance: No activity should be logged in xrdmgm.log or eosreport
    invalid_metadata = json.dumps({"NotTheSiteName": {"activity": "CTA-Test-HTTP-CI-TEST-activity-passing"}})
    print(f"Invalid metadata created: {invalid_metadata}")

    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-bringonline --staging-metadata '{invalid_metadata}' "
        f"davs://{disk_instance_name}:8443/{invalid_instance_file} >/dev/null"
    )

    # Evict the retrieved file
    eos_client.exec(
        f"BEARER_TOKEN={token_eospower1} gfal-evict "
        f"https://{disk_instance_name}:8443/{invalid_instance_file} >/dev/null"
    )

    report_file = eos_mgm.get_report_file_path()
    print(f"Report file: {report_file}")

    # Arguably not the most efficient. If this becomes a problem, just replace by grep
    content = eos_mgm.exec_with_output(f"cat {report_file}")

    # Check that activity is set for staging of file with valid instance name
    valid_line = find_line(content, "event=stage", str(valid_instance_file))

    assert valid_line, f"Missing log line for {valid_instance_file}"

    assert "&activity=CTA-Test-HTTP-CI-TEST-activity-passing&" in valid_line, (
        f"Activity not set correctly for valid instance: {valid_line}"
    )

    # Check that activity is NOT set for staging of file with invalid instance name
    invalid_line = find_line(content, "event=stage", str(invalid_instance_file))

    assert invalid_line, f"Missing log line for {invalid_instance_file}"

    assert "&activity=&" in invalid_line, f"Activity unexpectedly set for invalid instance: {invalid_line}"


def test_xrootd_activity_ends_up_in_eos_report(eos_client: EosClientHost, eos_mgm: EosMgmHost, test_dir: Path) -> None:
    disk_instance_name = eos_mgm.instance_name
    test_file = test_dir / "test_xrootd_activity"
    test_file = eos_client.generate_and_archive_file(disk_instance_name, test_file, wait=True)

    # TODO: eventually move definitions like this to a central place
    eospower_user = "poweruser1"

    # Retrieve with activity
    eos_client.exec(
        f"KRB5CCNAME=/tmp/{eospower_user}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs {disk_instance_name} "
        f"prepare -s {test_file}?activity=XRootD_Act"
    )

    # Evict
    eos_client.exec(
        f"XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/{eospower_user}/krb5cc_0 xrdfs {disk_instance_name} "
        f"prepare -e {test_file}"
    )

    # Check activity is set for XRootD staging request through xrdfs
    report_file = eos_mgm.get_report_file_path()
    print(f"Report file: {report_file}")
    content = eos_mgm.exec_with_output(f"cat {report_file}")

    xrd_line = find_line(content, "event=stage", "XRootD_Act")

    assert xrd_line, "Missing log line for XRootD activity"

    assert "&activity=XRootD_Act&" in xrd_line, f"Activity not set correctly for XRootD: {xrd_line}"


def test_add_errors_to_whitelist(error_whitelist: set[str]) -> None:
    error_whitelist.add("In OStoreDB::RepackArchiveReportBatch::report(): async job update failed.")
