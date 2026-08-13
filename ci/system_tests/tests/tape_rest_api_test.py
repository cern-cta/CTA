# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import base64
import functools
import json
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from packaging.version import Version

from system_tests.helpers.hosts import CtaCliHost, DiskClientHost, DiskInstanceHost, EosClientHost, EosMgmHost
from system_tests.helpers.utils import find_line


# =========================================================================
#  Helpers
# =========================================================================


def _get_rest_api_endpoint(disk_client: DiskClientHost, disk_instance: DiskInstanceHost) -> str:
    # Rediscover the endpoint for each workflow instead of sharing state between ordered tests
    print(f"Discovering the Tape REST API endpoint through {disk_instance.webdav_url}")
    response = disk_client.http_request(f"{disk_instance.webdav_url}/.well-known/wlcg-tape-rest-api")
    discovery = json.loads(response)
    assert isinstance(discovery, dict), f"Expected a discovery object, got: {discovery!r}"
    endpoints = discovery.get("endpoints")
    assert isinstance(endpoints, list), f"Discovery response has no endpoints array: {discovery}"
    version_one = next(
        (endpoint for endpoint in endpoints if isinstance(endpoint, dict) and endpoint.get("version") == "v1"),
        None,
    )
    assert version_one is not None, f"Discovery response has no v1 endpoint: {discovery}"
    endpoint_uri = version_one.get("uri")
    assert isinstance(endpoint_uri, str), f"The v1 endpoint URI is not a string: {version_one}"
    assert endpoint_uri, f"The v1 endpoint has an empty URI: {version_one}"
    print(f"Discovered Tape REST API v1 endpoint: {endpoint_uri}")
    return endpoint_uri.rstrip("/")


def _request_id(response: str) -> str:
    response_data = json.loads(response)
    assert isinstance(response_data, dict), f"Expected a stage submission object, got: {response_data!r}"
    assert set(response_data) == {"requestId"}, f"Unexpected stage submission response: {response_data}"
    request_id = response_data["requestId"]
    assert isinstance(request_id, str), f"REST response has no string requestId: {response_data}"
    assert request_id, f"REST response has an empty requestId: {response_data}"
    print(f"Created stage request: {request_id}")
    return request_id


def _assert_empty_response(response: str, operation: str) -> None:
    assert not response, f"{operation} returned an unexpected response body: {response}"
    print(f"{operation} completed successfully")


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    payload_base64 = token.strip().split(".")[1]
    payload_base64 += "=" * (-len(payload_base64) % 4)
    payload = base64.urlsafe_b64decode(payload_base64)
    payload_json = json.loads(payload)
    assert isinstance(payload_json, dict), f"Expected a JWT payload object, got: {payload_json!r}"
    return payload_json


def skip_if_staging_tokens_unsupported(test_function: Any) -> Any:
    @functools.wraps(test_function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        eos_mgm = kwargs["eos_mgm"]
        minimum_version = Version("5.5.0")
        if eos_mgm.eos_version < minimum_version:
            pytest.skip(f"This test requires EOS >= {minimum_version}, got {eos_mgm.eos_version}")
        return test_function(*args, **kwargs)

    return wrapper


def _archive_info(
    disk_client: DiskClientHost,
    rest_api_endpoint: str,
    file_path: Path,
    token: str,
    certificate_options: str = "--insecure",
    expect_error: bool = False,
) -> dict[str, Any]:
    print(f"Querying archive locality for {file_path}")
    response = disk_client.http_request(
        f"{rest_api_endpoint}/archiveinfo",
        token=token,
        certificate_options=certificate_options,
        data={"paths": [str(file_path)]},
    )
    archive_infos = json.loads(response)
    assert isinstance(archive_infos, list), f"Expected an archiveinfo array, got: {archive_infos!r}"
    assert len(archive_infos) == 1, f"Expected one archiveinfo result, got: {archive_infos}"
    archive_info = archive_infos[0]
    assert isinstance(archive_info, dict), f"Expected an archiveinfo object, got: {archive_info!r}"
    assert not (set(archive_info) - {"path", "locality", "error"}), (
        f"Unexpected archiveinfo response fields: {archive_info}"
    )
    assert archive_info.get("path") == str(file_path), f"Archiveinfo returned the wrong path: {archive_info}"
    if expect_error:
        assert "error" in archive_info, f"Archiveinfo did not return an error: {archive_info}"
        assert "locality" not in archive_info, f"Archiveinfo returned locality with an error: {archive_info}"
        print(f"Archiveinfo response for {file_path}: {archive_info}")
        return archive_info
    assert "error" not in archive_info, f"Archiveinfo returned an error: {archive_info}"
    locality = archive_info.get("locality")
    assert locality in {"DISK", "TAPE", "DISK_AND_TAPE", "LOST", "NONE", "UNAVAILABLE"}, (
        f"Archiveinfo returned an invalid locality: {archive_info}"
    )
    print(f"Archiveinfo response for {file_path}: {archive_info}")
    return archive_info


def _stage_request(
    disk_client: DiskClientHost,
    rest_api_endpoint: str,
    file_path: Path,
    token: str,
    certificate_options: str = "--insecure",
) -> str:
    print(f"Submitting a stage request for {file_path}")
    response = disk_client.http_request(
        f"{rest_api_endpoint}/stage",
        token=token,
        certificate_options=certificate_options,
        data={"files": [{"path": str(file_path)}]},
    )
    return _request_id(response)


def _generate_poweruser_scitoken(eos_mgm: EosMgmHost, scope: str) -> str:
    return eos_mgm.generate_scitoken(
        [
            ("scope", scope),
            ("sub", "sub_poweruser1"),
            ("aud", "ctaeos"),
        ],
        keyid="ctaeos",
        timeout=600,
    )


def _delete_stage_request(
    disk_client: DiskClientHost,
    rest_api_endpoint: str,
    request_id: str,
    token: str,
    certificate_options: str = "--insecure",
) -> None:
    print(f"Deleting stage request {request_id}")
    delete_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=token,
        certificate_options=certificate_options,
        method="DELETE",
    )
    _assert_empty_response(delete_response, "Stage request deletion")


def _assert_stage_status(response: str, request_id: str, file_path: Path) -> dict[str, Any]:
    request_status = json.loads(response)
    assert isinstance(request_status, dict), f"Expected a stage status object, got: {request_status!r}"
    allowed_request_fields = {"id", "createdAt", "startedAt", "completedAt", "files"}
    required_request_fields = {"id", "createdAt", "startedAt", "files"}
    assert not (set(request_status) - allowed_request_fields), f"Unexpected stage status fields: {request_status}"
    assert not (required_request_fields - set(request_status)), f"Missing stage status fields: {request_status}"
    assert request_status["id"] == request_id
    assert isinstance(request_status["createdAt"], int)
    assert request_status["createdAt"] >= 0
    assert isinstance(request_status["startedAt"], int)
    assert request_status["startedAt"] >= 0
    if "completedAt" in request_status:
        assert isinstance(request_status["completedAt"], int)
        assert request_status["completedAt"] >= 0

    files = request_status["files"]
    assert isinstance(files, list), f"The stage status files field is not an array: {request_status}"
    assert all(isinstance(file_status, dict) for file_status in files), (
        f"Every stage file status must be an object: {files}"
    )
    matching_files = [file_status for file_status in files if file_status.get("path") == str(file_path)]
    assert len(matching_files) == 1, f"The stage response must contain the requested path exactly once: {files}"
    file_status = matching_files[0]
    assert isinstance(file_status, dict)
    allowed_file_fields = {"path", "finishedAt", "startedAt", "error", "onDisk", "state"}
    assert not (set(file_status) - allowed_file_fields), f"Unexpected stage file status fields: {file_status}"
    assert isinstance(file_status["path"], str)
    if "finishedAt" in file_status:
        assert isinstance(file_status["finishedAt"], int)
    if "startedAt" in file_status:
        assert isinstance(file_status["startedAt"], int)
    if "error" in file_status:
        assert isinstance(file_status["error"], str)
    # Servers may report either disk residency or lifecycle state, but never both for one file
    # In practice, EOS should always report onDisk
    if "onDisk" in file_status:
        assert isinstance(file_status["onDisk"], bool)
        assert "state" not in file_status
    if "state" in file_status:
        assert file_status["state"] in {"SUBMITTED", "STARTED", "CANCELLED", "FAILED", "COMPLETED"}
        assert "onDisk" not in file_status
    assert "onDisk" in file_status or "state" in file_status or "error" in file_status
    print(f"Stage request {request_id} status for {file_path}: {file_status}")
    return file_status


# =========================================================================
#  Fixtures
# =========================================================================


@pytest.fixture(scope="module")
def rest_api_certificate_options(
    eos_client: EosClientHost, eos_mgm: EosMgmHost, tmp_path_factory: pytest.TempPathFactory
) -> str:
    print("Installing CA certificates in the EOS client for Tape REST API requests")
    local_certificates = tmp_path_factory.mktemp("tape_rest_api_certificates")
    eos_mgm.copy_from(Path("/etc/grid-security/certificates"), local_certificates)
    eos_client.copy_to(local_certificates, Path("/etc/grid-security"))

    is_almalinux = eos_client.exec_with_output(
        "grep -qi almalinux /etc/redhat-release 2>/dev/null && echo true || echo false"
    )
    if is_almalinux == "true":
        print("WARNING: Certificate checks are disabled in this test on AlmaLinux.")
        return "--insecure --capath /etc/grid-security/certificates"
    return "--capath /etc/grid-security/certificates"


@pytest.fixture(scope="module")
def rest_api_user_token(disk_client: DiskClientHost, disk_instance_name: str) -> str:
    return disk_client.generate_token(
        disk_instance_name,
        owner="user1",
        group="eosusers",
        permission="rwx",
    )


@pytest.fixture(scope="module")
def rest_api_poweruser_token(disk_client: DiskClientHost, disk_instance_name: str) -> str:
    return disk_client.generate_token(
        disk_instance_name,
        owner="poweruser1",
        group="powerusers",
        permission="prwx",
    )


# =========================================================================
#  Tests
# =========================================================================

# These tests intentionally form one linear file lifecycle. Keep asynchronous submission and polling in the same test
# so rerunning a failed operation does not depend on an unfinished request from a previous run


def test_well_known_endpoint(disk_client: DiskClientHost, disk_instance: DiskInstanceHost) -> None:
    print("Validating the Tape REST API well-known discovery document")
    response = disk_client.http_request(f"{disk_instance.webdav_url}/.well-known/wlcg-tape-rest-api")
    discovery = json.loads(response)
    assert isinstance(discovery, dict), f"Expected a discovery object, got: {discovery!r}"
    print(f"Well-known response: {json.dumps(discovery, indent=2)}")

    # The discovery document is deliberately strict: extension data belongs inside endpoint metadata
    allowed_fields = {"sitename", "description", "endpoints"}
    required_fields = {"sitename", "endpoints"}
    assert not (set(discovery) - allowed_fields), f"Unrecognised well-known fields: {set(discovery) - allowed_fields}"
    assert not (required_fields - set(discovery)), (
        f"Missing required well-known fields: {required_fields - set(discovery)}"
    )
    assert isinstance(discovery["sitename"], str)
    assert discovery["sitename"]
    if "description" in discovery:
        assert isinstance(discovery["description"], str)

    endpoints = discovery["endpoints"]
    assert isinstance(endpoints, list)
    assert endpoints
    versions: list[str] = []
    for endpoint in endpoints:
        assert isinstance(endpoint, dict)
        allowed_endpoint_fields = {"uri", "version", "metadata"}
        required_endpoint_fields = {"uri", "version"}
        assert not (set(endpoint) - allowed_endpoint_fields), f"Unrecognised endpoint fields: {endpoint}"
        assert not (required_endpoint_fields - set(endpoint)), f"Missing required endpoint fields: {endpoint}"
        assert isinstance(endpoint["uri"], str)
        assert endpoint["uri"]
        assert isinstance(endpoint["version"], str)
        assert endpoint["version"]
        if "metadata" in endpoint:
            assert isinstance(endpoint["metadata"], dict)
        versions.append(endpoint["version"])
    assert len(versions) == len(set(versions)), "The discovery response contains duplicate endpoint versions"
    assert versions.count("v1") == 1, "The discovery response must advertise exactly one v1 endpoint"
    print(f"Well-known discovery document is valid; advertised versions: {versions}")


@skip_if_staging_tokens_unsupported
def test_generate_scitoken(eos_mgm: EosMgmHost) -> None:
    print("Generating and validating a SciToken")
    scope = "storage.stage:/eos/"
    scitoken = eos_mgm.generate_scitoken(
        [("scope", scope), ("sub", "test")],
        keyid="ctaeos",
    )
    payload_json = _decode_jwt_payload(scitoken)

    assert payload_json["sub"] == "test", f"SciToken with wrong sub: {payload_json['sub']}"
    assert payload_json["scope"] == scope, f"SciToken with wrong scope: {payload_json['scope']}"
    assert payload_json["wlcg.ver"] == "1.0", f"SciToken with wrong wlcg version: {payload_json['wlcg.ver']}"


def test_archive_file_and_track_archiveinfo(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    disk_instance_name: str,
    cta_cli: CtaCliHost,
    test_dir: Path,
    rest_api_user_token: str,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Uploading and archiving test file {file_path}")
    temporary_file = Path(disk_client.exec_with_output("mktemp /tmp/tape_rest_api.XXXXXX"))
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    try:
        with suppress(RuntimeError):
            disk_client.delete_file(disk_instance_name, file_path)
        disk_client.exec(f'printf Dummy > "{temporary_file}"')

        # Prevent CTA from flushing the upload before archiveinfo can observe its initial DISK locality
        cta_cli.set_all_drives_down()
        print(f"Uploading {temporary_file} to {disk_instance.webdav_url}{file_path} while drives are down")
        upload_response = disk_client.http_request(
            f"{disk_instance.webdav_url}{file_path}",
            token=rest_api_user_token,
            upload_file=temporary_file,
        )
        if upload_response:
            print(f"Upload response: {upload_response}")
        archive_info = _archive_info(
            disk_client,
            rest_api_endpoint,
            file_path,
            rest_api_poweruser_token,
            rest_api_certificate_options,
        )
        assert archive_info["locality"] == "DISK"
        print("The uploaded file is on disk and has not yet been archived")
        cta_cli.set_all_drives_up()

        archived_file_info = disk_client.wait_for_archive_locality(
            rest_api_endpoint,
            file_path,
            "TAPE",
            token=rest_api_poweruser_token,
            certificate_options=rest_api_certificate_options,
            wait_timeout_secs=30,
        )
        assert archived_file_info["path"] == str(file_path)
        assert archived_file_info.get("error") is None
        print(f"File archived successfully: {archived_file_info}")
    finally:
        disk_client.exec(f'rm -f "{temporary_file}"')


def test_archive_metadata_ends_up_in_eos_report(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    disk_instance_name: str,
    eos_mgm: EosMgmHost,
    test_dir: Path,
    rest_api_user_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "archive_metadata_file"
    archive_metadata = {"scheduling_hints": f"test_{str(uuid.uuid4())[:8]}"}
    archive_metadata_b64 = base64.b64encode(json.dumps(archive_metadata, separators=(",", ":")).encode()).decode()

    print(f"Uploading {file_path} with archive metadata: {archive_metadata}")
    with suppress(RuntimeError):
        disk_client.delete_file(disk_instance_name, file_path)
    upload_response = disk_client.http_request(
        f"{disk_instance.webdav_url}{file_path}",
        token=rest_api_user_token,
        certificate_options=rest_api_certificate_options,
        headers={"ArchiveMetadata": archive_metadata_b64},
        method="PUT",
        upload_file=Path("/etc/group"),
    )
    _assert_empty_response(upload_response, "Archive metadata upload")

    # EOS records the opaque base64 value verbatim, allowing CTA scheduling metadata to be audited
    report_file = eos_mgm.get_report_file_path()
    print(f"Checking EOS report file {report_file} for the archive metadata")
    report = eos_mgm.exec_with_output(f"cat {report_file}")
    report_line = find_line(report, f"archivemetadata={archive_metadata_b64}")
    assert report_line, f'Missing EOS report entry for Archive Metadata string: "{archive_metadata_b64}"'
    print(f"Found archive metadata in EOS report: {report_line}")


def test_stage_poll_and_release(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Staging, polling, and releasing {file_path}")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    assert (
        _archive_info(
            disk_client,
            rest_api_endpoint,
            file_path,
            rest_api_poweruser_token,
            rest_api_certificate_options,
        )["locality"]
        == "TAPE"
    )

    request_id = _stage_request(
        disk_client,
        rest_api_endpoint,
        file_path,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )
    initial_status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
    )
    initial_file_status = _assert_stage_status(initial_status_response, request_id, file_path)
    assert "error" not in initial_file_status
    assert initial_file_status.get("state") != "FAILED"

    # Poll the same request here so this test owns the complete asynchronous stage operation
    disk_client.wait_for_stage_file_status(
        rest_api_endpoint,
        request_id,
        file_path,
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        expected_state="COMPLETED",
        expected_on_disk=True,
        wait_timeout_secs=30,
    )
    completed_status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
    )
    completed_file_status = _assert_stage_status(completed_status_response, request_id, file_path)
    assert "error" not in completed_file_status
    assert completed_file_status.get("state") == "COMPLETED" or completed_file_status.get("onDisk") is True

    print(f"Releasing {file_path} from stage request {request_id}")
    release_response = disk_client.http_request(
        f"{rest_api_endpoint}/release/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        data={"paths": [str(file_path)]},
    )
    _assert_empty_response(release_response, "Release request")
    # RELEASE is asynchronous in practice; archiveinfo is the storage-independent source of locality
    released_file_info = disk_client.wait_for_archive_locality(
        rest_api_endpoint,
        file_path,
        "TAPE",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        wait_timeout_secs=90,
    )
    assert released_file_info["path"] == str(file_path)
    assert released_file_info.get("error") is None
    print(f"Released file returned to tape-only locality: {released_file_info}")


def test_cancel_stage_request(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    cta_cli: CtaCliHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Submitting and cancelling a stage request for {file_path}")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    # Keep the request pending long enough to exercise cancellation deterministically
    cta_cli.set_all_drives_down()
    request_id = _stage_request(
        disk_client,
        rest_api_endpoint,
        file_path,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )
    initial_status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
    )
    initial_file_status = _assert_stage_status(initial_status_response, request_id, file_path)
    assert "error" not in initial_file_status
    assert initial_file_status.get("state") not in {"FAILED", "CANCELLED"}

    print(f"Cancelling {file_path} in stage request {request_id}")
    cancel_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}/cancel",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        data={"paths": [str(file_path)]},
    )
    _assert_empty_response(cancel_response, "Stage cancellation")
    disk_client.wait_for_stage_file_status(
        rest_api_endpoint,
        request_id,
        file_path,
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        expected_state="CANCELLED",
        expected_on_disk=False,
        wait_timeout_secs=30,
    )
    cancelled_status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
    )
    cancelled_file_status = _assert_stage_status(cancelled_status_response, request_id, file_path)
    # The specification permits either the state model or the mutually exclusive onDisk model
    assert cancelled_file_status.get("state") == "CANCELLED" or cancelled_file_status.get("onDisk") is False
    print(f"Deleting cancelled stage request {request_id}")
    delete_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        method="DELETE",
    )
    _assert_empty_response(delete_response, "Cancelled stage request deletion")
    cta_cli.set_all_drives_up()

    assert (
        _archive_info(
            disk_client,
            rest_api_endpoint,
            file_path,
            rest_api_poweruser_token,
            rest_api_certificate_options,
        )["locality"]
        == "TAPE"
    )


def test_delete_stage_request(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    cta_cli: CtaCliHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Submitting and deleting a stage request for {file_path}")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    # Keep the request active so DELETE exercises its best-effort cancellation
    cta_cli.set_all_drives_down()
    request_id = _stage_request(
        disk_client,
        rest_api_endpoint,
        file_path,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )
    print(f"Deleting active stage request {request_id}")
    delete_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=rest_api_poweruser_token,
        certificate_options=rest_api_certificate_options,
        method="DELETE",
    )
    _assert_empty_response(delete_response, "Active stage request deletion")
    cta_cli.set_all_drives_up()

    assert (
        _archive_info(
            disk_client,
            rest_api_endpoint,
            file_path,
            rest_api_poweruser_token,
            rest_api_certificate_options,
        )["locality"]
        == "TAPE"
    )


@skip_if_staging_tokens_unsupported
def test_wlcg_scitoken_stage_with_root_scope_and_poll_token(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    disk_instance_name: str,
    eos_mgm: EosMgmHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Staging {file_path} with a root-scoped WLCG SciToken")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    stage_token = _generate_poweruser_scitoken(eos_mgm, "storage.stage:/")
    poll_token = _generate_poweruser_scitoken(eos_mgm, "storage.poll:/")

    request_id = _stage_request(disk_client, rest_api_endpoint, file_path, stage_token)
    disk_client.wait_for_stage_file_status(
        rest_api_endpoint,
        request_id,
        file_path,
        token=poll_token,
        expected_state="COMPLETED",
        expected_on_disk=True,
        wait_timeout_secs=30,
    )
    status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=poll_token,
    )
    completed_file_status = _assert_stage_status(status_response, request_id, file_path)
    assert "error" not in completed_file_status
    assert completed_file_status.get("state") == "COMPLETED" or completed_file_status.get("onDisk") is True

    disk_client.evict_file(disk_instance_name, file_path, wait_timeout_secs=30)
    _delete_stage_request(
        disk_client,
        rest_api_endpoint,
        request_id,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )


@skip_if_staging_tokens_unsupported
def test_wlcg_scitoken_stage_with_test_dir_scope(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    disk_instance_name: str,
    eos_mgm: EosMgmHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Staging {file_path} with a test-dir-scoped WLCG SciToken")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    stage_token = _generate_poweruser_scitoken(eos_mgm, f"storage.stage:{test_dir}")
    poll_token = _generate_poweruser_scitoken(eos_mgm, f"storage.poll:{test_dir}")

    request_id = _stage_request(disk_client, rest_api_endpoint, file_path, stage_token)
    disk_client.wait_for_stage_file_status(
        rest_api_endpoint,
        request_id,
        file_path,
        token=poll_token,
        expected_state="COMPLETED",
        expected_on_disk=True,
        wait_timeout_secs=30,
    )

    disk_client.evict_file(disk_instance_name, file_path, wait_timeout_secs=30)
    _delete_stage_request(
        disk_client,
        rest_api_endpoint,
        request_id,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )


@skip_if_staging_tokens_unsupported
def test_wlcg_scitoken_stage_with_invalid_scope_fails(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    eos_mgm: EosMgmHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Checking that staging {file_path} fails with a wrong WLCG scope")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    stage_token = _generate_poweruser_scitoken(eos_mgm, "storage.stage:/path/does/not/exist")

    request_id = _stage_request(disk_client, rest_api_endpoint, file_path, stage_token)
    status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=stage_token,
    )
    file_status = _assert_stage_status(status_response, request_id, file_path)
    assert "error" in file_status, f"Invalid SciToken request did not report an error: {file_status}"
    _delete_stage_request(
        disk_client,
        rest_api_endpoint,
        request_id,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )


@skip_if_staging_tokens_unsupported
def test_wlcg_scitoken_stage_request_with_poll_scope_fails(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    eos_mgm: EosMgmHost,
    test_dir: Path,
    rest_api_poweruser_token: str,
    rest_api_certificate_options: str,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Checking that a storage.poll WLCG SciToken cannot stage {file_path}")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    poll_token = _generate_poweruser_scitoken(eos_mgm, f"storage.poll:{test_dir}")

    request_id = _stage_request(disk_client, rest_api_endpoint, file_path, poll_token)
    status_response = disk_client.http_request(
        f"{rest_api_endpoint}/stage/{request_id}",
        token=poll_token,
    )
    file_status = _assert_stage_status(status_response, request_id, file_path)
    assert "error" in file_status, f"storage.poll SciToken request did not report an error: {file_status}"
    _delete_stage_request(
        disk_client,
        rest_api_endpoint,
        request_id,
        rest_api_poweruser_token,
        rest_api_certificate_options,
    )


@skip_if_staging_tokens_unsupported
def test_wlcg_scitoken_archiveinfo_with_poll_scope(
    disk_client: DiskClientHost,
    disk_instance: DiskInstanceHost,
    eos_mgm: EosMgmHost,
    test_dir: Path,
) -> None:
    file_path = test_dir / "test_http-rest-api"
    print(f"Checking archiveinfo access for {file_path} with WLCG storage.poll SciTokens")
    rest_api_endpoint = _get_rest_api_endpoint(disk_client, disk_instance)
    valid_poll_token = _generate_poweruser_scitoken(eos_mgm, f"storage.poll:{test_dir}")
    invalid_poll_token = _generate_poweruser_scitoken(eos_mgm, "storage.poll:/path/does/not/exist")

    archive_info = _archive_info(
        disk_client,
        rest_api_endpoint,
        file_path,
        valid_poll_token,
    )
    assert archive_info["path"] == str(file_path)

    invalid_archive_info = _archive_info(
        disk_client,
        rest_api_endpoint,
        file_path,
        invalid_poll_token,
        expect_error=True,
    )

    assert "error" in invalid_archive_info, (
        f"storage.poll request with invalid path did not report an error: {invalid_archive_info}"
    )
