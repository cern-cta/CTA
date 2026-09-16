# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""System tests for cta-restore-files.

The tool lists and restores the entries of the CTA tape file recycle bin. A restore
recreates the file in the EOS namespace (through the EOS gRPC API, authorized by the
namespace keytab) and then restores the tape file copy in the CTA catalogue.
"""

import json
import shlex
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from system_tests.helpers.hosts import CtaCliHost, EosClientHost, EosMgmHost
from system_tests.helpers.utils.timeout import Timeout

# The EOS MGM serves the gRPC namespace API without TLS on this port
EOS_GRPC_PORT = 50051
# The namespace keytab maps the gRPC key to this EOS user, which must be allowed to
# create namespace entries. The MGM setup gives daemon (uid/gid 2) sudo membership.
EOS_GRPC_UID = 2
EOS_GRPC_GID = 2


@dataclass(frozen=True)
class DeletedFile:
    """An archived file that has been deleted, so it sits in the recycle bin."""

    path: Path
    archive_file_id: int
    disk_file_id: str
    size_in_bytes: int
    checksum: str
    vid: str
    copy_nb: int


@dataclass(frozen=True)
class RestoreFilesTool:
    """Runs cta-restore-files on a host with a fixed set of connection options."""

    host: CtaCliHost
    connection_options: str

    def _command(self, selection: str, subcommand: str) -> str:
        return f"cta-restore-files {self.connection_options} {selection} {subcommand}"

    def list(self, selection: str) -> list[dict[str, Any]]:
        """Return the matching recycle-bin entries, parsed from JSON Lines."""
        output = self.host.exec_with_output(self._command(selection, "list --json"))
        return [json.loads(line) for line in output.splitlines() if line.strip()]

    def list_table(self, selection: str) -> str:
        """Return the matching recycle-bin entries in the default table format."""
        return self.host.exec_with_output(self._command(selection, "list"))

    def restore(self, selection: str) -> None:
        # RUST_LOG surfaces the per-file progress in the test output on failure
        self.host.exec(f"RUST_LOG=info {self._command(selection, 'restore')}")


def _wait_for_namespace_entry(
    eos_client: EosClientHost,
    disk_instance_name: str,
    path: Path,
    *,
    wait_timeout_secs: int = 30,
) -> dict[str, Any]:
    """Return the EOS metadata of a path once it exists in the namespace again."""
    with Timeout(wait_timeout_secs) as timeout:
        while not timeout.expired:
            file_info = json.loads(eos_client.file_info(disk_instance_name, path, json_output=True))
            # EOS answers with an error payload (no fxid) while the entry is missing
            if "fxid" in file_info:
                return file_info
            time.sleep(1)
    raise TimeoutError(f"{path} did not reappear in the EOS namespace within {wait_timeout_secs} seconds")


def _cta_cli_option(cta_cli: CtaCliHost, key: str) -> str:
    """Read a single option out of cta-cli.conf, so the test follows the deployment."""
    value = cta_cli.exec_with_output(f"grep -E '^{key}[[:space:]]' /etc/cta/cta-cli.conf | awk '{{print $2}}'")
    assert value, f"{key} is not configured in /etc/cta/cta-cli.conf"
    return value


@pytest.fixture(scope="module")
def restore_files_tool(
    cta_cli: CtaCliHost,
    eos_mgm: EosMgmHost,
    disk_instance_name: str,
) -> Iterator[RestoreFilesTool]:
    """Provide a ready-to-use cta-restore-files, including EOS gRPC authorization.

    Restoring a file that is gone from the namespace requires the EOS gRPC API, which
    is only reachable with a mapped gRPC key from an authorized gateway. Both are set
    up here and removed again afterwards.
    """
    cta_frontend_endpoint = _cta_cli_option(cta_cli, "cta.endpoint")
    ca_cert_bundle = _cta_cli_option(cta_cli, "grpc.tls.chain_cert_path")
    jwt_token_file = _cta_cli_option(cta_cli, "grpc.jwt_token_path")

    # The pod IP is what the MGM sees as the gRPC peer, and /etc/hosts is the only
    # place that has it in the minimal cta-tools image
    client_address = cta_cli.exec_with_output(
        "grep -w \"$(cat /etc/hostname)\" /etc/hosts | awk '{print $1}' | head -1"
    )
    assert client_address, "Failed to determine the address of the cta-restore-files host"

    grpc_key = uuid.uuid4().hex
    keytab_file = Path(f"/tmp/cta-restore-files-namespace-{uuid.uuid4().hex}.keytab")
    keytab_entry = f"{disk_instance_name} http://{disk_instance_name}:{EOS_GRPC_PORT} {grpc_key}\n"

    eos_mgm.exec(f"eos -r 0 0 vid set map -grpc key:{grpc_key} vuid:{EOS_GRPC_UID} vgid:{EOS_GRPC_GID}")
    eos_mgm.exec(f"eos -r 0 0 vid add gateway {shlex.quote(client_address)} grpc")
    try:
        cta_cli.exec(f"printf %s {shlex.quote(keytab_entry)} > {keytab_file} && chmod 600 {keytab_file}")
        yield RestoreFilesTool(
            cta_cli,
            connection_options=(
                f"--cta-frontend-endpoint https://{cta_frontend_endpoint}"
                f" --jwt-token-file {jwt_token_file}"
                f" --ca-cert-bundle {ca_cert_bundle}"
                f" --namespace-keytab-file {keytab_file}"
            ),
        )
    finally:
        cta_cli.exec(f"rm -f {keytab_file}", throw_on_failure=False)
        eos_mgm.exec(f"eos -r 0 0 vid remove gateway {shlex.quote(client_address)} grpc", throw_on_failure=False)
        for mapping in ("uid", "gid"):
            # The rule name contains the quotes that eos itself prints in 'vid ls'
            rule_name = shlex.quote(f'grpc:"key:{grpc_key}":{mapping}')
            eos_mgm.exec(f"eos -r 0 0 vid rm {rule_name}", throw_on_failure=False)


@pytest.fixture
def deleted_file(
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    disk_instance_name: str,
    test_dir: Path,
) -> DeletedFile:
    """Archive a file, record its metadata and delete it into the recycle bin."""
    source_path = Path(f"/tmp/restore_files_source_{uuid.uuid4().hex}")
    file_path = test_dir / f"restore_files_{uuid.uuid4().hex}"

    # Drive state is shared between test modules and other tests leave drives down,
    # so the file would otherwise sit in the archive queue until the timeout expires
    cta_cli.set_all_drives_up()

    eos_client.exec(f"head -c 16384 /dev/urandom > {source_path}")
    try:
        # The archived file is evicted from disk, so the restore has to recreate the
        # namespace entry rather than just relink an existing one
        eos_client.archive_file(disk_instance_name, file_path, source_path, wait_timeout_secs=60)
    finally:
        eos_client.exec(f"rm -f {source_path}", throw_on_failure=False)

    file_info = json.loads(eos_client.file_info(disk_instance_name, file_path, json_output=True))
    disk_file_id = str(int(file_info["fxid"], 16))
    tape_files = json.loads(
        cta_cli.exec_with_output(
            f"cta-admin --json tf ls --fxid {file_info['fxid']} -i {shlex.quote(disk_instance_name)}"
        )
    )
    assert len(tape_files) == 1, f"Expected exactly one tape file for {file_path}"
    tape_file = tape_files[0]

    eos_client.delete_file(disk_instance_name, file_path)

    deleted = DeletedFile(
        path=file_path,
        archive_file_id=int(tape_file["af"]["archiveId"]),
        disk_file_id=disk_file_id,
        size_in_bytes=int(tape_file["af"]["size"]),
        checksum=tape_file["af"]["checksum"][0]["value"],
        vid=tape_file["tf"]["vid"],
        copy_nb=int(tape_file["tf"]["copyNb"]),
    )

    # The frontend reports the deletion asynchronously, so wait for the recycle-bin entry
    with Timeout(30) as timeout:
        while not timeout.expired:
            entries = json.loads(cta_cli.exec_with_output(f"cta-admin --json rtf ls --id {deleted.archive_file_id}"))
            if entries:
                return deleted
            time.sleep(1)
    raise TimeoutError(f"{file_path} did not appear in the recycle bin after deletion")


def test_list_reports_the_deleted_file(
    restore_files_tool: RestoreFilesTool,
    deleted_file: DeletedFile,
    disk_instance_name: str,
) -> None:
    """List must report the recycle-bin entry for every supported selection option."""
    selections = [
        f"--archive-file-id {deleted_file.archive_file_id}",
        f"--vid {shlex.quote(deleted_file.vid)}",
        f"--archive-file-id {deleted_file.archive_file_id} --copy-number {deleted_file.copy_nb}",
    ]

    for selection in selections:
        entries = restore_files_tool.list(selection)
        matching = [entry for entry in entries if entry["archive_file_id"] == deleted_file.archive_file_id]
        assert len(matching) == 1, f"Expected exactly one recycle-bin entry for '{selection}'"
        entry = matching[0]
        assert entry["vid"] == deleted_file.vid
        assert entry["copy_nb"] == deleted_file.copy_nb
        assert entry["disk_instance"] == disk_instance_name
        assert entry["disk_file_id"] == deleted_file.disk_file_id
        assert entry["disk_file_path"] == str(deleted_file.path)
        assert entry["size_in_bytes"] == deleted_file.size_in_bytes
        assert entry["checksum"][0]["value"] == deleted_file.checksum

    # The table output is the default, so make sure it renders the entry as well
    assert deleted_file.vid in restore_files_tool.list_table(f"--archive-file-id {deleted_file.archive_file_id}")


def test_restore_recreates_the_file_in_eos_and_cta(
    restore_files_tool: RestoreFilesTool,
    deleted_file: DeletedFile,
    eos_client: EosClientHost,
    cta_cli: CtaCliHost,
    disk_instance_name: str,
) -> None:
    """Restore must bring the file back into both the EOS namespace and the catalogue."""
    selection = f"--archive-file-id {deleted_file.archive_file_id}"

    restore_files_tool.restore(selection)

    # EOS gets a new disk file id, since the original namespace entry is gone for good
    file_info = _wait_for_namespace_entry(eos_client, disk_instance_name, deleted_file.path)
    restored_disk_file_id = str(int(file_info["fxid"], 16))
    assert int(file_info["size"]) == deleted_file.size_in_bytes
    assert file_info["xattr"]["sys.archive.file_id"] == str(deleted_file.archive_file_id)

    # The catalogue must point the tape file at the recreated namespace entry
    tape_files = json.loads(cta_cli.exec_with_output(f"cta-admin --json tf ls --id {deleted_file.archive_file_id}"))
    assert len(tape_files) == 1, "Expected exactly one restored tape file"
    tape_file = tape_files[0]
    assert tape_file["df"]["diskId"] == restored_disk_file_id
    assert tape_file["df"]["diskInstance"] == disk_instance_name
    assert int(tape_file["af"]["size"]) == deleted_file.size_in_bytes
    assert tape_file["af"]["checksum"][0]["value"] == deleted_file.checksum
    assert tape_file["tf"]["vid"] == deleted_file.vid
    assert int(tape_file["tf"]["copyNb"]) == deleted_file.copy_nb

    # The restored EOS namespace entry must carry the correct checksum (not all zeros)
    # and a valid birth time (not epoch 0)
    # The restored EOS namespace entry must carry the correct checksum (not all zeros)
    # and a valid birth time (not epoch 0)
    assert file_info["checksumvalue"] == deleted_file.checksum, (
        f"EOS checksum mismatch: {file_info['checksumvalue']} != {deleted_file.checksum}"
    )
    assert file_info["btime"] > 0, f"EOS btime is zero (epoch): {file_info.get('btime')}"

    # A restored file is no longer a deleted file
    assert restore_files_tool.list(selection) == []
    assert json.loads(cta_cli.exec_with_output(f"cta-admin --json rtf ls --id {deleted_file.archive_file_id}")) == []
