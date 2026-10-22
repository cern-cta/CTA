# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


import asyncio
import json
import re
import shlex
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Union

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import ExecResult, RemoteConnection
from system_tests.helpers.utils.timeout import Timeout
from .disk_client_host import DiskClientHost, PrepareRequest, PrepareRequests
from .eos_constants import EOS_TAPE_FILESYSTEM_ID


class EosClientHost(DiskClientHost):
    def prepare_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
    ) -> ExecResult:
        """Submit one XRootD PREPARE stage request containing all supplied paths."""
        path_arguments = " ".join(shlex.quote(str(path)) for path in paths)
        return self.exec(
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"xrdfs {shlex.quote(disk_instance_name)} prepare -s {path_arguments}",
            capture_output=True,
            throw_on_failure=False,
        )

    def query_prepare(
        self,
        disk_instance_name: str,
        request_id: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
    ) -> dict[str, Any]:
        """Return EOS's per-path status for an XRootD PREPARE request."""
        path_arguments = " ".join(shlex.quote(str(path)) for path in paths)
        output = self.exec_with_output(
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"xrdfs {shlex.quote(disk_instance_name)} query prepare {shlex.quote(request_id)} {path_arguments}"
        )
        return json.loads(output)

    def abort_prepare(
        self,
        disk_instance_name: str,
        request_id: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
    ) -> ExecResult:
        """Abort the supplied paths from an XRootD PREPARE request."""
        path_arguments = " ".join(shlex.quote(str(path)) for path in paths)
        return self.exec(
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"xrdfs {shlex.quote(disk_instance_name)} prepare -a {shlex.quote(request_id)} {path_arguments}",
            capture_output=True,
            throw_on_failure=False,
        )

    def evict_prepare(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
    ) -> ExecResult:
        """Evict the supplied paths through an XRootD PREPARE request."""
        path_arguments = " ".join(shlex.quote(str(path)) for path in paths)
        return self.exec(
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f"xrdfs {shlex.quote(disk_instance_name)} prepare -e {path_arguments}",
            capture_output=True,
            throw_on_failure=False,
        )

    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @override
    def generate_token(
        self,
        disk_instance_name: str,
        *,
        owner: str,
        group: str,
        permission: str,
    ) -> str:
        expires = int((datetime.now() + timedelta(days=1)).timestamp())
        print(f"Generating a Tape REST API bearer token for {owner}")
        token = self.exec_with_output(
            "XrdSecPROTOCOL=krb5 KRB5CCNAME=/tmp/eosadmin1/krb5cc_0 eos -r 0 0 "
            f"root://{disk_instance_name} token --tree --path '/eos/ctaeos/://:/api/' --expires {expires} "
            f"--owner {owner} --group {group} --permission {permission}"
        )
        if not token:
            raise RuntimeError(f"Token generation returned an empty token for {owner}")
        return token

    def count_files_in_namespace(self, eos_host: str, dest_dir: Path, num_dirs: int, count_procs: int) -> int:
        """Count files in namespace using parallel queries on the remote host.

        Requires count_files.py to be deployed to /tmp/count_files.py first.
        """
        cmd = (
            f"python3 /tmp/count_files.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {count_procs}"
        )
        output = self.exec_with_output(cmd)
        try:
            return int(output.strip())
        except ValueError:
            return 0

    def generate_and_archive_files(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        file_count: int,
        file_size_kb: int,
        parallelism: int = 1,
    ) -> None:
        """Create directory/0/file_<index> and wait for the entire batch to be on tape only.

        The directory must be dedicated to this batch. Files remain available for subsequent tests.
        """
        if file_count < 1 or file_size_kb < 1 or parallelism < 1:
            raise ValueError("File count, file size, and parallelism must all be positive")
        started = time.monotonic()
        print(
            f"Archiving {file_count} files of {file_size_kb} KiB to "
            f"{disk_instance_name}:{directory} as user1 using {parallelism} workers",
            flush=True,
        )
        self.exec(f"eos root://{disk_instance_name} mkdir -p {shlex.quote(str(directory / '0'))}")

        # Run parallel workers in the pod: concurrent Kubernetes exec calls share mutable API client state.
        # Generate distinct contents for every file and propagate both generation and copy failures.
        worker = (
            f"dd if=/dev/urandom bs=1K count={file_size_kb} status=none | "
            "KRB5CCNAME=/tmp/user1/krb5cc_0 xrdcp - "
            f'{shlex.quote(f"root://{disk_instance_name}/{directory}/0/file_")}"$1"'
        )
        self.exec(
            f"set -o pipefail; seq 0 {file_count - 1} | "
            f"xargs -P {parallelism} -n 1 bash -o pipefail -c {shlex.quote(worker)} _"
        )

        print(f"All {file_count} disk copies completed in {time.monotonic() - started:.1f}s", flush=True)
        # Tape-only locations with no pending unlinks establish both archival and eviction.
        timeout = 4 * (40 + file_count // 5)
        print(f"Waiting up to {timeout}s for archival and automatic eviction", flush=True)
        self.wait_for_directory_eviction(disk_instance_name, directory, wait_timeout_secs=timeout)
        total, remaining = self.file_eviction_counts(disk_instance_name, directory)
        assert total == file_count, "File count changed during archival"
        assert remaining == 0, "Some archived files are not fully evicted"
        print(f"Archive complete: {total}/{file_count} files on tape only in {time.monotonic() - started:.1f}s")

    @override
    def archive_file(
        self,
        disk_instance_name: str,
        destination_path: Path,
        source_path: Path,
        *,
        user: str = "user1",
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        print(f"Copying {source_path} to {destination_path} on disk instance {disk_instance_name}")
        self.exec(f"KRB5CCNAME=/tmp/{user}/krb5cc_0 xrdcp {source_path} root://{disk_instance_name}/{destination_path}")
        if wait:
            self.wait_for_file_archival(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)
            if wait_for_evict:
                self.wait_for_file_eviction(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)

    @override
    def retrieve_file(
        self,
        disk_instance_name: str,
        path: Path,
        *,
        user: str = "poweruser1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        activity: Optional[str] = None,
    ) -> str:
        return super().retrieve_file(
            disk_instance_name,
            path,
            user=user,
            wait=wait,
            wait_timeout_secs=wait_timeout_secs,
            activity=activity,
        )

    @override
    def retrieve_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        activity: Optional[str] = None,
        parallelism: int = 1,
    ) -> PrepareRequests:
        """Submit one retrieve request per path, using workers in the EOS client pod."""
        if not paths:
            return PrepareRequests([])

        request_list: list[PrepareRequest] = []
        for path_batch in self._batches(paths):
            request_list.extend(
                self._retrieve_from_path_stream(
                    disk_instance_name,
                    self._paths_command(path_batch),
                    user=user,
                    activity=activity,
                    parallelism=parallelism,
                )
            )
        requests = PrepareRequests(request_list)
        if wait:
            self.wait_for_files_retrieval(
                disk_instance_name, [path for _, path in requests], wait_timeout_secs=wait_timeout_secs
            )
        return requests

    def _wait_for_directory_retrieval(
        self,
        disk_instance_name: str,
        directory: Path,
        remote_manifest: Path,
        expected_files: int,
        *,
        wait_timeout_secs: int,
    ) -> None:
        print(f"Waiting for retrieval of {expected_files} files...")
        # Read the requested paths once; unrelated files must not hide missing or unretrieved files.
        manifest_paths = self.exec_with_output(f"cut -f 2 {shlex.quote(str(remote_manifest))}").splitlines()
        expected_paths = set(manifest_paths)
        if len(manifest_paths) != expected_files or len(expected_paths) != expected_files:
            raise ValueError(f"Invalid retrieval manifest {remote_manifest}: expected {expected_files} unique paths")
        started = time.monotonic()
        deadline = started + wait_timeout_secs
        last_report = started - 10
        previous_count = -1
        while time.monotonic() < deadline:
            listing = self.exec_with_output(
                f"eos {shlex.quote(f'root://{disk_instance_name}')} find -f --fs {shlex.quote(str(directory))}"
            )
            retrieved = set()
            for line in listing.splitlines():
                match = re.fullmatch(r"(.+) fsid=([0-9]+(?:,[0-9]+)*)?\s*", line)
                if match is None:
                    raise ValueError(f"Invalid EOS filesystem listing entry: {line!r}")
                locations = {int(fsid) for fsid in match[2].split(",")} if match[2] else set()
                if EOS_TAPE_FILESYSTEM_ID in locations and locations - {0, EOS_TAPE_FILESYSTEM_ID}:
                    retrieved.add(match[1])
            count = len(expected_paths & retrieved)
            now = time.monotonic()
            if count != previous_count or now - last_report >= 10:
                print(
                    f"Retrieved {count}/{expected_files} files after {now - started:.1f}s "
                    f"(timeout: {wait_timeout_secs}s; manifest: {remote_manifest})",
                    flush=True,
                )
                previous_count = count
                last_report = now
            if count == expected_files:
                print("Files retrieved")
                return
            time.sleep(0.5)
        raise TimeoutError(f"Failed to retrieve all files within timeout of {wait_timeout_secs} seconds")

    @override
    def retrieve_directory(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        user: str = "poweruser1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        activity: Optional[str] = None,
        parallelism: int = 1,
    ) -> PrepareRequests:
        """Retrieve every file below a directory, checking completion with bulk namespace queries."""
        # Keep request IDs in a remote manifest so abort_files() can stream them without hitting argument-size limits.
        remote_manifest = Path(f"/tmp/cta-prepare-requests-{uuid.uuid4().hex}")
        requests = self._retrieve_from_path_stream(
            disk_instance_name,
            self._directory_files_command(disk_instance_name, directory),
            user=user,
            activity=activity,
            parallelism=parallelism,
            remote_manifest=remote_manifest,
        )
        if wait:
            self._wait_for_directory_retrieval(
                disk_instance_name,
                directory,
                remote_manifest,
                len(requests),
                wait_timeout_secs=wait_timeout_secs,
            )
        return requests

    def _retrieve_from_path_stream(
        self,
        disk_instance_name: str,
        path_stream_command: str,
        *,
        user: str,
        activity: Optional[str],
        parallelism: int,
        remote_manifest: Optional[Path] = None,
    ) -> PrepareRequests:
        if parallelism < 1:
            raise ValueError("parallelism must be at least 1")

        # xargs supplies the path and activity as $1 and $2 to each worker. Each successful worker emits one
        # tab-separated request-id/path record, which is safe to process concurrently because each record is short.
        worker = (
            'path="$1"; activity="$2"; retrieve_path="$path"; '
            '[ -z "$activity" ] || retrieve_path="$path?activity=$activity"; '
            "request_id=$(KRB5CCNAME=/tmp/"
            f"{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 xrdfs {shlex.quote(disk_instance_name)} "
            'prepare -s "$retrieve_path") || exit; printf "%s\\t%s\\n" "$request_id" "$path"'
        )
        command = (
            f"set -o pipefail; {path_stream_command} | xargs -r -P {parallelism} -I{{}} "
            f"sh -c {shlex.quote(worker)} _ '{{}}' {shlex.quote(activity or '')}"
        )
        if remote_manifest is not None:
            # tee retains the records for a later abort while wc keeps the response returned to Python constant-sized.
            command += f" | tee {shlex.quote(str(remote_manifest))} | wc -l"
        output = self.exec_with_output(command)
        if remote_manifest is not None:
            return PrepareRequests([], remote_manifest, int(output))

        requests = [
            (request_id, Path(path))
            for request_id, path in (line.split("\t", maxsplit=1) for line in output.splitlines())
        ]
        return PrepareRequests(requests, remote_manifest)

    @override
    def abort_file(
        self,
        disk_instance_name: str,
        request_id: str,
        path: Path,
        *,
        user: str = "poweruser1",
    ) -> None:
        super().abort_file(disk_instance_name, request_id, path, user=user)

    @override
    def abort_files(
        self,
        disk_instance_name: str,
        requests: Union[list[PrepareRequest], PrepareRequests],
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None:
        """Abort retrieve requests using workers in the EOS client pod."""
        if not requests:
            return
        if parallelism < 1:
            raise ValueError("parallelism must be at least 1")

        worker = (
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f'xrdfs {shlex.quote(disk_instance_name)} prepare -a "$1" "$2"'
        )
        if isinstance(requests, PrepareRequests) and requests.remote_manifest is not None:
            # Each xargs item is one manifest line. Split it at the first tab inside the worker rather than expanding
            # every request into the outer shell command, which would exceed ARG_MAX for large directories.
            manifest_worker = (
                'tab=$(printf "\\t"); request_id=${1%%"$tab"*}; path=${1#*"$tab"}; '
                f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
                f'xrdfs {shlex.quote(disk_instance_name)} prepare -a "$request_id" "$path"'
            )
            self.exec(
                f"xargs -r -d '\\n' -P {parallelism} -I{{}} sh -c {shlex.quote(manifest_worker)} _ '{{}}' "
                f"< {shlex.quote(str(requests.remote_manifest))} && "
                f"rm -f {shlex.quote(str(requests.remote_manifest))}"
            )
            return

        request_list = list(requests)
        # Explicit Python lists do need to cross the exec boundary, so cap each command to a conservative size.
        for offset in range(0, len(request_list), 100):
            arguments = " ".join(
                f"{shlex.quote(request_id)} {shlex.quote(str(path))}"
                for request_id, path in request_list[offset : offset + 100]
            )
            self.exec(f"printf '%s\\0' {arguments} | xargs -r -0 -n 2 -P {parallelism} sh -c {shlex.quote(worker)} _")

    @override
    def evict_file(
        self,
        disk_instance_name: str,
        path: Path,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        super().evict_file(
            disk_instance_name,
            path,
            user=user,
            wait=wait,
            wait_timeout_secs=wait_timeout_secs,
        )

    @override
    def evict_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        parallelism: int = 1,
    ) -> None:
        evicted_paths = []
        for path_batch in self._batches(paths):
            evicted_paths.extend(
                self._run_path_operation(
                    self._paths_command(path_batch),
                    self._evict_worker(disk_instance_name, user),
                    parallelism,
                )
            )
        if wait:
            self.wait_for_files_eviction(disk_instance_name, evicted_paths, wait_timeout_secs=wait_timeout_secs)

    @override
    def evict_directory(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        parallelism: int = 1,
    ) -> None:
        evicted_paths = self._run_path_operation(
            self._directory_files_command(disk_instance_name, directory),
            self._evict_worker(disk_instance_name, user),
            parallelism,
        )
        if wait:
            self.wait_for_files_eviction(disk_instance_name, evicted_paths, wait_timeout_secs=wait_timeout_secs)

    def _evict_worker(self, disk_instance_name: str, user: str) -> str:
        return (
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 eos -r 0 0 "
            f'root://{shlex.quote(disk_instance_name)} file drop "$1" 1 >/dev/null'
        )

    @override
    def delete_file(self, disk_instance_name: str, path: Path, *, user: str = "poweruser1") -> None:
        super().delete_file(disk_instance_name, path, user=user)

    @override
    def delete_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None:
        for path_batch in self._batches(paths):
            self._run_path_operation(
                self._paths_command(path_batch),
                self._delete_worker(disk_instance_name, user),
                parallelism,
            )

    @override
    def delete_directory(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None:
        self._run_path_operation(
            self._directory_files_command(disk_instance_name, directory),
            self._delete_worker(disk_instance_name, user),
            parallelism,
        )

    def _delete_worker(self, disk_instance_name: str, user: str) -> str:
        return (
            f"KRB5CCNAME=/tmp/{shlex.quote(user)}/krb5cc_0 XrdSecPROTOCOL=krb5 "
            f'eos root://{shlex.quote(disk_instance_name)} rm -rf --no-confirmation "$1" >/dev/null'
        )

    @staticmethod
    def _paths_command(paths: list[Path]) -> str:
        if not paths:
            return ":"
        quoted_paths = " ".join(shlex.quote(str(path)) for path in paths)
        return f"printf '%s\\n' {quoted_paths}"

    @staticmethod
    def _batches(paths: list[Path], batch_size: int = 100) -> Iterator[list[Path]]:
        for offset in range(0, len(paths), batch_size):
            yield paths[offset : offset + batch_size]

    @staticmethod
    def _directory_files_command(disk_instance_name: str, directory: Path) -> str:
        return f"eos root://{shlex.quote(disk_instance_name)} find -f {shlex.quote(str(directory))}"

    def _run_path_operation(self, path_stream_command: str, worker: str, parallelism: int) -> list[Path]:
        if parallelism < 1:
            raise ValueError("parallelism must be at least 1")

        # Report a path only when its worker succeeds. pipefail also propagates failures from the path producer.
        reporting_worker = f'{worker} && printf "%s\\n" "$1"'
        output = self.exec_with_output(
            f"set -o pipefail; {path_stream_command} | xargs -r -P {parallelism} -I{{}} "
            f"sh -c {shlex.quote(reporting_worker)} _ '{{}}'"
        )
        return [Path(path) for path in output.splitlines()]

    def retrieve_async(
        self,
        eos_host: str,
        dest_dir: Path,
        num_dirs: int,
        num_procs: int,
        *,
        batch_size: int = 500,
        discovery_procs: int = 5,
        krb5_cache: str = "/tmp/poweruser1/krb5cc_0",
        activity: str = "T0Reprocess",
    ) -> asyncio.Future[ExecResult]:
        """Start retrieve (prepare/stage-in) asynchronously. Returns a future that can be awaited."""
        cmd = (
            f"python3 -u /tmp/xrootd_retrieve.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--batch-size {batch_size} "
            f"--discovery-procs {discovery_procs} "
            f"--krb5-cache {krb5_cache} "
            f"--activity {activity}"
        )
        return self.exec_async(cmd)

    @override
    def is_file_on_tape_only(self, disk_instance_name: str, path: Path) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "d0::t1" | wc -l')) == 1

    @override
    def is_file_eviction_complete(self, disk_instance_name: str, path: Path) -> bool:
        return self.file_eviction_counts(disk_instance_name, path) == (1, 0)

    @override
    def is_file_on_tape(self, disk_instance_name: str, path: Path) -> bool:
        return int(self.exec_with_output(f'eos root://{disk_instance_name} ls {path} -y | grep "::t1" | wc -l')) == 1

    @override
    def is_file_on_disk(self, disk_instance_name: str, path: Path) -> bool:
        listing = self.exec_with_output(
            f"eos {shlex.quote(f'root://{disk_instance_name}')} ls {shlex.quote(str(path))} -y"
        )
        return any(re.match(r"^d[1-9][0-9]*::", line) for line in listing.splitlines())

    @override
    def is_file_on_disk_only(self, disk_instance_name: str, path: Path) -> bool:
        listing = self.exec_with_output(
            f"eos {shlex.quote(f'root://{disk_instance_name}')} ls {shlex.quote(str(path))} -y"
        )
        return any(re.match(r"^d[1-9][0-9]*::t0\b", line) for line in listing.splitlines())

    def file_eviction_counts(self, disk_instance_name: str, path: Path) -> tuple[int, int]:
        """Return total files and files with incomplete eviction using one recursive EOS query."""
        endpoint = shlex.quote(f"root://{disk_instance_name}")
        output = self.exec_with_output(f"eos {endpoint} find -f --fs --nunlink {shlex.quote(str(path))}")
        lines = output.splitlines()
        remaining = 0

        for line in lines:
            # Match the final fields, allowing spaces in paths and empty location lists.
            match = re.fullmatch(r".+ fsid=([0-9]+(?:,[0-9]+)*)? nunlink=([0-9]+)\s*", line)
            if match is None:
                raise ValueError(f"Invalid EOS filesystem listing entry: {line!r}")

            # Pending FST deletions must finish before a recall can safely start.
            if match[1] != str(EOS_TAPE_FILESYSTEM_ID) or int(match[2]) != 0:
                remaining += 1

        return len(lines), remaining

    @override
    def wait_for_directory_eviction(
        self, disk_instance_name: str, directory: Path, wait_timeout_secs: int = 20
    ) -> None:
        print(f"Waiting for eviction of files in {directory}...")
        previous_remaining: Optional[int] = None
        started = time.monotonic()
        last_report = started - 10
        with Timeout(wait_timeout_secs) as timeout:
            while not timeout.expired:
                total, remaining = self.file_eviction_counts(disk_instance_name, directory)
                if remaining == 0:
                    print(f"All files in {directory} evicted")
                    return
                now = time.monotonic()
                if remaining != previous_remaining or now - last_report >= 10:
                    print(
                        f"{total - remaining}/{total} files on tape only with disk deletions complete in {directory}; "
                        f"{remaining} pending after {now - started:.1f}s (timeout: {wait_timeout_secs}s)",
                        flush=True,
                    )
                    previous_remaining = remaining
                    last_report = now
                time.sleep(1)

        raise TimeoutError(
            f"Failed to evict {previous_remaining} files in {directory} within {wait_timeout_secs} seconds"
        )

    @override
    def file_info(self, disk_instance_name: str, path: Path, *, json_output: bool = False) -> str:
        json_option = "-j " if json_output else ""
        endpoint = shlex.quote(f"root://{disk_instance_name}")
        return self.exec_with_output(f"eos {json_option}{endpoint} file info {shlex.quote(str(path))}")
