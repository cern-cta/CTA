# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

from system_tests.helpers.utils.timeout import Timeout
from system_tests.helpers.hosts.remote_host import RemoteHost

PrepareRequest = tuple[str, Path]


@dataclass(frozen=True)
class PrepareRequests:
    requests: list[PrepareRequest]
    remote_manifest: Optional[Path] = None
    request_count: Optional[int] = None

    def __iter__(self) -> Iterator[PrepareRequest]:
        return iter(self.requests)

    def __len__(self) -> int:
        return self.request_count if self.request_count is not None else len(self.requests)

    def __getitem__(self, index: int) -> PrepareRequest:
        return self.requests[index]


class DiskClientHost(RemoteHost):
    def generate_token(
        self,
        disk_instance_name: str,
        *,
        owner: str,
        group: str,
        permission: str,
    ) -> str: ...

    def http_request(
        self,
        url: str,
        *,
        token: Optional[str] = None,
        certificate_options: str = "--insecure",
        data: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        method: Optional[str] = None,
        upload_file: Optional[Path] = None,
    ) -> str:
        command = f'curl --fail-with-body --silent --show-error -L {certificate_options} -H "Accept: application/json"'
        if token is not None:
            command += f' -H "Authorization: Bearer {token}"'
        if data is not None:
            command += f" -H \"Content-Type: application/json\" --data '{json.dumps(data)}'"
        if headers is not None:
            for name, value in headers.items():
                command += f' -H "{name}: {value}"'
        if method is not None:
            command += f" -X {method}"
        if upload_file is not None:
            command += f' --upload-file "{upload_file}"'
        return self.exec_with_output(f'{command} "{url}"')

    def wait_for_archive_locality(
        self,
        rest_api_endpoint: str,
        path: Path,
        expected_locality: str,
        *,
        token: str,
        certificate_options: str,
        wait_timeout_secs: int = 90,
    ) -> dict[str, Any]:
        last_file_info: dict[str, Any] = {}
        last_reported_locality: Optional[str] = None
        print(f"Waiting for {path} to reach archive locality {expected_locality}...")
        with Timeout(wait_timeout_secs) as timeout:
            while not timeout.expired:
                response = self.http_request(
                    f"{rest_api_endpoint}/archiveinfo",
                    token=token,
                    certificate_options=certificate_options,
                    data={"paths": [str(path)]},
                )
                file_infos = json.loads(response)
                if isinstance(file_infos, list):
                    file_info = next(
                        (item for item in file_infos if isinstance(item, dict) and item.get("path") == str(path)),
                        None,
                    )
                    if file_info is not None:
                        last_file_info = file_info
                        locality = file_info.get("locality")
                        if locality != last_reported_locality:
                            print(f"Archive locality for {path}: {locality}")
                            last_reported_locality = locality if isinstance(locality, str) else None
                        if locality == expected_locality:
                            print(f"File {path} reached archive locality {expected_locality}")
                            return file_info
                time.sleep(1)
        raise TimeoutError(
            f"File {path} did not reach archive locality {expected_locality} within {wait_timeout_secs} seconds. "
            f"Last response: {last_file_info}"
        )

    def wait_for_stage_file_status(
        self,
        rest_api_endpoint: str,
        request_id: str,
        path: Path,
        *,
        token: str,
        certificate_options: str,
        expected_state: Optional[str] = None,
        expected_on_disk: Optional[bool] = None,
        wait_timeout_secs: int = 90,
    ) -> dict[str, Any]:
        if expected_state is None and expected_on_disk is None:
            raise ValueError("An expected stage state or onDisk value must be provided")

        last_file_status: dict[str, Any] = {}
        last_reported_status: tuple[object, object] = (None, None)
        expected_description = expected_state if expected_state is not None else f"onDisk={expected_on_disk}"
        print(f"Waiting for {path} in stage request {request_id} to reach {expected_description}...")
        with Timeout(wait_timeout_secs) as timeout:
            while not timeout.expired:
                response = self.http_request(
                    f"{rest_api_endpoint}/stage/{request_id}",
                    token=token,
                    certificate_options=certificate_options,
                )
                request_status = json.loads(response)
                if isinstance(request_status, dict) and isinstance(request_status.get("files"), list):
                    file_status = next(
                        (
                            item
                            for item in request_status["files"]
                            if isinstance(item, dict) and item.get("path") == str(path)
                        ),
                        None,
                    )
                    if file_status is not None:
                        last_file_status = file_status
                        reported_status = (file_status.get("state"), file_status.get("onDisk"))
                        if reported_status != last_reported_status:
                            print(f"Stage status for {path}: {file_status}")
                            last_reported_status = reported_status
                        state_matches = expected_state is not None and file_status.get("state") == expected_state
                        on_disk_matches = expected_on_disk is not None and file_status.get("onDisk") is expected_on_disk
                        if state_matches or on_disk_matches:
                            print(f"File {path} reached the expected stage status")
                            return file_status
                time.sleep(1)
        raise TimeoutError(
            f"File {path} did not reach the expected stage status within {wait_timeout_secs} seconds. "
            f"Last response: {last_file_status}"
        )

    def is_file_on_tape_only(self, disk_instance_name: str, path: Path) -> bool: ...

    def is_file_on_tape(self, disk_instance_name: str, path: Path) -> bool: ...

    def is_file_on_disk(self, disk_instance_name: str, path: Path) -> bool: ...

    def is_file_on_disk_only(self, disk_instance_name: str, path: Path) -> bool: ...

    def wait_for_file_archival(self, disk_instance_name: str, path: Path, *, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for archival of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_tape(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to archive file within timeout of {wait_timeout_secs} seconds")
        print("File archived!")

    def wait_for_file_retrieval(self, disk_instance_name: str, path: Path, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for retrieval of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_disk(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to retrieve file within timeout of {wait_timeout_secs} seconds")
        print("File retrieved")

    def wait_for_file_eviction(self, disk_instance_name: str, path: Path, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for eviction of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_tape_only(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to evict file within timeout of {wait_timeout_secs} seconds")
        print("File evicted")

    def wait_for_files_retrieval(self, disk_instance_name: str, paths: list[Path], wait_timeout_secs: int = 20) -> None:
        for path in paths:
            self.wait_for_file_retrieval(disk_instance_name, path, wait_timeout_secs=wait_timeout_secs)

    def wait_for_files_eviction(self, disk_instance_name: str, paths: list[Path], wait_timeout_secs: int = 20) -> None:
        for path in paths:
            self.wait_for_file_eviction(disk_instance_name, path, wait_timeout_secs=wait_timeout_secs)

    def generate_and_archive_file(
        self,
        disk_instance_name: str,
        destination_path: Path,
        *,
        user: str = "user1",
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
        append_uid: bool = False,
    ) -> Path:
        if append_uid:
            destination_path = Path(f"{destination_path}_{str(uuid.uuid4())[:8]}")
        tmp_file_path = Path(f"/tmp/generated_archive_file_{str(uuid.uuid4())[:8]}")
        self.exec(f"dd if=/dev/urandom of={tmp_file_path}  bs=1M  count=1")
        self.archive_file(
            disk_instance_name,
            destination_path,
            tmp_file_path,
            user=user,
            wait=wait,
            wait_for_evict=wait_for_evict,
            wait_timeout_secs=wait_timeout_secs,
        )
        # Even if we don't wait, the file will already have been copied, so we can safely remove it
        self.exec(f"rm -rf {tmp_file_path}")
        return destination_path

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
    ) -> None: ...

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
        requests = self.retrieve_files(
            disk_instance_name,
            [path],
            user=user,
            wait=wait,
            wait_timeout_secs=wait_timeout_secs,
            activity=activity,
        )
        return requests[0][0]

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
    ) -> PrepareRequests: ...

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
    ) -> PrepareRequests: ...

    def abort_file(
        self,
        disk_instance_name: str,
        request_id: str,
        path: Path,
        *,
        user: str = "poweruser1",
    ) -> None:
        self.abort_files(disk_instance_name, [(request_id, path)], user=user)

    def abort_files(
        self,
        disk_instance_name: str,
        requests: Union[list[PrepareRequest], PrepareRequests],
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None: ...

    def evict_file(
        self,
        disk_instance_name: str,
        path: Path,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        self.evict_files(
            disk_instance_name,
            [path],
            user=user,
            wait=wait,
            wait_timeout_secs=wait_timeout_secs,
        )

    def evict_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        parallelism: int = 1,
    ) -> None: ...

    def evict_directory(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
        parallelism: int = 1,
    ) -> None: ...

    def delete_file(self, disk_instance_name: str, path: Path, *, user: str = "poweruser1") -> None:
        self.delete_files(disk_instance_name, [path], user=user)

    def delete_files(
        self,
        disk_instance_name: str,
        paths: list[Path],
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None: ...

    def delete_directory(
        self,
        disk_instance_name: str,
        directory: Path,
        *,
        user: str = "poweruser1",
        parallelism: int = 1,
    ) -> None: ...

    def file_info(self, disk_instance_name: str, path: Path) -> str: ...
