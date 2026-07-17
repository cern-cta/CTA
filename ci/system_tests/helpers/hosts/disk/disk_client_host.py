# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import time
import uuid

from ...utils.timeout import Timeout
from ..remote_host import RemoteHost


class DiskClientHost(RemoteHost):
    def is_file_on_tape_only(self, disk_instance_name: str, path: str) -> bool: ...

    def is_file_on_tape(self, disk_instance_name: str, path: str) -> bool: ...

    def is_file_on_disk(self, disk_instance_name: str, path: str) -> bool: ...

    def is_file_on_disk_only(self, disk_instance_name: str, path: str) -> bool: ...

    def wait_for_file_archival(self, disk_instance_name: str, path: str, *, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for archival of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_tape(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to archive file within timeout of {wait_timeout_secs} seconds")
        print("File archived!")

    def wait_for_file_retrieval(self, disk_instance_name: str, path: str, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for retrieval of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_disk(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to retrieve file within timeout of {wait_timeout_secs} seconds")
        print("File retrieved")

    def wait_for_file_eviction(self, disk_instance_name: str, path: str, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting for eviction of {path}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_file_on_tape_only(disk_instance_name, path) and not t.expired:
                time.sleep(0.1)
            if t.expired:
                print(self.file_info(disk_instance_name, path))
                raise TimeoutError(f"Failed to evict file within timeout of {wait_timeout_secs} seconds")
        print("File evicted")

    def generate_and_archive_file(
        self,
        disk_instance_name: str,
        destination_path: str,
        *,
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
        append_uid: bool = False,
    ) -> str:
        if append_uid:
            destination_path = f"{destination_path}_{str(uuid.uuid4())[:8]}"
        tmp_file_path = f"/tmp/generated_archive_file_{str(uuid.uuid4())[:8]}"
        self.exec(f"dd if=/dev/urandom of={tmp_file_path}  bs=1M  count=1")
        self.archive_file(
            disk_instance_name,
            destination_path,
            tmp_file_path,
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
        destination_path: str,
        source_path: str,
        *,
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None: ...

    def retrieve_file(
        self,
        disk_instance_name: str,
        path: str,
        *,
        user: str = "poweruser1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None: ...

    def evict_file(
        self,
        disk_instance_name: str,
        path: str,
        *,
        user: str = "eosadmin1",
        wait: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None: ...

    def delete_file(self, disk_instance_name: str, path: str) -> None: ...

    def file_info(self, disk_instance_name: str, path: str) -> str: ...
