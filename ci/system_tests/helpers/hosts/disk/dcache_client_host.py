# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import shlex
import time

from .disk_client_host import DiskClientHost
from ...utils.timeout import Timeout


class DCacheClientHost(DiskClientHost):
    """A client which accesses dCache through its WebDAV and REST doors."""

    def __init__(self, conn, endpoint: str = "store-door-svc"):
        super().__init__(conn)
        self.endpoint = endpoint

    @staticmethod
    def _remote_path(path: str) -> str:
        return str(path).lstrip("/")

    def _namespace_info(self, path: str, *, throw_on_failure: bool = True) -> dict:
        url = shlex.quote(f"http://{self.endpoint}:3880/api/v1/namespace/{self._remote_path(path)}?locality=true")
        output = self.exec_with_output(
            f"curl -ksS -u admin:dickerelch {url}",
            throw_on_failure=throw_on_failure,
        )
        return json.loads(output) if output else {}

    def archive_file(
        self,
        disk_instance_name: str,
        destination_path: str,
        source_path: str,
        *,
        wait: bool = True,
        wait_for_evict: bool = True,
        wait_timeout_secs: int = 20,
    ) -> None:
        destination = shlex.quote(f"https://{self.endpoint}:8083/{self._remote_path(destination_path)}")
        source = shlex.quote(source_path)
        self.exec(f"curl -kfsS -u admin:dickerelch --upload-file {source} " f"{destination}")
        if wait:
            self.wait_for_file_archival(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)

    def is_file_on_tape_only(self, disk_instance_name: str, path: str) -> bool:
        return self._namespace_info(path).get("fileLocality") == "NEARLINE"

    def is_file_on_tape(self, disk_instance_name: str, path: str) -> bool:
        return self._namespace_info(path).get("fileLocality") in ("NEARLINE", "ONLINE_AND_NEARLINE")

    def is_file_on_disk(self, disk_instance_name: str, path: str) -> bool:
        return self._namespace_info(path).get("fileLocality") in ("ONLINE", "ONLINE_AND_NEARLINE")

    def is_file_on_disk_only(self, disk_instance_name: str, path: str) -> bool:
        return self._namespace_info(path).get("fileLocality") == "ONLINE"

    def delete_file(self, disk_instance_name: str, path: str) -> None:
        url = shlex.quote(f"https://{self.endpoint}:8083/{self._remote_path(path)}")
        self.exec(f"curl -kfsS -u admin:dickerelch -X DELETE " f"{url}")

    def wait_for_file_deletion(self, path: str, *, wait_timeout_secs: int = 20) -> None:
        with Timeout(wait_timeout_secs) as timeout:
            while not timeout.expired:
                response = self._namespace_info(path, throw_on_failure=False)
                if response.get("status") == 404 and response.get("title") == "Not Found":
                    return
                time.sleep(0.1)
        raise TimeoutError(f"Failed to delete file within timeout of {wait_timeout_secs} seconds")

    def file_info(self, disk_instance_name: str, path: str) -> str:
        return json.dumps(self._namespace_info(path), indent=2)
