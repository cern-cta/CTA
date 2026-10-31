# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import shlex
import time
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.utils.timeout import Timeout

from .dcache_constants import DCACHE_ADMIN_AUTH, DCACHE_REST_PORT, DCACHE_WEBDAV_PORT
from .disk_client_host import DiskClientHost


class DCacheClientHost(DiskClientHost):
    """A client which accesses dCache through its WebDAV and REST doors."""

    @staticmethod
    def _remote_path(path: Path) -> str:
        return str(path).lstrip("/")

    def _namespace_info(
        self, disk_instance_name: str, path: Path, *, throw_on_failure: bool = True
    ) -> dict[str, object]:
        url = shlex.quote(
            f"https://{disk_instance_name}:{DCACHE_REST_PORT}/api/v1/namespace/{self._remote_path(path)}?locality=true"
        )
        # Bound each poll so an unresponsive door cannot block the wait loop indefinitely.
        output = self.exec_with_output(
            f"curl -ksS --connect-timeout 5 --max-time 10 -u {shlex.quote(DCACHE_ADMIN_AUTH)} {url}",
            throw_on_failure=throw_on_failure,
        )
        return json.loads(output) if output else {}

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
        del user
        destination = shlex.quote(
            f"https://{disk_instance_name}:{DCACHE_WEBDAV_PORT}/{self._remote_path(destination_path)}"
        )
        source = shlex.quote(str(source_path))
        print(f"Uploading {source_path} to dCache path {destination_path}")
        self.exec(
            f"curl -kfsS --connect-timeout 5 --max-time 120 -u {shlex.quote(DCACHE_ADMIN_AUTH)} "
            f"--upload-file {source} {destination}"
        )
        info = self._namespace_info(disk_instance_name, destination_path)
        print(f"Upload completed; namespace info: {json.dumps(info, indent=2)}")
        if wait:
            self.wait_for_file_archival(disk_instance_name, destination_path, wait_timeout_secs=wait_timeout_secs)

    @override
    def is_file_on_tape_only(self, disk_instance_name: str, path: Path) -> bool:
        return self._namespace_info(disk_instance_name, path).get("fileLocality") == "NEARLINE"

    @override
    def is_file_on_tape(self, disk_instance_name: str, path: Path) -> bool:
        return self._namespace_info(disk_instance_name, path).get("fileLocality") in ("NEARLINE", "ONLINE_AND_NEARLINE")

    @override
    def is_file_on_disk(self, disk_instance_name: str, path: Path) -> bool:
        return self._namespace_info(disk_instance_name, path).get("fileLocality") in ("ONLINE", "ONLINE_AND_NEARLINE")

    @override
    def is_file_on_disk_only(self, disk_instance_name: str, path: Path) -> bool:
        return self._namespace_info(disk_instance_name, path).get("fileLocality") == "ONLINE"

    @override
    def delete_file(self, disk_instance_name: str, path: Path, *, user: str = "poweruser1") -> None:
        del user
        url = shlex.quote(f"https://{disk_instance_name}:{DCACHE_WEBDAV_PORT}/{self._remote_path(path)}")
        print(f"Deleting dCache file {path}")
        self.exec(f"curl -kfsS --connect-timeout 5 --max-time 10 -u {shlex.quote(DCACHE_ADMIN_AUTH)} -X DELETE {url}")
        print(f"Delete request completed for {path}")

    def wait_for_file_deletion(self, disk_instance_name: str, path: Path, *, wait_timeout_secs: int = 20) -> None:
        print(f"Waiting up to {wait_timeout_secs} seconds for dCache to remove {path}")
        with Timeout(wait_timeout_secs) as timeout:
            while not timeout.expired:
                response = self._namespace_info(disk_instance_name, path, throw_on_failure=False)
                # dCache versions can encode the status as either a number or a string.
                if str(response.get("status")) == "404" and response.get("title") == "Not Found":
                    print(f"dCache file {path} was removed")
                    return
                time.sleep(0.1)
        raise TimeoutError(f"Failed to delete file within timeout of {wait_timeout_secs} seconds")

    @override
    def file_info(self, disk_instance_name: str, path: Path, *, json_output: bool = False) -> str:
        del json_output
        return json.dumps(self._namespace_info(disk_instance_name, path), indent=2)
