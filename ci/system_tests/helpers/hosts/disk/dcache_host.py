# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property
from pathlib import Path
import shlex

from .disk_instance_host import DiskInstanceHost, DiskInstanceImplementation


class DCacheHost(DiskInstanceHost):
    def __init__(self, conn, endpoint: str = "store-door-svc"):
        super().__init__(conn)
        self.endpoint = endpoint

    @cached_property
    def implementation(self) -> DiskInstanceImplementation:
        return DiskInstanceImplementation.DCACHE

    @cached_property
    def instance_name(self) -> str:
        return "dcache"

    @cached_property
    def base_dir_path(self) -> Path:
        return Path("/data")

    def mkdir(self, directory: str, parent: bool = True) -> None:
        path = Path(directory)
        directories = list(reversed([path, *path.parents])) if parent else [path]
        for item in directories:
            if item == Path("/"):
                continue
            url = shlex.quote(f"https://{self.endpoint}:8083/{str(item).lstrip('/')}")
            # Existing collections return 405, which is harmless for mkdir -p semantics.
            self.exec(
                f"status=$(curl -ksS -o /dev/null -w '%{{http_code}}' -u admin:dickerelch -X MKCOL {url}); "
                f'test "$status" = 201 -o "$status" = 405'
            )

    def force_remove_directory(self, directory: str) -> None:
        url = shlex.quote(f"https://{self.endpoint}:8083/{str(directory).lstrip('/')}")
        self.exec(f"curl -ksS -u admin:dickerelch -X DELETE {url}", throw_on_failure=False)
