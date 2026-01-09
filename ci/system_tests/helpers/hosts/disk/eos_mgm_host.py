# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .disk_instance_host import DiskInstanceHost, DiskInstanceImplementation
from functools import cached_property


class EosMgmHost(DiskInstanceHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def implementation(self) -> str:
        return DiskInstanceImplementation.EOS

    @cached_property
    def instance_name(self) -> str:
        return self.execWithOutput("eos --json version | jq -r '.version[0].EOS_INSTANCE'")

    @cached_property
    def base_dir_path(self) -> str:
        return f"/eos/{self.instance_name}"

    @cached_property
    def workflow_dir(self) -> str:
        return f"{self.base_dir}/proc/cta/workflow"

    def force_remove_directory(self, directory: str) -> str:
        self.exec(f"eos rm -rF --no-confirmation {directory} 2>/dev/null || true")

    def list_files_in_directory(self, directory: str) -> list[str]:
        # Note that for now this also counts subdirectories
        return self.execWithOutput(f"eos ls {directory}").splitlines()

    def num_files_in_directory(self, directory: str) -> int:
        # Note that for now this also counts subdirectories
        return int(self.execWithOutput(f"eos ls {directory} | wc -l"))

    def num_files_on_tape_only(self, directory: str) -> int:
        return int(self.execWithOutput(f'eos ls {directory} -y | grep "d0::t1" | wc -l'))

    def num_files_on_disk_only(self, directory: str) -> int:
        return int(self.execWithOutput(f'eos ls {directory} -y | grep "d1::t0" | wc -l'))
