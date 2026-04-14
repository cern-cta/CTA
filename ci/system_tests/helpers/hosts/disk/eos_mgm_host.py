# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property
from pathlib import Path

from .disk_instance_host import DiskInstanceHost, DiskInstanceImplementation


class EosMgmHost(DiskInstanceHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def implementation(self) -> DiskInstanceImplementation:
        return DiskInstanceImplementation.EOS

    @cached_property
    def instance_name(self) -> str:
        return self.exec_with_output("eos --json version | jq -r '.version[0].EOS_INSTANCE'")

    @cached_property
    def base_dir_path(self) -> Path:
        return Path("/eos") / self.instance_name

    @cached_property
    def workflow_dir(self) -> Path:
        return Path(self.base_dir_path) / "proc" / "cta" / "workflow"

    def force_remove_directory(self, directory: str) -> None:
        self.exec(f"eos rm -rF --no-confirmation {directory} 2>/dev/null || true")

    def list_entries_in_directory(self, directory: str) -> list[str]:
        # This function counts both files and subdirectories
        return self.exec_with_output(f"eos ls {directory}").splitlines()

    def list_subdirectories_in_directory(self, directory: str) -> list[str]:
        output = self.exec_with_output(f"eos ls -l {directory}")
        lines = output.splitlines()

        files = [line.split()[-1] for line in lines if line.startswith("d")]

        return files

    def list_files_in_directory(self, directory: str) -> list[str]:
        output = self.exec_with_output(f"eos ls -l {directory}")
        lines = output.splitlines()

        files = [line.split()[-1] for line in lines if line.startswith("-")]

        return files

    def num_files_in_directory(self, directory: str) -> int:
        # Note that for now this also counts subdirectories
        return int(self.exec_with_output(f"eos ls {directory} | wc -l"))

    def num_files_on_tape_only(self, directory: str) -> int:
        return int(self.exec_with_output(f'eos ls {directory} -y | grep "d0::t1" | wc -l'))

    def num_files_on_disk_only(self, directory: str) -> int:
        return int(self.exec_with_output(f'eos ls {directory} -y | grep "d1::t0" | wc -l'))
