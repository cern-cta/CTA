# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import datetime
from functools import cached_property
from pathlib import Path

from typing_extensions import override

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .disk_instance_host import DiskInstanceHost, DiskInstanceImplementation


class EosMgmHost(DiskInstanceHost):
    def __init__(self, conn: RemoteConnection) -> None:
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
    def webdav_url(self) -> str:
        return f"https://{self.instance_name}:8443"

    @cached_property
    def workflow_dir(self) -> Path:
        return Path(self.base_dir_path) / "proc" / "cta" / "workflow"

    @override
    def force_remove_directory(self, directory: Path) -> None:
        self.exec(f"eos rm -rF --no-confirmation {directory} 2>/dev/null || true")

    @override
    def list_entries_in_directory(self, directory: Path) -> list[str]:
        # This function counts both files and subdirectories
        return self.exec_with_output(f"eos ls {directory}").splitlines()

    @override
    def list_subdirectories_in_directory(self, directory: Path) -> list[str]:
        output = self.exec_with_output(f"eos ls -l {directory}")
        lines = output.splitlines()

        return [line.split()[-1] for line in lines if line.startswith("d")]

    @override
    def list_files_in_directory(self, directory: Path) -> list[str]:
        output = self.exec_with_output(f"eos ls -l {directory}")
        lines = output.splitlines()

        return [line.split()[-1] for line in lines if line.startswith("-")]

    @override
    def num_files_in_directory(self, directory: Path) -> int:
        # Note that for now this also counts subdirectories
        return int(self.exec_with_output(f"eos ls {directory} | wc -l"))

    @override
    def num_files_on_tape_only(self, directory: Path) -> int:
        return int(self.exec_with_output(f'eos ls {directory} -y | grep "d0::t1" | wc -l'))

    @override
    def num_files_on_disk_only(self, directory: Path) -> int:
        return int(self.exec_with_output(f'eos ls {directory} -y | grep "d1::t0" | wc -l'))

    def get_report_file_path(self) -> Path:
        now = datetime.datetime.now()
        base_path = Path("/var") / "log" / "eos" / "report" / f"{now:%Y}" / f"{now:%m}"
        file_name = f"{now:%Y}{now:%m}{now:%d}.eosreport"
        return base_path / file_name

    @override
    def mkdir(self, directory: Path, parent: bool = True) -> None:
        flags = ""
        if parent:
            flags += "-p"
        self.exec(f'eos mkdir {flags} "{directory}"')
