# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import shlex
import socket
import subprocess
from pathlib import Path
from typing import Optional

from typing_extensions import override

from .remote_connection import ExecResult, RemoteConnection


class SSHConnection(RemoteConnection):
    def __init__(self, host: str, user: str) -> None:
        super().__init__()
        self.host = host
        self.user = user

    @property
    @override
    def name(self) -> str:
        return f"{self.host}"

    @property
    @override
    def description(self) -> str:
        return f"SSH connection {self.name}"

    @override
    def exec(
        self, command: str, capture_output: bool = False, throw_on_failure: bool = True, print_command: bool = False
    ) -> ExecResult:
        if print_command:
            print(command)
        remote = f"{self.user}@{self.host}"
        command_args = ["ssh", remote, command]
        full_command = shlex.join(command_args)
        result = subprocess.run(command_args, capture_output=capture_output, check=False)
        success = result.returncode == 0
        if throw_on_failure and not success:
            raise RuntimeError(f'"{full_command}" failed with exit code {result.returncode}: {result.stderr}')
        stdout = result.stdout if capture_output else b""
        stderr = result.stderr if capture_output else b""
        return ExecResult(stdout=stdout.decode(), stderr=stderr.decode(), success=success)

    @override
    def copy_to(
        self,
        src_path: Path,
        dst_path: Path,
        throw_on_failure: bool = True,
        permissions: Optional[str] = None,
    ) -> None:
        command_args = ["scp", str(src_path), f"{self.user}@{self.host}:{dst_path}"]
        full_command = shlex.join(command_args)
        result = subprocess.run(command_args, capture_output=True, check=False)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{full_command}" failed with exit code {result.returncode}: {result.stderr}')
        if permissions:
            target = dst_path
            if self.exec(f"test -d {dst_path}", capture_output=True, throw_on_failure=False).success:
                target /= src_path.name
            self.exec(f"chmod {permissions} {target}")

    @override
    def copy_from(self, src_path: Path, dst_path: Path, throw_on_failure: bool = True) -> None:
        command_args = ["scp", f"{self.user}@{self.host}:{src_path}", str(dst_path)]
        full_command = shlex.join(command_args)
        result = subprocess.run(command_args, capture_output=True, check=False)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f'"{full_command}" failed with exit code {result.returncode}: {result.stderr}')

    @override
    def restart(self, throw_on_failure: bool = True) -> None:
        self.exec("reboot now", throw_on_failure=throw_on_failure)

    @override
    def is_up(self) -> bool:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=2", f"{self.user}@{self.host}", "true"]
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return result.returncode == 0

    @override
    def get_ip(self) -> str:
        return socket.gethostbyname(self.host)
