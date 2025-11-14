import subprocess
from typing import Optional
from functools import cached_property

from .remote_connection import RemoteConnection

class SSHConnection(RemoteConnection):

    def __init__(self, host: str, user: str):
        super().__init__()
        self.host = host
        self.user = user


    @cached_property
    def name(self) -> str:
        return f"{self.host}"

    @cached_property
    def short_description(self) -> str:
        return f"SSH connection {self.name}"

    def exec(self, command: str, capture_output = False, throw_on_failure = True) -> subprocess.CompletedProcess[bytes]:
        full_command = f"ssh {self.user}@{self.host} \"{command}\""
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"\"{full_command}\" failed with exit code {result.returncode}: {result.stderr}")
        return result

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure = True, permissions: Optional[str] = None) -> None:
        full_command = f"scp {src_path} {self.user}@{self.host}:{dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"\"{full_command}\" failed with exit code {result.returncode}: {result.stderr}")
        if permissions:
            self.exec(f"chmod -R {permissions} {dst_path}")

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure = True) -> None:
        full_command = f"scp {self.user}@{self.host}:{src_path} {dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"\"{full_command}\" failed with exit code {result.returncode}: {result.stderr}")

    def restart(self, throw_on_failure = True) -> None:
        self.exec("reboot now", throw_on_failure=throw_on_failure)

    def is_up(self) -> bool:
        cmd = (
            f"ssh -o BatchMode=yes "
            f"-o ConnectTimeout=2 "
            f"{self.user}@{self.host} true"
        )
        result = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return result.returncode == 0
