import subprocess
from typing import Optional

from .remote_connection import RemoteConnection

class SSHConnection(RemoteConnection):

    def __init__(self, host: str, user: str):
        super().__init__()
        self.host = host
        self.user = user

    def exec(self, command: str, capture_output = False, throw_on_failure = True) -> subprocess.CompletedProcess[bytes]:
        full_command = f"ssh {self.user}@{self.host} \"{command}\""
        result = subprocess.run(full_command, shell=True, capture_output=capture_output)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"ssh exec of {command} failed with exit code {result.returncode}: {result.stderr}")
        return result

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure = True, permissions: Optional[str] = None) -> None:
        full_command = f"scp {src_path} {self.user}@{self.host}:{dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"scp failed with exit code {result.returncode}: {result.stderr}")
        if permissions:
            self.exec(f"chmod -R {permissions} {dst_path}")

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure = True) -> None:
        full_command = f"scp {self.user}@{self.host}:{src_path} {dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if throw_on_failure and result.returncode != 0:
            raise RuntimeError(f"scp failed with exit code {result.returncode}: {result.stderr}")
