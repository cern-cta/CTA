from typing import Any
import subprocess
from remote_connection import RemoteConnection

class SSHConnection(RemoteConnection):

    def __init__(self, host: str, user: str):
        super().__init__()
        self.host = host
        self.user = user

    def run(self, command: str) -> bool:
        full_command = f"ssh {self.user}@{self.host} \"{command}\""
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0

    def copyTo(self, src_path: str, dst_path: str) -> bool:
        full_command = f"scp {src_path} {self.user}@{self.host}:{dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0

    def copyFrom(self, src_path: str, dst_path: str) -> bool:
        full_command = f"scp {self.user}@{self.host}:{src_path} {dst_path}"
        result = subprocess.run(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
