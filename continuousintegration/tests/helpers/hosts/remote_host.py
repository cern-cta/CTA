from ..connections.remote_connection import RemoteConnection
from subprocess import CompletedProcess
from typing import Optional

class RemoteHost():
    def __init__(self, conn: RemoteConnection):
        self.conn = conn

    def get_name(self) -> str:
        return self.conn.get_name()

    def get_short_description(self) -> str:
        return self.conn.get_short_description()

    def exec(self, command: str, capture_output = False, throw_on_failure = True) -> CompletedProcess[bytes]:
        return self.conn.exec(command, capture_output=capture_output, throw_on_failure=throw_on_failure)

    def execWithOutput(self, command: str, throw_on_failure = True) -> str:
        return self.conn.exec(command, capture_output=True, throw_on_failure=throw_on_failure).stdout.decode().strip()

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure = True, permissions: Optional[str] = None) -> None:
        return self.conn.copyTo(src_path, dst_path, throw_on_failure=throw_on_failure, permissions=permissions)

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure = True) -> None:
        return self.conn.copyFrom(src_path, dst_path, throw_on_failure=throw_on_failure)
