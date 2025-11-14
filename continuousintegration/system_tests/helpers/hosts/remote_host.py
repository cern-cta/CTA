from ..connections.remote_connection import RemoteConnection
from subprocess import CompletedProcess
from functools import cached_property
from typing import Optional
import time


class RemoteHost:
    def __init__(self, conn: RemoteConnection):
        self.conn = conn
        # print("Initialising remote host: " + self.description)

    @cached_property
    def name(self) -> str:
        return self.conn.name

    @cached_property
    def description(self) -> str:
        return self.conn.description

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> CompletedProcess[bytes]:
        return self.conn.exec(command, capture_output=capture_output, throw_on_failure=throw_on_failure)

    def execWithOutput(self, command: str, throw_on_failure=True) -> str:
        return self.conn.exec(command, capture_output=True, throw_on_failure=throw_on_failure).stdout.decode().strip()

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure=True, permissions: Optional[str] = None) -> None:
        return self.conn.copyTo(src_path, dst_path, throw_on_failure=throw_on_failure, permissions=permissions)

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure=True) -> None:
        return self.conn.copyFrom(src_path, dst_path, throw_on_failure=throw_on_failure)

    def restart(self, throw_on_failure=True) -> None:
        return self.conn.restart(throw_on_failure)

    def is_host_up(self) -> None:
        return self.conn.is_up()

    def wait_for_host_up(self) -> None:
        print(f"Waiting for {self.name} to be up...")
        while not self.is_host_up():
            time.sleep(1)
        print(f"{self.name} is up")
