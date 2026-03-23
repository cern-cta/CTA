# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import asyncio
from ..connections.remote_connection import RemoteConnection
from subprocess import CompletedProcess
from functools import cached_property
from typing import Optional
import time
from concurrent.futures import Future


class RemoteHost:
    def __init__(self, conn: RemoteConnection):
        self.conn = conn

    @cached_property
    def name(self) -> str:
        return self.conn.name

    @cached_property
    def description(self) -> str:
        return self.conn.description

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> CompletedProcess[bytes]:
        return self.conn.exec(command, capture_output=capture_output, throw_on_failure=throw_on_failure)

    def execWithOutput(self, command: str, throw_on_failure=True) -> str:
        return self.conn.exec(command, capture_output=True, throw_on_failure=throw_on_failure).stdout.strip()

    def exec_async(self, command: str, throw_on_failure=True) -> Future:
        """Start a command asynchronously and return a future that can be awaited.

        This allows concurrent monitoring while the process runs.
        """
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(
            None,
            self.exec,
            command,
            True,
            throw_on_failure,
        )

    def copyTo(self, src_path: str, dst_path: str, throw_on_failure=True, permissions: Optional[str] = None) -> None:
        return self.conn.copyTo(src_path, dst_path, throw_on_failure=throw_on_failure, permissions=permissions)

    def copyFrom(self, src_path: str, dst_path: str, throw_on_failure=True) -> None:
        return self.conn.copyFrom(src_path, dst_path, throw_on_failure=throw_on_failure)

    def restart(self, wait_for_restart=True, throw_on_failure=True) -> None:
        print(f"Restarting {self.name}...")
        self.conn.restart(throw_on_failure)
        if wait_for_restart:
            self.wait_for_host_up()

    def is_host_up(self) -> None:
        return self.conn.is_up()

    def wait_for_host_up(self) -> None:
        print(f"Waiting for {self.name} to be up...")
        while not self.is_host_up():
            time.sleep(1)
        print(f"{self.name} is up")

    def get_ip(self) -> str:
        return self.conn.get_ip()
