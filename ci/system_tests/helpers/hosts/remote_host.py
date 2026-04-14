# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import asyncio
import time
from functools import cached_property
from typing import Optional

from ..connections.remote_connection import ExecResult, RemoteConnection
from ..utils.timeout import Timeout


class RemoteHost:
    def __init__(self, conn: RemoteConnection):
        self.conn = conn

    @cached_property
    def name(self) -> str:
        return self.conn.name

    @cached_property
    def description(self) -> str:
        return self.conn.description

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> ExecResult:
        return self.conn.exec(command, capture_output=capture_output, throw_on_failure=throw_on_failure)

    def exec_with_output(self, command: str, throw_on_failure=True) -> str:
        return self.conn.exec(command, capture_output=True, throw_on_failure=throw_on_failure).stdout.strip()

    def exec_async(self, command: str, throw_on_failure=True) -> asyncio.Future[ExecResult]:
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

    def copy_to(self, src_path: str, dst_path: str, throw_on_failure=True, permissions: Optional[str] = None) -> None:
        return self.conn.copy_to(src_path, dst_path, throw_on_failure=throw_on_failure, permissions=permissions)

    def copy_from(self, src_path: str, dst_path: str, throw_on_failure=True) -> None:
        return self.conn.copy_from(src_path, dst_path, throw_on_failure=throw_on_failure)

    def restart(self, wait_for_restart=True, throw_on_failure=True) -> None:
        print(f"Restarting {self.name}...")
        self.conn.restart(throw_on_failure)
        if wait_for_restart:
            self.wait_for_host_up()

    def is_host_up(self) -> bool:
        return self.conn.is_up()

    def wait_for_host_up(self, wait_timeout_secs: int = 120) -> None:
        print(f"Waiting for {self.name} to be up...")
        with Timeout(wait_timeout_secs) as t:
            while not self.is_host_up() and not t.expired:
                time.sleep(1)
            if t.expired:
                raise TimeoutError(f"Host failed to come up within timeout of {wait_timeout_secs} seconds")
        print(f"{self.name} is up")

    def get_ip(self) -> str:
        return self.conn.get_ip()
