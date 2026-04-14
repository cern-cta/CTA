# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from functools import cached_property
from typing import Optional, Protocol


@dataclass
class ExecResult:
    stdout: str
    stderr: str
    success: bool


class RemoteConnection(Protocol):

    @cached_property
    def name(self) -> str: ...

    @cached_property
    def description(self) -> str: ...

    def exec(self, command: str, capture_output=False, throw_on_failure=True) -> ExecResult: ...

    def copy_to(
        self, src_path: str, dst_path: str, throw_on_failure=True, permissions: Optional[str] = None
    ) -> None: ...

    def copy_from(self, src_path: str, dst_path: str, throw_on_failure=True) -> None: ...

    def restart(self, throw_on_failure=True) -> None: ...

    def is_up(self) -> bool: ...

    def get_ip(self) -> str: ...
