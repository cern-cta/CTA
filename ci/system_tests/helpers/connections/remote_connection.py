# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol, Union


@dataclass
class ExecResult:
    stdout: str
    stderr: str
    success: bool


class RemoteConnection(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def description(self) -> str: ...

    def exec(
        self, command: str, capture_output: bool = False, throw_on_failure: bool = True, print_command: bool = False
    ) -> ExecResult: ...

    def copy_to(
        self,
        src_path: Union[str, Path],
        dst_path: Union[str, Path],
        throw_on_failure: bool = True,
        permissions: Optional[str] = None,
    ) -> None: ...

    def copy_from(
        self, src_path: Union[str, Path], dst_path: Union[str, Path], throw_on_failure: bool = True
    ) -> None: ...

    def restart(self, throw_on_failure: bool = True) -> None: ...

    def is_up(self) -> bool: ...

    def get_ip(self) -> str: ...
