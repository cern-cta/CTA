# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .remote_host import RemoteHost
from functools import cached_property


class CtaTapedHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def log_file_location(self) -> str:
        return f"/var/log/cta/cta-taped-{self.drive_name}.log"

    @cached_property
    def drive_name(self) -> str:
        return self.execWithOutput("printenv DRIVE_NAME")

    @cached_property
    def drive_device(self) -> str:
        device: str = self.execWithOutput("printenv DRIVE_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    @cached_property
    def library_device(self) -> str:
        device: str = self.execWithOutput("printenv LIBRARY_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    def label_tapes(self, tapes: list[str]) -> None:
        for tape in tapes:
            self.label_tape(tape)

    def label_tape(self, tape: str) -> None:
        self.exec(f"cta-tape-label --vid {tape} --force")
