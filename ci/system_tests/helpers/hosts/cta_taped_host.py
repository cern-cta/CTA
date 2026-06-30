# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property

from .remote_host import RemoteHost


class CtaTapedHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def log_file_location(self) -> str:
        return f"/var/log/cta/cta-taped-{self.drive_name}.log"

    @cached_property
    def drive_name(self) -> str:
        return self.exec_with_output("printenv DRIVE_NAME")

    @cached_property
    def drive_device(self) -> str:
        device: str = self.exec_with_output("printenv DRIVE_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    @cached_property
    def drive_control_path(self) -> str:
        return self.exec_with_output("printenv DRIVE_CONTROL_PATH")

    @cached_property
    def drive_index(self) -> int:
        control_path = self.drive_control_path
        index = control_path.removeprefix("smc")
        if control_path.startswith("smc") and index.isdigit():
            return int(index)
        raise RuntimeError(f"Could not determine drive index from DRIVE_CONTROL_PATH={control_path}")

    @cached_property
    def library_device(self) -> str:
        device: str = self.exec_with_output("printenv LIBRARY_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    @cached_property
    def logical_library_name(self) -> str:
        return self.exec_with_output("printenv LOGICAL_LIBRARY_NAME")

    def label_tapes(self, tapes: list[str]) -> None:
        for tape in tapes:
            self.label_tape(tape)

    def label_tape(self, tape: str) -> None:
        self.exec(f"cta-tape-label --vid {tape} --force")
