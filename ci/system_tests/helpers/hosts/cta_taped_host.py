# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from functools import cached_property
from pathlib import Path

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .remote_host import RemoteHost


class CtaTapedHost(RemoteHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @cached_property
    def log_file_path(self) -> Path:
        return Path("/var") / "log" / "cta" / "cta-taped.log"

    @cached_property
    def drive_name(self) -> str:
        return self.exec_with_output("printenv DRIVE_NAME")

    @cached_property
    def process_name(self) -> str:
        return "taped"

    @cached_property
    def drive_device(self) -> str:
        device: str = self.exec_with_output("printenv DRIVE_DEVICE")
        if not device.startswith("/dev/") and "://" not in device:
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

    @cached_property
    def is_stress_mode(self) -> bool:
        """True when this taped pod is configured with StressMode = yes (no real SCSI hardware)."""
        device = self.exec_with_output("printenv DRIVE_DEVICE")
        return device == "stress://"

    def setup_stress_tmpfs(self, base_dir: str) -> None:
        """Create the shared tmpfs directory layout required by StressDrive and
        NullMediaChangerFacade.  Safe to call from multiple pods simultaneously
        because mkdir -p is idempotent.

        Layout created:
          <base_dir>/tapes/   -- one subdirectory per VID, created on first archive
          <base_dir>/drives/  -- one symlink per drive, managed by NullMediaChangerFacade
        """
        self.exec(f"mkdir -p {base_dir}/tapes {base_dir}/drives")
