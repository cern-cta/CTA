# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import re
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path

from system_tests.helpers.connections.remote_connection import RemoteConnection
from .remote_host import RemoteHost


class CtaRmcdHost(RemoteHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    @cached_property
    def log_file_path(self) -> Path:
        return Path("/var") / "log" / "cta" / "cta-rmcd.log"

    @cached_property
    def library_device(self) -> str:
        device: str = self.exec_with_output("printenv LIBRARY_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    def list_tapes_in_library(self) -> set[str]:
        volumes = json.loads(self.exec_with_output("cta-smc -q V --json"))
        return {volume["vid"] for volume in volumes}

    def list_loaded_drives(self) -> list[tuple[int, int]]:
        """Retrieve loaded drive ordinals and their source slots using mtx."""
        status_output = self.exec_with_output(f"mtx -f {self.library_device} status").splitlines()
        loaded_drives = []

        for line in status_output:
            match = re.search(r"Data Transfer Element (\d+):Full \(Storage Element (\d+) Loaded\)", line)
            if match:
                loaded_drives.append((int(match.group(1)), int(match.group(2))))

        return loaded_drives

    def unload_tapes(self) -> None:
        for drive, slot in self.list_loaded_drives():
            self.exec(f"mtx -f {self.library_device} unload {slot} {drive}")

    def list_mounted_tapes(self) -> list[tuple[int, str]]:
        drives = json.loads(self.exec_with_output("cta-smc -q D --json"))
        return [(drive["driveOrdinal"], drive["vid"]) for drive in drives if drive["vid"]]

    @staticmethod
    def list_all_tapes_in_libraries(cta_rmcd_hosts: Sequence["CtaRmcdHost"]) -> list[str]:
        """List unique volume tags from multiple tape libraries."""
        volume_tags = set()
        for rmcd in cta_rmcd_hosts:
            volume_tags.update(rmcd.list_tapes_in_library())
        return sorted(volume_tags)
