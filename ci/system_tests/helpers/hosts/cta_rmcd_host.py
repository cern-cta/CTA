# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .remote_host import RemoteHost
from typing import List, Tuple
from functools import cached_property
import re


class CtaRmcdHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    @cached_property
    def log_file_location(self) -> str:
        return "/var/log/cta/cta-rmcd.log"

    @cached_property
    def library_device(self) -> str:
        device: str = self.execWithOutput("printenv LIBRARY_DEVICE")
        if not device.startswith("/dev/"):
            device = "/dev/" + device
        return device

    def list_tapes_in_library(self) -> set[str]:
        volume_tags: set[str] = set()
        output: list[str] = self.execWithOutput(f"mtx -f {self.library_device} status").splitlines()
        # Extract tape VIDs
        for line in output:
            if "Storage Element" in line and "Full" in line:
                match = re.search(r"VolumeTag *= *(\S{6})", line)
                if match:
                    volume_tags.add(match.group(1))
        return volume_tags

    def list_loaded_drives(self) -> List[Tuple[int, int]]:
        """Retrieves a list of loaded drives and their corresponding slots for the library device associated with this ctarmcd host."""
        status_output = self.execWithOutput(f"mtx -f {self.library_device} status").splitlines()
        loaded_drives = []

        for line in status_output:
            match = re.search(r"Data Transfer Element (\d+):Full \(Storage Element (\d+) Loaded\)", line)
            if match:
                drive = int(match.group(1))
                slot = int(match.group(2))
                loaded_drives.append((drive, slot))

        return loaded_drives

    def unload_tapes(self):
        """Unloads all loaded tapes from their drives for the library device associated with this rmcd host."""
        library_device = self.library_device
        loaded_drives = self.list_loaded_drives()
        for drive, slot in loaded_drives:
            self.exec(f"mtx -f {library_device} unload {slot} {drive}")

    @staticmethod
    def list_all_tapes_in_libraries(ctarmcd_hosts) -> list[str]:
        """Lists unique volume tags from multiple tape libraries."""
        volume_tags = set()
        for rmcd in ctarmcd_hosts:
            for tape in rmcd.list_tapes_in_library():
                volume_tags.add(tape)
        return sorted(volume_tags)
