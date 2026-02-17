# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time

from .remote_host import RemoteHost


class CtaCliHost(RemoteHost):
    def __init__(self, conn):
        super().__init__(conn)

    def get_drive_status(self, drive_name: str) -> str:
        output = self.execWithOutput(f"cta-admin --json drive ls {drive_name}")
        drive_list = json.loads(output)

        if len(drive_list) == 0:
            raise RuntimeError(f"Failed to find drive: {drive_name}")

        drive = drive_list[0]
        if drive["driveName"] == drive_name:
            return drive["driveStatus"]

        raise RuntimeError(f"Failing to find drive status for drive: {drive_name}")

    def wait_for_drive_status(self, drive_name: str, desired_status: str, timeout: int = 30):
        print(f"Waiting for drives {drive_name} to be {desired_status}")
        for _ in range(timeout):
            drives_info = json.loads(self.execWithOutput(f"cta-admin --json drive ls '{drive_name}'"))
            if not any(drive["driveStatus"] != desired_status for drive in drives_info):
                print(f"Drives {drive_name} are set {desired_status}")
                return
            time.sleep(1)
        raise RuntimeError(f"Timeout reached while trying to put drives to: {desired_status}")

    def set_drive_up(self, drive_name: str, wait: bool = True):
        self.exec(f"cta-admin dr up '{drive_name}' --reason 'Setting drive up'")
        if wait:
            self.wait_for_drive_status(drive_name, "UP")

    def set_drive_down(self, drive_name: str, wait: bool = True):
        self.exec(f"cta-admin dr down '{drive_name}' --reason 'Setting drive down'")
        if wait:
            self.wait_for_drive_status(drive_name, "DOWN")

    def set_all_drives_up(self, wait: bool = True):
        self.set_drive_up(".*", wait=wait)

    def set_all_drives_down(self, wait: bool = True):
        self.set_drive_down(".*", wait=wait)
