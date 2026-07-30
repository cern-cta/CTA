# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time
from collections.abc import Callable
from typing import Any

from ..connections.remote_connection import RemoteConnection
from ..utils.timeout import Timeout
from .remote_host import RemoteHost


class CtaCliHost(RemoteHost):
    def __init__(self, conn: RemoteConnection) -> None:
        super().__init__(conn)

    def get_drive_status(self, drive_name: str) -> str:
        output = self.exec_with_output(f"cta-admin --json drive ls {drive_name}")
        drive_list = json.loads(output)

        if len(drive_list) == 0:
            raise RuntimeError(f"Failed to find drive: {drive_name}")

        drive = drive_list[0]
        if drive["driveName"] == drive_name:
            return drive["driveStatus"]

        raise RuntimeError(f"Failing to find drive status for drive: {drive_name}")

    def wait_for_drive_status(self, drive_name: str, desired_status: str, *, timeout: int = 10) -> None:
        print(f"Waiting for drives {drive_name} to be {desired_status}")
        for _ in range(timeout):
            drives_info = json.loads(self.exec_with_output(f"cta-admin --json drive ls '{drive_name}'"))
            if not any(drive["driveStatus"] != desired_status for drive in drives_info):
                print(f"Drives {drive_name} are set {desired_status}")
                return
            time.sleep(1)
        raise RuntimeError(f"Timeout reached while trying to put drives to: {desired_status}")

    def set_drive_up(self, drive_name: str, *, wait: bool = True) -> None:
        self.exec(f"cta-admin dr up '{drive_name}' --reason 'Setting drive up'")
        if wait:
            self.wait_for_drive_status(drive_name, "UP")

    def set_drive_down(self, drive_name: str, *, wait: bool = True) -> None:
        self.exec(f"cta-admin dr down '{drive_name}' --reason 'Setting drive down'")
        if wait:
            self.wait_for_drive_status(drive_name, "DOWN")

    def set_all_drives_up(self, *, wait: bool = True) -> None:
        self.set_drive_up(".*", wait=wait)

    def set_all_drives_down(self, *, wait: bool = True) -> None:
        self.set_drive_down(".*", wait=wait)

    def list_all_tape_vids(self) -> list[str]:
        output = self.exec_with_output("cta-admin --json tape ls --all")
        tape_list = json.loads(output)
        return [tape["vid"] for tape in tape_list]

    def file_exists_in_cta(self, vid: str, archive_id: str) -> bool:
        # Ls by --id is annoying because it will exit with a failure if the id does not exist
        return int(self.exec_with_output(f"cta-admin --json tf ls -v {vid} | grep {archive_id} | wc -l")) == 1

    def get_single_ls_item(
        self,
        ls_command: str,
        filter_func: Callable[[dict[str, Any]], bool],
    ) -> dict[str, Any]:
        ls_out = self.exec_with_output("cta-admin --json " + ls_command)
        ls_json = json.loads(ls_out)
        # If only cta-admin was a well designed API we wouldn't have to do this...
        filtered_list = list(filter(filter_func, ls_json))
        assert len(filtered_list) == 1
        return filtered_list[0]

    def list_writable_tapes(self) -> list[dict[str, Any]]:
        tape_ls_json = json.loads(self.exec_with_output("cta-admin --json tape ls --all"))
        return [tape for tape in tape_ls_json if not tape.get("full", False)]

    def get_first_vid_containing_files(self) -> str:
        tapes = json.loads(self.exec_with_output("cta-admin --json tape ls --all"))
        for tape in tapes:
            occupancy = tape.get("occupancy", 0)
            last_fseq = tape.get("lastFseq", 0)
            if occupancy not in (None, "0", 0) and last_fseq not in (None, "0", 0):
                return tape["vid"]
        raise RuntimeError("Failed to find a VID to repack")

    def get_number_of_files_on_tape(self, vid: str) -> int:
        tapefile_json = self.exec_with_output(f"cta-admin --json tf ls --vid {vid}")
        return len(json.loads(tapefile_json))

    def wait_for_tape_state(self, vid: str, expected_state: str, *, timeout_secs: int = 60) -> None:
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            tape_json = json.loads(
                self.exec_with_output(f"cta-admin --json tape ls --state {expected_state} --vid {vid}")
            )
            if any(tape.get("vid") == vid for tape in tape_json):
                return
            time.sleep(1)
        raise TimeoutError(f"Tape {vid} did not reach state {expected_state} within {timeout_secs} seconds")

    def modify_tape_state(self, vid: str, state: str, reason: str = "Testing", wait: bool = True) -> None:
        print(f"Marking tape {vid} as {state}")
        self.exec(f"cta-admin tape ch --state {state} --reason '{reason}' --vid {vid}")
        if wait:
            self.wait_for_tape_state(vid, state)

    def remove_repack_request(self, vid: str, throw_on_failure: bool = True) -> None:
        print(f"Removing repack request for {vid}")
        self.exec(f"cta-admin repack rm --vid {vid}", throw_on_failure=throw_on_failure)

    def reclaim_tape(self, vid: str) -> None:
        print(f"Reclaiming tape {vid}")
        self.exec(f"cta-admin tape reclaim --vid {vid}")

    def has_repack_request(self, vid: str) -> bool:
        try:
            re_ls_json = json.loads(self.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
            return len(re_ls_json) > 0
        except json.JSONDecodeError:
            return False

    def retrieve_queue_empty(self, vid: str) -> bool:
        queues = json.loads(self.exec_with_output("cta-admin --json showqueues"))
        return not any(q["vid"] == vid for q in queues)

    def count_files_in_queue(self, vid: str) -> int:
        queues = json.loads(self.exec_with_output("cta-admin --json showqueues"))
        for queue in queues:
            if queue["vid"] == vid:
                return int(queue["queuedFiles"])
        return 0

    def wait_for_repack_request_launch(self, vid: str, wait_timeout_secs: int = 30) -> None:
        print(f"Waiting for the launch of the repack request on VID {vid}...")
        with Timeout(wait_timeout_secs) as t:
            while not self.has_repack_request(vid) and not t.expired:
                time.sleep(1)
            if t.expired:
                raise TimeoutError(
                    f"No repack request appeared for VID {vid} within timeout of {wait_timeout_secs} seconds"
                )
        print("Repack request launched")

    def wait_for_repack_request_expansion(self, vid: str, wait_timeout_secs: int = 30) -> None:
        print(f"Waiting for repack request expansion on VID {vid}...")
        tape_json = json.loads(self.exec_with_output(f"cta-admin --json tape ls --vid {vid}"))
        last_fseq = tape_json[0]["lastFseq"]
        wait_timeout_secs = 20
        with Timeout(wait_timeout_secs) as t:
            last_expanded_fseq = 0
            while last_expanded_fseq != last_fseq and not t.expired:
                try:
                    repack_json = json.loads(self.exec_with_output(f"cta-admin --json re ls --vid {vid}"))
                    if not repack_json:
                        continue
                    last_expanded_fseq = repack_json[0]["lastExpandedFseq"]
                except json.JSONDecodeError:
                    ...  # In this case re ls just didn't found anything
                time.sleep(1)
            if t.expired:
                raise TimeoutError(
                    f"Repack request for VID {vid} failed to expand within timeout of {wait_timeout_secs} "
                    f"seconds. Last fSeq on tape: {last_fseq}, last expanded fSeq: {last_expanded_fseq}"
                )
        print("Repack request expanded")

    def wait_for_queue_to_empty(self, vid: str, wait_timeout_secs: int = 30) -> None:
        print(f"Waiting for retrieve queue of {vid} to be empty...")
        wait_timeout_secs = 20
        with Timeout(wait_timeout_secs) as t:
            while not self.retrieve_queue_empty(vid) and not t.expired:
                time.sleep(1)
            if t.expired:
                raise TimeoutError(
                    f"Retrieve queue for VID {vid} failed to empty within timeout of {wait_timeout_secs} seconds."
                )
        print("Retrieve queue empty")
