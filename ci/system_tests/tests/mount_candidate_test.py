# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import time
from typing import Callable

CANDIDATE_TIMEOUT_SECS = 60
TEST_STATE_CHANGE_REASON = "system test mount candidate listing"
ARCHIVE_MOUNT = "ARCHIVE"
RETRIEVE_MOUNT = "RETRIEVE"


def candidate_int(candidate: dict, key: str) -> int:
    return int(candidate.get(key, 0) or 0)


def is_mount_candidate(candidate: dict, mount_type: str, state=None) -> bool:
    if mount_type not in str(candidate.get("mountType", "")).upper():
        return False
    return state is None or candidate.get("state") == state


def has_queued_work(candidate: dict) -> bool:
    return candidate_int(candidate, "filesQueued") >= 1 and candidate_int(candidate, "bytesQueued") > 0


def list_mount_candidates(cta_cli) -> list[dict]:
    return json.loads(cta_cli.exec_with_output("cta-admin --json mountcandidate ls"))


def wait_for_one_mount_candidate_with(
    cta_cli,
    filter_func: Callable[[dict], bool],
    *,
    timeout: int = 30,
    interval: float = 1.0,
) -> dict:
    last_candidates = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        last_candidates = list_mount_candidates(cta_cli)
        matching_candidates = [candidate for candidate in last_candidates if filter_func(candidate)]
        if matching_candidates:
            return matching_candidates[0]
        time.sleep(interval)

    raise RuntimeError(f"Timeout reached while waiting for mount candidate. Last candidates: {last_candidates}")


def wait_for_no_mount_candidates_with(
    cta_cli,
    filter_func: Callable[[dict], bool],
    *,
    timeout: int = 30,
    interval: float = 1.0,
) -> None:
    last_candidates = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        last_candidates = list_mount_candidates(cta_cli)
        matching_candidates = [candidate for candidate in last_candidates if filter_func(candidate)]
        if not matching_candidates:
            return
        time.sleep(interval)

    raise RuntimeError(
        "Timeout reached while waiting for mount candidates to disappear. " f"Last candidates: {last_candidates}"
    )


def cleanup_file(eos_client, disk_instance_name, file_path) -> None:
    if file_path is not None:
        eos_client.exec(f"eos root://{disk_instance_name} rm --no-confirmation {file_path}", throw_on_failure=False)


def wait_for_no_archive_or_retrieve_candidates(cta_cli) -> None:
    wait_for_no_mount_candidates_with(
        cta_cli,
        lambda candidate: is_mount_candidate(candidate, ARCHIVE_MOUNT) or is_mount_candidate(candidate, RETRIEVE_MOUNT),
        timeout=CANDIDATE_TIMEOUT_SECS,
    )


def restore_initial_mount_candidate_state(cta_cli) -> None:
    cta_cli.set_all_drives_up()
    wait_for_no_archive_or_retrieve_candidates(cta_cli)


def wait_for_archive_candidate(cta_cli, state: str) -> dict:
    return wait_for_one_mount_candidate_with(
        cta_cli,
        lambda candidate: is_mount_candidate(candidate, ARCHIVE_MOUNT, state) and has_queued_work(candidate),
        timeout=CANDIDATE_TIMEOUT_SECS,
    )


def wait_for_retrieve_candidate(cta_cli, state: str) -> dict:
    return wait_for_one_mount_candidate_with(
        cta_cli,
        lambda candidate: is_mount_candidate(candidate, RETRIEVE_MOUNT, state)
        and has_queued_work(candidate)
        and candidate.get("vid", "") != "",
        timeout=CANDIDATE_TIMEOUT_SECS,
    )


def disable_active_tapes(cta_cli) -> dict[str, str]:
    active_tapes = [tape for tape in cta_cli.list_tapes() if tape["state"] == "ACTIVE"]
    assert active_tapes, "Expected at least one ACTIVE tape to disable"

    original_states = {tape["vid"]: tape["state"] for tape in active_tapes}
    for vid in original_states:
        cta_cli.set_tape_state(vid, "DISABLED", reason=TEST_STATE_CHANGE_REASON)
    return original_states


def restore_tape_states(cta_cli, original_states: dict[str, str]) -> None:
    for vid, state in original_states.items():
        if cta_cli.get_tape_state(vid) != state:
            cta_cli.set_tape_state(vid, state, reason=TEST_STATE_CHANGE_REASON)


def test_archive_mount_candidate_available(eos_client, cta_cli, test_dir, disk_instance_name):
    file_path = None
    try:
        restore_initial_mount_candidate_state(cta_cli)
        cta_cli.set_all_drives_down()
        file_path = eos_client.generate_and_archive_file(
            disk_instance_name,
            destination_path=test_dir / "test_amca_file",
            wait=False,
            append_uid=True,
        )

        candidate = wait_for_archive_candidate(cta_cli, "Available")

        # A queued archive should be available when only drives are down.
        assert candidate.get("stateReason", "") == ""
    finally:
        cleanup_file(eos_client, disk_instance_name, file_path)
        cta_cli.set_all_drives_up()


def test_archive_mount_candidate_blocked_when_tapes_are_disabled(eos_client, cta_cli, test_dir, disk_instance_name):
    file_path = None
    original_tape_states = {}
    try:
        restore_initial_mount_candidate_state(cta_cli)
        cta_cli.set_all_drives_down()
        file_path = eos_client.generate_and_archive_file(
            disk_instance_name,
            destination_path=test_dir / "test_amcbwtad_file",
            wait=False,
            append_uid=True,
        )

        wait_for_archive_candidate(cta_cli, "Available")

        original_tape_states = disable_active_tapes(cta_cli)
        candidate = wait_for_archive_candidate(cta_cli, "Blocked")

        # With all active tapes disabled, archive work should be blocked with a reason.
        assert candidate.get("stateReason", "") != ""
    finally:
        restore_tape_states(cta_cli, original_tape_states)
        cleanup_file(eos_client, disk_instance_name, file_path)
        cta_cli.set_all_drives_up()


def test_retrieve_mount_candidate_available(eos_client, cta_cli, test_dir, disk_instance_name):
    file_path = None
    try:
        restore_initial_mount_candidate_state(cta_cli)
        file_path = eos_client.generate_and_archive_file(
            disk_instance_name,
            destination_path=test_dir / "test_rmca_file",
            wait=True,
            append_uid=True,
        )

        cta_cli.set_all_drives_down()
        wait_for_no_archive_or_retrieve_candidates(cta_cli)
        eos_client.retrieve_file(disk_instance_name, file_path, wait=False)

        candidate = wait_for_retrieve_candidate(cta_cli, "Available")

        # A queued retrieve should identify the tape to mount and be immediately available.
        assert candidate["vid"]
        assert candidate.get("stateReason", "") == ""

        cta_cli.set_all_drives_up()
        eos_client.wait_for_file_retrieval(disk_instance_name, file_path, wait_timeout_secs=CANDIDATE_TIMEOUT_SECS)
    finally:
        cleanup_file(eos_client, disk_instance_name, file_path)
        cta_cli.set_all_drives_up()
