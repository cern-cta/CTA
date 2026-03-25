# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import time
from collections import deque
from enum import Enum

from ..remote_host import RemoteHost


class DiskInstanceImplementation(Enum):
    EOS = ("eos",)
    DCACHE = ("dcache",)

    def __str__(self):
        return self.value[0]

    @property
    def label(self) -> str:
        return self.value[0]


class DiskInstanceHost(RemoteHost):

    @property
    def implementation(self) -> DiskInstanceImplementation: ...

    @property
    def instance_name(self) -> str: ...

    @property
    def base_dir_path(self) -> str: ...

    def force_remove_directory(self, directory: str) -> str: ...

    def num_files_in_directory(self, directory: str) -> int: ...

    def num_files_on_tape_only(self, directory: str) -> int: ...

    def num_files_on_disk_only(self, directory: str) -> int: ...

    def list_entries_in_directory(self, directory: str) -> list[str]: ...

    def list_files_in_directory(self, directory: str) -> list[str]: ...

    def list_subdirectories_in_directory(self, directory: str) -> list[str]: ...

    # Eventually we should move this method to a more central place
    def wait_for_archival_in_directory(
        self,
        archive_dir_path: str,
        check_archive_interval_sec: int,
        max_no_progress_intervals: int = 3,
    ) -> tuple[int, float]:
        # Few problems to solve here:
        # Not all files might be archived for whatever reason (it's a stress test; errors are bound to happen)
        # Additionally, we have no guarantee on the order in which CTA archives the directories
        # So we can't wait for directories to complete one by one and instead, we continuously loop through all of them as they progress.
        # We don't want to check directories that have completed, so we use a queue

        directories = self.list_subdirectories_in_directory(archive_dir_path)

        # For tracking progress; how many files are left for each directory
        num_files_left: dict[str, int] = {}
        # While we could compute this on the fly, we only need to do it once
        num_files_total: dict[str, int] = {}

        # Construct our queue of all directories we need to check
        dirs_left_queue = deque()
        for directory in directories:
            full_path: str = f"{archive_dir_path}/{directory}"
            num_files_total[full_path] = self.num_files_in_directory(full_path)
            num_files_left[full_path] = self.num_files_on_disk_only(full_path)
            dirs_left_queue.append(full_path)

        total_files_to_archive = sum(num_files_total.values())

        # Wait for archival
        # If no progress for consecutive intervals, stop waiting for archival and proceed to retrieval
        print(f"Waiting for archival in {len(directories)} directories ({total_files_to_archive} files)")
        consecutive_no_progress_intervals = 0
        previous_remaining_files = sum(num_files_left.values())

        # As long as the queue is not empty and we're making progress, keep checking
        while dirs_left_queue and consecutive_no_progress_intervals < max_no_progress_intervals:
            # Process all directories in the current queue
            next_queue = deque()
            for full_dir_path in dirs_left_queue:
                num_files_in_directory = num_files_total[full_dir_path]
                archived_files = self.num_files_on_tape_only(full_dir_path)
                # Don't call num_files_on_disk_only as this is expensive; just do the math here
                num_files_left[full_dir_path] = num_files_in_directory - archived_files

                if archived_files != num_files_in_directory:
                    # Not everything was archived -> put it back in the queue
                    next_queue.append(full_dir_path)

            dirs_left_queue = next_queue
            current_remaining_files = sum(num_files_left.values())

            print(
                f"{current_remaining_files} files remaining to be archived to tape ({len(dirs_left_queue)} directories left)"
            )

            # Check if progress was made
            if current_remaining_files < previous_remaining_files:
                consecutive_no_progress_intervals = 0
                previous_remaining_files = current_remaining_files
            else:
                consecutive_no_progress_intervals += 1
                print(f"No progress in this interval ({consecutive_no_progress_intervals}/{max_no_progress_intervals})")

            # Sleep before next check if there are still directories to process
            if dirs_left_queue and consecutive_no_progress_intervals < max_no_progress_intervals:
                time.sleep(check_archive_interval_sec)

        # Calculate missing archives
        missing_archives = sum(num_files_left.values())

        if consecutive_no_progress_intervals >= max_no_progress_intervals:
            print(f"No progress for {max_no_progress_intervals} consecutive intervals, waiting for archival stopped")

        # Check if the loss is acceptable
        if total_files_to_archive > 0:
            loss_percent = (missing_archives / total_files_to_archive) * 100
        else:
            loss_percent = 0.0

        print(f"{total_files_to_archive - missing_archives}/{total_files_to_archive} files archived to tape")

        return missing_archives, loss_percent
