# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from ..remote_host import RemoteHost
import time
from collections import deque
from enum import Enum
from ...utils.timeout import Timeout


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

    def num_files_in_directory(self, directory: str) -> str: ...

    def num_files_on_tape_only(self, directory: str) -> str: ...

    def num_files_on_disk_only(self, directory: str) -> str: ...

    def list_files_in_directory(self, directory: str) -> list[str]: ...

    # Eventually we should move this method to a more central place
    def wait_for_archival_in_directory(self, archive_dir_path: str, timeout_secs: int) -> int:
        # Few problems to solve here:
        # Not all files might be archived for whatever reason (it's a stress test; errors are bound to happen)
        # Additionally, we have no guarantee on the order in which CTA archives the directories
        # So we can't wait for directories to complete one by one and instead, we continuously loop through all of them as they progress.
        # We don't want to check directories that have completed, so we use a queue

        # TODO: split methods for num files and num directories
        directories = self.list_files_in_directory(archive_dir_path)

        # For tracking progress; how many files are left for each directory
        total_files_to_archive: int = 0
        num_files_left: dict[str, int] = {}
        # While we could compute this on the fly, we only need to do it once
        num_files_total: dict[str, int] = {}

        # Construct our queue of all directories we need to check
        q = deque()
        for directory in directories:
            full_path: str = f"{archive_dir_path}/{directory}"
            num_files_total[full_path] = self.num_files_in_directory(full_path)
            num_files_left[full_path] = self.num_files_on_disk_only(full_path)
            q.append(full_path)

        # Wait for archival
        print(f"Waiting for archival in {len(directories)} directories")
        with Timeout(timeout_secs) as t:
            # As long as the queue is not empty, keep checking the directories in there
            while q and not t.expired:
                directory = q.popleft()
                num_files_in_directory = num_files_total[directory]
                archived_files = self.num_files_on_tape_only(directory)
                # Don't call the num_files_on_disk_only as this is expensive; just do the math here
                num_files_left[directory] = num_files_in_directory - archived_files

                if archived_files != num_files_in_directory:
                    # Not everything was archived -> put it back in the queue
                    q.append(directory)
                    # Prevent hammering EOS too hard
                    # Only do this when the check failed; if everything is done in a directory, we don't want to needlessly wait
                    time.sleep(1)

                print(
                    f"{sum(num_files_left.values())} files remaining to be archived to tape ({len(q)} directories left)"
                )

        # At this point either everything was archived or the timeout was reached
        missing_archives: int = 0

        if q:
            print(f"Timeout of {timeout_secs} seconds reached")
            while q:
                directory = q.popleft()
                missing_archives_in_dir = self.num_files_on_disk_only(directory)
                print(f"Directory {directory} has {missing_archives_in_dir} files not archived to tape")
                missing_archives += missing_archives_in_dir
            print(f"{missing_archives} files failed to archive")

        total_files_to_archive = sum(num_files_total.values())
        print(f"{total_files_to_archive - missing_archives}/{total_files_to_archive} files archived to tape")

        return missing_archives
