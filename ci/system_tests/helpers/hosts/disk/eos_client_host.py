# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import asyncio

from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

    def install_xrootd_python(self):
        """Ensure python3 and XRootD Python bindings are installed."""
        self.exec("rpm -q python3-xrootd || dnf install -y python3 xrootd-client python3-xrootd")

    def _archive_cmd(self, eos_host, dest_dir, num_files, num_dirs, num_procs, file_size, batch_size):
        return (
            f"python3 -u /root/xrootd_archive.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-files {num_files} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--file-size {file_size} "
            f"--batch-size {batch_size}"
        )

    def count_files_in_namespace(self, eos_host: str, dest_dir: str, num_dirs: int, count_procs: int) -> int:
        """Count files in namespace using parallel queries on the remote host.

        Requires count_files.py to be deployed to /root/count_files.py first.
        """
        cmd = (
            f"python3 /root/count_files.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {count_procs}"
        )
        output = self.execWithOutput(cmd)
        try:
            return int(output.strip())
        except ValueError:
            return 0

    async def start_archive_process_async(
        self,
        eos_host: str,
        dest_dir: str,
        num_files: int,
        num_dirs: int,
        num_procs: int,
        file_size: int = 512,
        batch_size: int = 1000,
    ) -> asyncio.subprocess.Process:
        """Start archive as an async subprocess. Returns process handle for awaiting."""
        cmd = self._archive_cmd(eos_host, dest_dir, num_files, num_dirs, num_procs, file_size, batch_size)
        return await self.conn.exec_async(cmd)
