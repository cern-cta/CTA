# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json

from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

    def install_xrootd_python(self):
        """Ensure python3 and XRootD Python bindings are installed."""
        self.exec("rpm -q python3-xrootd || dnf install -y python3 xrootd-client python3-xrootd")

    def archive_files_xrootd(
        self,
        eos_host: str,
        dest_dir: str,
        num_files: int,
        num_dirs: int,
        num_procs: int,
        file_size: int = 512,
    ) -> dict:
        """Archive files using persistent XRootD Python client.

        Runs xrootd_archive.py on the pod (must be copied to the client pod first).
        Returns parsed JSON summary dict with keys: files_created, elapsed_secs, files_per_sec.
        """
        output = self.execWithOutput(
            f"python3 -u /root/xrootd_archive.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-files {num_files} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--file-size {file_size}"
        )
        print(output)
        return json.loads(output.splitlines()[-1])
