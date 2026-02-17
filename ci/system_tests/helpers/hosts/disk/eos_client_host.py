# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

from .disk_client_host import DiskClientHost


class EosClientHost(DiskClientHost):
    def __init__(self, conn):
        super().__init__(conn)

    def install_xrootd_python(self):
        """Ensure python3 and XRootD Python bindings are installed."""
        self.exec("rpm -q python3-xrootd || dnf install -y python3 xrootd-client python3-xrootd")

    def _archive_cmd(self, eos_host, dest_dir, num_files, num_dirs, num_procs, file_size):
        return (
            f"python3 -u /root/xrootd_archive.py "
            f"--eos-host {eos_host} "
            f"--dest-dir {dest_dir} "
            f"--num-files {num_files} "
            f"--num-dirs {num_dirs} "
            f"--num-procs {num_procs} "
            f"--file-size {file_size}"
        )

    def archive_files_xrootd_async(
        self,
        eos_host: str,
        dest_dir: str,
        num_files: int,
        num_dirs: int,
        num_procs: int,
        file_size: int = 512,
        log_file: str = "/root/archive.log",
    ) -> str:
        """Start archive in background. Returns PID of the background process."""
        cmd = self._archive_cmd(eos_host, dest_dir, num_files, num_dirs, num_procs, file_size)
        pid = self.execWithOutput(f"nohup {cmd} > {log_file} 2>&1 & echo \\$!")
        return pid.strip()

    def is_process_running(self, pid: str) -> bool:
        """Check if a background process is still running."""
        result = self.exec(f"kill -0 {pid} 2>/dev/null", throw_on_failure=False)
        return result.returncode == 0

